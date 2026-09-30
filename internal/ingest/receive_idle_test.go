package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

// ingestFor builds an Ingest whose sink delivers to the given store URL,
// with flushing driven by the test rather than by a timer.
func ingestFor(t *testing.T, spec *extract.Spec, storeURL string) (*Ingest, *Sink) {
	t.Helper()
	client := wire.NewClient(storeURL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.BatchSize = 1 << 20
	cfg.FlushEvery = time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return ing, sink
}

// The receive path has no end of file to flush at, so the idle tick is
// the only thing that ever closes a peer's open multiline record or its
// half-filled aggregation window -- and it is the only thing that drains
// a peer the eviction sweep retires. Unlike a followed file there is no
// checkpoint to re-read from, so whatever it drops is gone.
//
// receiveIdleFlush is a five-second ticker and the profile's own idle
// timeout defaults to thirty, so nothing in the suite reached this
// function: it was the least-covered data-loss path on the listener.
func TestReceiveIdleFlushDrainsWhatAPeerIsHolding(t *testing.T) {
	const spec = `
version: 1
profiles:
  - name: numbered
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    framing:
      multiline:
        - start_contains: "BEGIN"
          continue_regex: "^ n="
          join:
            - regex: "^ (n=\\d+)$"
              capture: 1
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`
	sp, err := extract.Parse([]byte(spec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := ingestFor(t, sp, rs.srv.URL)
	defer func() { _ = sink.Close(context.Background()) }()

	r := &receiver{
		ing:     ing,
		opts:    ReceiveOptions{Mode: "logs", Listener: "test", MaxPeers: 8, PeerIdle: time.Hour},
		streams: map[string]*peerStream{},
		allowed: map[string]struct{}{},
		conns:   make(chan struct{}, 1),
	}
	ctx := context.Background()
	// A start marker opens a buffer, and a continuation line is joined
	// into it. Neither has produced a sample yet: the record only exists
	// inside extract.Stream.
	if err := r.handleRecord(ctx, "10.0.0.1", "1756382400000 BEGIN"); err != nil {
		t.Fatalf("start marker: %v", err)
	}
	if err := r.handleRecord(ctx, "10.0.0.1", "1756382400001 n=7"); err != nil {
		t.Fatalf("continuation: %v", err)
	}
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := rs.counts()[7]; got != 0 {
		t.Fatalf("a record still being assembled was delivered %d time(s)", got)
	}

	// Well past the profile's idle timeout: the buffered record is the
	// only copy there is, so the tick has to emit it.
	r.flushIdle(ctx, time.Now().Add(time.Hour))
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := rs.counts()[7]; got != 1 {
		t.Fatalf("the idle tick delivered the buffered record %d time(s), want 1: %v", got, rs.counts())
	}

	// A second tick must not deliver it again: a socket has no
	// checkpoint, so a duplicate here is a duplicate row.
	r.flushIdle(ctx, time.Now().Add(2*time.Hour))
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := rs.counts()[7]; got != 1 {
		t.Fatalf("a second idle tick delivered the same record again: %v", rs.counts())
	}
}

// A peer the eviction sweep retires is drained rather than dropped:
// deleting it outright discarded whatever it was still assembling, and
// there is nothing to re-read it from.
func TestAnEvictedPeerIsDrained(t *testing.T) {
	const spec = `
version: 1
profiles:
  - name: numbered
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    framing:
      multiline:
        - start_contains: "BEGIN"
          continue_regex: "^ n="
          join:
            - regex: "^ (n=\\d+)$"
              capture: 1
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`
	sp, err := extract.Parse([]byte(spec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := ingestFor(t, sp, rs.srv.URL)
	defer func() { _ = sink.Close(context.Background()) }()

	r := &receiver{
		ing:     ing,
		opts:    ReceiveOptions{Mode: "logs", Listener: "test", MaxPeers: 8, PeerIdle: time.Minute},
		streams: map[string]*peerStream{},
		allowed: map[string]struct{}{},
		conns:   make(chan struct{}, 1),
	}
	ctx := context.Background()
	if err := r.handleRecord(ctx, "10.0.0.2", "1756382400000 BEGIN"); err != nil {
		t.Fatalf("start marker: %v", err)
	}
	if err := r.handleRecord(ctx, "10.0.0.2", "1756382400001 n=11"); err != nil {
		t.Fatalf("continuation: %v", err)
	}
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := rs.counts()[11]; got != 0 {
		t.Fatalf("a record still being assembled was delivered %d time(s); this test would pass vacuously", got)
	}
	// The peer has been silent for longer than PeerIdle, so the sweep
	// retires it -- and has to drain it on the way out.
	r.flushIdle(ctx, time.Now().Add(time.Hour))
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := rs.counts()[11]; got != 1 {
		t.Fatalf("an evicted peer's buffered record reached the store %d time(s), want 1: %v", got, rs.counts())
	}
	if len(r.peers()) != 0 {
		t.Errorf("%d peer(s) survived eviction", len(r.peers()))
	}
}
