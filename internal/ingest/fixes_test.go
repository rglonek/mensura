package ingest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

// A listener that cannot bind has to fail the command.
//
// The bind used to happen inside the serving goroutine and its error went
// into a channel nothing read until every other listener had exited, which
// they only do on cancellation. So the failure was silent, the surviving
// listeners logged "receiving on ...", and everything sent to the dead
// address was refused by the kernel with nothing on this side recording it.
func TestReceiveRefusesToStartWhenTheFirstListenerCannotBind(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy a port: %v", err)
	}
	defer busy.Close()

	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	defer func() { _ = sink.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ing.Receive(ctx, ReceiveOptions{
			TCPAddr: busy.Addr().String(), UDPAddr: "127.0.0.1:0", Listener: "default",
		})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Receive returned nil for an address it could not bind")
		}
		if !strings.Contains(err.Error(), "tcp listener") {
			t.Fatalf("error does not name the listener: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Receive kept running with a listener that never bound")
	}
}

// The same, for a listener that is not the first one bound: the earlier
// ones are closed again and the command still fails, rather than serving
// two of its three addresses and looking healthy.
func TestReceiveReleasesBoundListenersWhenALaterOneFails(t *testing.T) {
	// A free address, taken and released so it can be named.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	free := probe.Addr().String()
	_ = probe.Close()

	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy a port: %v", err)
	}
	defer busy.Close()

	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	defer func() { _ = sink.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ing.Receive(ctx, ReceiveOptions{
			TCPAddr: free, HTTPAddr: busy.Addr().String(), Listener: "default",
		})
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "http listener") {
			t.Fatalf("expected the http bind failure, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Receive kept running with a listener that never bound")
	}
	// The TCP listener bound first; it must not have been left holding
	// the port on the way out.
	again, err := net.Listen("tcp", free)
	if err != nil {
		t.Fatalf("the already-bound listener was not released: %v", err)
	}
	_ = again.Close()
}

const holdSpec = `
version: 1
profiles:
  - name: ml
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
    framing:
      multiline:
        - start_contains: 'BEGIN'
          continue_regex: '^CONT'
          join: [{regex: '^CONT (.*)$', capture: 1}]
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`

// A record the extractor keeps rather than turns into a sample must hold
// the checkpoint back to its own offset.
//
// The read loop used to advance the resume point per record consumed. A
// line that only opened a multiline block produced no sample, and its
// bytes were still recorded as delivered -- so a crash before the block
// closed lost it, and a rewind after a dropped batch threw away whatever
// the block had already absorbed.
func TestFollowHoldsTheCheckpointBehindABufferedRecord(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	path := filepath.Join(dir, "app.log")

	spec, err := extract.Parse([]byte(holdSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	rs := newRecordingStore()
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	scfg := DefaultSinkConfig()
	// Batched, not one-per-Add: an inline flush from inside Add snapshots
	// the read position from before the record that triggered it, so the
	// checkpoint would never move at all and the assertions below could
	// not tell a hold-back from a stall.
	scfg.BatchSize = 8
	scfg.FlushEvery = 10 * time.Millisecond
	sink := NewSink(client, scfg, testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, StateDir: stateDir, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	defer func() { _ = sink.Close(context.Background()) }()

	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	var off int64
	appendAt := func(format string, args ...any) int64 {
		t.Helper()
		start := off
		f, ferr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if ferr != nil {
			t.Fatalf("open: %v", ferr)
		}
		n, ferr := fmt.Fprintf(f, format, args...)
		if ferr != nil {
			t.Fatalf("write: %v", ferr)
		}
		_ = f.Sync()
		_ = f.Close()
		off += int64(n)
		return start
	}

	appendAt("%d n=1\n", base)
	beginOff := int64(0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ing.Follow(ctx, FollowOptions{
			Paths: []string{path}, StartAt: "checkpoint", PollInterval: 10 * time.Millisecond,
		})
	}()
	defer func() { cancel(); <-done }()

	if !waitFor(t, 5*time.Second, func() bool { return rs.counts()[1] > 0 }) {
		t.Fatalf("the first line never arrived: %v", rs.counts())
	}

	cps, err := NewCheckpointStore(stateDir)
	if err != nil {
		t.Fatalf("checkpoints: %v", err)
	}
	acked := func() int64 {
		cp, ok := cps.Load(StreamID(path))
		if !ok {
			return -1
		}
		return cp.AckedOffset
	}
	if !waitFor(t, 5*time.Second, func() bool { return acked() > 0 }) {
		t.Fatalf("no checkpoint was written for the delivered line")
	}

	// A line that opens a multiline block, then an ordinary one after it.
	// The ordinary line's sample is delivered; the buffered one's is not.
	beginOff = appendAt("%d BEGIN n=2\n", base+1000)
	appendAt("%d n=3\n", base+2000)
	if !waitFor(t, 5*time.Second, func() bool { return rs.counts()[3] > 0 }) {
		t.Fatalf("the line after the block never arrived: %v", rs.counts())
	}
	// Give the flush loop room to write a checkpoint if it were going to.
	time.Sleep(300 * time.Millisecond)
	if at := acked(); at > beginOff {
		t.Fatalf("checkpoint advanced to %d, past the buffered record at %d: a crash here would lose it", at, beginOff)
	}

	// A second block marker closes the first one. Once its sample has
	// been delivered the checkpoint may move past it -- the hold-back
	// must not be a permanent freeze.
	closeOff := appendAt("%d BEGIN n=4\n", base+3000)
	if !waitFor(t, 5*time.Second, func() bool { return rs.counts()[2] > 0 }) {
		t.Fatalf("the buffered record was never flushed: %v", rs.counts())
	}
	if !waitFor(t, 5*time.Second, func() bool { return acked() > beginOff }) {
		t.Fatalf("checkpoint stuck at %d after the block closed; it should have reached %d", acked(), closeOff)
	}
	if at := acked(); at > closeOff {
		t.Fatalf("checkpoint advanced to %d, past the still-open block at %d", at, closeOff)
	}
}

// A sender the allow-list excludes is answered before its body is read.
//
// It used to be decoded first, so an excluded sender could still make the
// receiver parse 32 MiB of JSON and got a 400 about a set name rather than
// the 403 that was true.
func TestReceiveHTTPSamplesRefusesTheSenderBeforeReadingTheBody(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	defer func() { _ = sink.Close(context.Background()) }()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ing.Receive(ctx, ReceiveOptions{
			HTTPAddr: addr, Listener: "default",
			// Anything but the loopback address this test posts from.
			AllowedSources: []string{"10.255.255.1"},
		})
	}()
	defer func() { cancel(); <-done }()

	body := `{"set":"not a valid set name","samples":[]}`
	var resp *http.Response
	waitFor(t, 5*time.Second, func() bool {
		r, err := http.Post("http://"+addr+"/ingest/v1/samples", "application/json", strings.NewReader(body))
		if err != nil {
			return false
		}
		resp = r
		return true
	})
	if resp == nil {
		t.Fatal("the listener never came up")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status %d, want 403: the set name was judged before the sender was", resp.StatusCode)
	}
}

// A sender that closes cleanly after a line with no terminator has still
// finished that line, so it is extracted rather than dropped. Only a
// connection cut mid-record loses its fragment.
func TestReceiveTCPKeepsAnUnterminatedFinalLine(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	defer func() { _ = sink.Close(context.Background()) }()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ing.Receive(ctx, ReceiveOptions{TCPAddr: addr, Listener: "default"})
	}()

	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	var conn net.Conn
	waitFor(t, 5*time.Second, func() bool {
		c, derr := net.Dial("tcp", addr)
		if derr != nil {
			return false
		}
		conn = c
		return true
	})
	if conn == nil {
		t.Fatal("the listener never came up")
	}
	fmt.Fprintf(conn, "%d n=7\n", base)
	fmt.Fprintf(conn, "%d n=8", base+1000) // no terminator
	_ = conn.Close()

	if !waitFor(t, 5*time.Second, func() bool { c := rs.counts(); return c[7] > 0 && c[8] > 0 }) {
		t.Fatalf("a cleanly-closed final line was dropped: %v", rs.counts())
	}
	cancel()
	<-done
}
