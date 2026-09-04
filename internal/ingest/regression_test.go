package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// A line that the follower reads before its newline has arrived must be
// delivered once, whole. Buffering the fragment *and* rewinding past it
// prepends a stale copy to the completed line.
func TestFollowDeliversASplitLineOnceAndIntact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	appendLines(t, path, 0, 20) // push past the fingerprint window

	runFollowFor(t, ing, sink, path, func() {
		if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 20 }) {
			t.Fatalf("warmup incomplete: %d", len(rs.counts()))
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		fmt.Fprintf(f, "%d n=", base+99000) // first half, no newline
		_ = f.Sync()
		time.Sleep(200 * time.Millisecond) // let the follower see the fragment
		fmt.Fprintf(f, "99\n")             // second half
		_ = f.Sync()
		_ = f.Close()
		waitFor(t, 3*time.Second, func() bool { return rs.counts()[99] > 0 })
	})

	counts := rs.counts()
	if counts[99] != 1 {
		t.Fatalf("split line delivered %d time(s), want exactly 1", counts[99])
	}
	for n, c := range counts {
		if n > 1000 {
			t.Fatalf("a corrupted record reached the store: n=%d (count %d)", n, c)
		}
	}
}

// A file that grows past the fingerprint window must not be mistaken for a
// rewrite: the fingerprint of a short file changes on every append.
func TestFollowDoesNotReReadAGrowingShortFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

	appendLines(t, path, 0, 3) // well under the 256-byte window
	runFollowFor(t, ing, sink, path, func() {
		if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 3 }) {
			t.Fatalf("warmup incomplete: %d", len(rs.counts()))
		}
		appendLines(t, path, 3, 30) // crosses the window
		if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 30 }) {
			t.Fatalf("growth incomplete: %d", len(rs.counts()))
		}
		time.Sleep(300 * time.Millisecond)
	})

	var dupes []int64
	for n, c := range rs.counts() {
		if c > 1 {
			dupes = append(dupes, n)
		}
	}
	if len(dupes) > 0 {
		t.Fatalf("%d line(s) re-delivered after the file grew past the fingerprint window", len(dupes))
	}
}

// A genuine rewrite in place must still be detected once the file is long
// enough to have a stable fingerprint.
func TestFollowStillDetectsARewriteInPlace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

	appendLines(t, path, 0, 30)
	runFollowFor(t, ing, sink, path, func() {
		if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 30 }) {
			t.Fatalf("first pass incomplete: %d", len(rs.counts()))
		}
		// Replace the contents wholesale, keeping the file at least as
		// long as the fingerprint window.
		if err := os.Truncate(path, 0); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		appendLines(t, path, 100, 130)
		// Wait for every rewritten line, not just the last one. A poll
		// that seeks with the pre-truncate offset can read a fragment
		// from the middle of the new file and deliver the tail first;
		// the rewrite is detected on the following poll, which re-reads
		// from the start. Waiting only for the highest line number
		// sampled that intermediate state and then cancelled before the
		// corrective pass, which is what this test exists to observe.
		if !waitFor(t, 5*time.Second, func() bool {
			counts := rs.counts()
			for i := int64(100); i < 130; i++ {
				if counts[i] == 0 {
					return false
				}
			}
			return true
		}) {
			t.Fatalf("post-rewrite lines never arrived")
		}
	})
	counts := rs.counts()
	for i := int64(100); i < 130; i++ {
		if counts[i] == 0 {
			t.Fatalf("line %d was lost across the rewrite", i)
		}
	}
}

// docs/design/04-wire-protocol.md section 6 defines a set keyed by
// `offset` as hashing the stream identity and the byte offset. Nothing
// supplied that hint, so the key hashed set, timestamp and labels and
// nothing else -- dropping even the field values that content keying
// hashes, and collapsing two records that share a millisecond into one
// row. Every acquisition path must attach one, and it must be distinct
// per occurrence.
func TestEveryAcquisitionPathSuppliesAKeyHint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	ks := newKeyHintStore()
	defer ks.srv.Close()

	// Two byte-identical records in the same millisecond: indistinguishable
	// to everything except the offset they were read at.
	body := "1756382400000 n=1\n1756382400000 n=1\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	ing, sink := newFollowIngest(t, ks.recordingStore, "")
	if err := ing.Batch(context.Background(), []string{path}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if err := sink.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	hints := ks.hints()
	if len(hints) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(hints))
	}
	for _, h := range hints {
		if h == "" {
			t.Fatalf("a sample reached the store with no key hint: %q", hints)
		}
	}
	if hints[0] == hints[1] {
		t.Fatalf("two occurrences shared the key hint %q, so offset keying would collapse them", hints[0])
	}
}

// keyHintStore records the key hint of every sample it is sent.
type keyHintStore struct {
	*recordingStore
	mu    sync.Mutex
	saw   []string
	inner *httptest.Server
}

func newKeyHintStore() *keyHintStore {
	ks := &keyHintStore{recordingStore: &recordingStore{seen: map[int64]int{}}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/write", func(w http.ResponseWriter, r *http.Request) {
		var req wire.WriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n := 0
		ks.mu.Lock()
		for _, b := range req.Batches {
			for _, s := range b.Samples {
				ks.saw = append(ks.saw, s.KeyHint)
				n++
			}
		}
		ks.mu.Unlock()
		_ = json.NewEncoder(w).Encode(wire.WriteResponse{Accepted: n})
	})
	ks.srv = httptest.NewServer(mux)
	return ks
}

func (k *keyHintStore) hints() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.saw...)
}

// A TCP sender's over-long line used to make bufio.Scanner return
// ErrTooLong, which ended the read loop and discarded the rest of the
// connection without a word. The record is truncated and counted; what
// follows it still arrives.
func TestReceiveTCPSurvivesAnOverlongLine(t *testing.T) {
	ks := newKeyHintStore()
	defer ks.srv.Close()
	ing, sink := newFollowIngest(t, ks.recordingStore, "")
	ing.cfg.ReadBufferBytes = 4096
	defer func() { _ = sink.Close(context.Background()) }()

	// Bind to pick a free port, then hand the address to the receiver.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ing.Receive(ctx, ReceiveOptions{TCPAddr: addr, Listener: "test"}) }()

	var conn net.Conn
	if !waitFor(t, 5*time.Second, func() bool {
		c, derr := net.Dial("tcp", addr)
		if derr != nil {
			return false
		}
		conn = c
		return true
	}) {
		t.Fatal("listener never came up")
	}
	defer conn.Close()

	long := strings.Repeat("x", 32<<10)
	if _, err := fmt.Fprintf(conn, "1756382400000 n=1 %s\n1756382400001 n=2\n", long); err != nil {
		t.Fatalf("send: %v", err)
	}
	if !waitFor(t, 5*time.Second, func() bool { return len(ks.hints()) >= 2 }) {
		t.Fatalf("the record after an over-long line never arrived (%d sample(s))", len(ks.hints()))
	}
}

// recorded reports whether the recording store accepted the sample whose
// `n` field carried this value.
func recorded(rs *recordingStore, n int64) bool { return rs.counts()[n] > 0 }

// A field carrying NaN or an infinity -- which strconv.ParseFloat
// produces from the literal text in a log line -- cannot be marshalled,
// and the failure lands on the whole request rather than the sample. One
// such line used to make the batch permanently undeliverable, and an
// undeliverable batch is reported to the delivery observers as a hole,
// so it also froze every followed file's checkpoint for the life of the
// process. The sample has to be screened before it enters a buffer.
func TestUnencodableSampleDoesNotPoisonItsBatch(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour // flush only when we say so
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()

	ctx := context.Background()
	if err := sink.AddSample(ctx, "lines", model.Sample{
		TSMs: 1000, Fields: map[string]model.Value{"n": model.Int(1)},
	}); err != nil {
		t.Fatalf("good sample: %v", err)
	}
	if err := sink.AddSample(ctx, "lines", model.Sample{
		TSMs: 1001, Fields: map[string]model.Value{"n": model.Float(math.NaN())},
	}); err != nil {
		t.Fatalf("poisoned sample returned an error to the reader: %v", err)
	}
	if err := sink.AddSample(ctx, "lines", model.Sample{
		TSMs: 1002, Fields: map[string]model.Value{"n": model.Int(2)},
	}); err != nil {
		t.Fatalf("good sample: %v", err)
	}
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !recorded(rs, 1) || !recorded(rs, 2) {
		t.Fatal("the batch was lost along with the sample that could not be encoded")
	}
	if got := sink.Snapshot().Unencodable; got != 1 {
		t.Fatalf("Unencodable = %d, expected 1", got)
	}
	if got := sink.Snapshot().Dropped; got != 0 {
		t.Fatalf("Dropped = %d, expected the batch to survive", got)
	}
}

// A rejected credential is an operator mistake that will be corrected,
// not a malformed batch. Treating 401 as fatal discarded good data and,
// because a dropped batch reads as a hole, froze every checkpoint
// permanently: a mistyped token cost far more than the outage.
func TestRejectedCredentialHoldsTheBatchInsteadOfDroppingIt(t *testing.T) {
	var refuse atomic.Bool
	refuse.Store(true)
	rs := newRecordingStore()
	defer rs.srv.Close()
	guard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refuse.Load() {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(wire.APIError{Error: "not authorised"})
			return
		}
		rs.srv.Config.Handler.ServeHTTP(w, r)
	}))
	defer guard.Close()

	client := wire.NewClient(guard.URL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if err := sink.AddSample(ctx, "lines", model.Sample{
			TSMs: int64(1000 + i), Fields: map[string]model.Value{"n": model.Int(int64(i))},
		}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := sink.Flush(ctx); err == nil {
		t.Fatal("a rejected credential was reported as a successful flush")
	}
	if got := sink.Snapshot().Dropped; got != 0 {
		t.Fatalf("Dropped = %d: the batch was discarded rather than held", got)
	}
	// The hold stops the sink spinning on a credential that will not
	// change; Close lifts it for one last attempt.
	refuse.Store(false)
	if err := sink.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := int64(1); i <= 3; i++ {
		if !recorded(rs, i) {
			t.Fatalf("sample %d was lost to the auth failure", i)
		}
	}
}

// The read offset must stay before a record until every sample it
// produced has reached the sink. Advancing first meant a delivery
// failure part-way through a record left the rest unqueued and the next
// poll seeking past the bytes that would have produced them -- silently,
// because a later record's own advance then carried the skipped bytes
// into the acknowledged range.
func TestFollowLeavesTheOffsetBeforeAnUndeliveredRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	appendLines(t, path, 0, 3)

	var refuse atomic.Bool
	refuse.Store(true)
	rs := newRecordingStore()
	defer rs.srv.Close()
	guard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refuse.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(wire.APIError{Error: "shedding"})
			return
		}
		rs.srv.Config.Handler.ServeHTTP(w, r)
	}))
	defer guard.Close()

	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(guard.URL, "")
	client.Compress, client.MaxRetries = false, 0
	cfg := DefaultSinkConfig()
	// One sample per batch, so the very first Add delivers and fails.
	cfg.BatchSize, cfg.FlushEvery = 1, time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	cps, err := NewCheckpointStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("checkpoints: %v", err)
	}
	f := &follower{ing: ing, opts: FollowOptions{MaxRecordBytes: defaultMaxRecordBytes}, cps: cps,
		tailers: map[string]*tailer{}, noProfile: map[string]time.Time{}}
	tl, err := f.ensure(path)
	if err != nil || tl == nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := f.read(context.Background(), tl); err == nil {
		t.Fatal("read reported success against a shedding store")
	}
	if tl.offset != 0 {
		t.Fatalf("offset moved to %d past a record whose samples never reached the sink", tl.offset)
	}
	if got := tl.pendingOffsetForTest(); got != 0 {
		t.Fatalf("pending moved to %d, so a commit could acknowledge undelivered bytes", got)
	}

	// With the store healthy the same bytes are read again, whole. The
	// hold the shed write installed is stepped over: it exists to stop
	// the sink spinning on a store that is refusing writes, not to delay
	// one that has recovered.
	refuse.Store(false)
	sink.now = func() time.Time { return time.Now().Add(2 * retryHold) }
	if err := f.read(context.Background(), tl); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for i := int64(0); i < 3; i++ {
		if !recorded(rs, i) {
			t.Errorf("record %d was skipped rather than re-read", i)
		}
	}
}

// A set keyed by `offset` needs a hint on every sample or the store
// refuses it. The metrics listener and the HTTP samples endpoint used to
// supply none, so a listener could never write to the very sets the
// scheme exists for.
func TestReceivedSamplesCarryAKeyHint(t *testing.T) {
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	var mu sync.Mutex
	var hints []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req wire.WriteRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		n := 0
		for _, b := range req.Batches {
			for _, s := range b.Samples {
				hints = append(hints, s.KeyHint)
				n++
			}
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(wire.WriteResponse{Accepted: n})
	}))
	defer srv.Close()

	client := wire.NewClient(srv.URL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	r := &receiver{ing: ing, opts: ReceiveOptions{Mode: "metrics", Listener: "l", MaxPeers: 4, PeerIdle: time.Hour}, streams: map[string]*peerStream{}}
	r.allowed = map[string]struct{}{}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := r.handleRecord(ctx, "10.0.0.1", "app host=web1 n=1 1700000000000"); err != nil {
			t.Fatalf("line protocol: %v", err)
		}
	}
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	_ = sink.Close(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(hints) != 3 {
		t.Fatalf("got %d sample(s), expected 3", len(hints))
	}
	seen := map[string]bool{}
	for _, h := range hints {
		if h == "" {
			t.Fatal("a received sample carried no key hint, so an offset-keyed set would refuse it")
		}
		if seen[h] {
			t.Fatalf("two records share the hint %q, so they would collapse into one row", h)
		}
		seen[h] = true
	}
}

// evictLocked returns the peers it retired precisely so they can be
// drained. stream() discarded that return value, so a peer evicted on
// the arrival of a new one lost its open multiline record and its
// half-filled aggregation window.
func TestEvictedPeerIsFlushedNotDropped(t *testing.T) {
	spec, err := extract.Parse([]byte(aggregatingSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	rs := newRecordingStore()
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	// PeerIdle of nothing, so the next arrival evicts the previous peer.
	r := &receiver{ing: ing, opts: ReceiveOptions{Mode: "logs", Listener: "l", MaxPeers: 8, PeerIdle: time.Nanosecond}, streams: map[string]*peerStream{}}
	r.allowed = map[string]struct{}{}
	ctx := context.Background()

	// One peer opens an aggregation window and then goes silent.
	if err := r.handleRecord(ctx, "10.0.0.1", "1700000000000 n=5"); err != nil {
		t.Fatalf("first peer: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	// A second peer arrives, which is what evicts the first.
	if err := r.handleRecord(ctx, "10.0.0.2", "1700000000000 n=6"); err != nil {
		t.Fatalf("second peer: %v", err)
	}
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	_ = sink.Close(ctx)
	if !recorded(rs, 5) {
		t.Fatal("the evicted peer's buffered window was dropped rather than flushed")
	}
}

const aggregatingSpec = `
version: 1
profiles:
  - name: agg
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
        aggregate: {every: 1h, on: [], field: n, mode: last}
`

// Running out of retries against a store that is shedding load is
// back-pressure, not a verdict on the batch. The store answers 503 with
// Retry-After -- its own request to slow down -- and six retries is a few
// seconds of that, so discarding here threw good data away during exactly
// the condition the shedding exists to survive.
func TestRetryExhaustionRequeuesRatherThanDropping(t *testing.T) {
	var refuse atomic.Bool
	refuse.Store(true)
	rs := newRecordingStore()
	defer rs.srv.Close()
	guard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if refuse.Load() {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(wire.APIError{Error: "shedding"})
			return
		}
		rs.srv.Config.Handler.ServeHTTP(w, r)
	}))
	defer guard.Close()

	client := wire.NewClient(guard.URL, "")
	client.Compress, client.MaxRetries = false, 1
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	obs := &countingObserver{began: new(int), ended: new(int), drops: new(int), mu: &sync.Mutex{}}
	sink.Observe(obs)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if err := sink.AddSample(ctx, "lines", model.Sample{
			TSMs: int64(1000 + i), Fields: map[string]model.Value{"n": model.Int(int64(i))},
		}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := sink.Flush(ctx); err == nil {
		t.Fatal("a shed write was reported as a successful flush")
	}
	if got := sink.Snapshot().Dropped; got != 0 {
		t.Fatalf("Dropped = %d: the batch was discarded rather than held", got)
	}
	if *obs.drops != 0 {
		t.Fatal("the observers were told a batch was lost, which freezes every checkpoint")
	}
	// The store recovers; the hold exists to stop the sink spinning on an
	// overloaded store, not to delay one that is answering again.
	refuse.Store(false)
	sink.now = func() time.Time { return time.Now().Add(2 * retryHold) }
	if err := sink.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := int64(1); i <= 3; i++ {
		if !recorded(rs, i) {
			t.Errorf("sample %d was lost to the load shedding", i)
		}
	}
}

// Holding is only worth doing while there is somewhere to put the batch.
// Past the buffer cap the oldest batch is dropped, counted and reported as
// a hole -- an unbounded buffer turns a store outage into an out-of-memory
// kill that loses everything rather than the tail.
func TestAFullBufferStillDropsAndSaysSo(t *testing.T) {
	guard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(wire.APIError{Error: "shedding"})
	}))
	defer guard.Close()

	client := wire.NewClient(guard.URL, "")
	client.Compress, client.MaxRetries = false, 0
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour
	cfg.MaxBufferedSamples = 2
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if err := sink.AddSample(ctx, "lines", model.Sample{
			TSMs: int64(1000 + i), Fields: map[string]model.Value{"n": model.Int(int64(i))},
		}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	// The hold is stepped over between flushes; each one finds the buffer
	// fuller than the last.
	for i := 0; i < 3; i++ {
		sink.now = func() time.Time { return time.Now().Add(time.Duration(i+1) * 2 * retryHold) }
		_ = sink.Flush(ctx)
	}
	if got := sink.Snapshot().Dropped; got == 0 {
		t.Fatal("the buffer cap was passed and nothing was counted as dropped")
	}
}

// The checkpoint freeze a lost batch installs is the right policy; needing
// a process restart to leave it was not. A few seconds of store
// unavailability stopped checkpointing every followed file until someone
// noticed, and every one of those files then replayed from its last
// pre-incident offset.
func TestAFrozenCheckpointThawsWhenDeliveryRecovers(t *testing.T) {
	tl := &tailer{path: "/tmp/x", cp: &Checkpoint{Stream: "s"}}
	tl.setPending(600)
	tl.markInflight()
	if !tl.markHoled() {
		t.Fatal("a batch in flight did not freeze the tailer")
	}
	// While frozen, nothing may be acknowledged.
	tl.setPending(1200)
	tl.markInflight()
	if _, ok := tl.commitInflight(1); ok {
		t.Fatal("a frozen tailer acknowledged bytes past the hole")
	}
	at, thawed := tl.clearHole()
	if !thawed || at != 0 {
		t.Fatalf("clearHole() = (%d, %v), want (0, true)", at, thawed)
	}
	if got, ok := tl.takeRewind(); !ok || got != 0 {
		t.Fatalf("takeRewind() = (%d, %v), want (0, true) so the lost bytes are read again", got, ok)
	}
	if got := tl.pendingOffsetForTest(); got != 0 {
		t.Fatalf("pending = %d after the thaw: a commit could acknowledge bytes nothing re-read", got)
	}
	// Once thawed it checkpoints normally again.
	tl.setPending(64)
	tl.markInflight()
	if _, ok := tl.commitInflight(1); !ok {
		t.Fatal("a thawed tailer still refuses to checkpoint")
	}
}

// A rotation replaces the bytes the hole was in, so there is nothing left
// to protect by staying frozen.
func TestRotationClearsAFrozenCheckpoint(t *testing.T) {
	tl := &tailer{path: "/tmp/x", cp: &Checkpoint{Stream: "s"}}
	tl.setPending(600)
	tl.markInflight()
	tl.markHoled()
	tl.reset(0)
	tl.setPending(64)
	tl.markInflight()
	if _, ok := tl.commitInflight(1); !ok {
		t.Fatal("a rotated file is still frozen by a hole in the file it replaced")
	}
}

// Overlapping sources are ordinary -- "/logs" and "/logs/*.log" name the
// same files -- and every record in the overlap used to be extracted,
// delivered and counted twice.
func TestResolveDeduplicatesOverlappingSources(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	appendLines(t, path, 0, 1)
	i := &Ingest{}
	got, err := i.resolve([]string{dir, filepath.Join(dir, "*.log"), path})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("resolve returned %d entries for one file: %v", len(got), got)
	}
}

// A declaration the store refuses outright must not be requeued. The store
// applies metadata before any batch and answers a bad one with 400, which
// the client classifies as fatal, so putting it back at the head of the
// queue made the *next* batch fail for the same reason, and the one after
// that: one unusable line in a spec dropped every sample the ingester
// produced until MaxFatalDrops gave up on the process.
func TestFatallyRejectedMetadataIsNotResent(t *testing.T) {
	var mu sync.Mutex
	var withMeta, accepted int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req wire.WriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if len(req.SetMeta) > 0 {
			withMeta++
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(wire.APIError{Error: `set "_mensura_x" uses the reserved prefix`})
			return
		}
		for _, b := range req.Batches {
			accepted += len(b.Samples)
		}
		_ = json.NewEncoder(w).Encode(wire.WriteResponse{Accepted: accepted})
	}))
	defer srv.Close()

	client := wire.NewClient(srv.URL, "")
	client.Compress, client.MaxRetries = false, 0
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()

	ms := int64(3600000)
	sink.metaMu.Lock()
	sink.setQ = append(sink.setQ, wire.SetMeta{Set: "_mensura_x", RetentionMs: &ms})
	sink.metaMu.Unlock()

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := sink.AddSample(ctx, "lines", sampleN(int64(i))); err != nil {
			t.Fatalf("add: %v", err)
		}
		_ = sink.Flush(ctx)
	}
	mu.Lock()
	defer mu.Unlock()
	if withMeta != 1 {
		t.Errorf("the refused declaration was sent %d times; it must be discarded after the first rejection", withMeta)
	}
	if accepted != 3 {
		t.Errorf("%d of 3 later samples were accepted: the poison declaration kept failing their batches", accepted)
	}
	if got := sink.Snapshot().FatalDrop; got != 1 {
		t.Errorf("FatalDrop = %d, want exactly the one batch that carried the bad declaration", got)
	}
}

// Holding is only worth doing while there is somewhere to put the batch.
// The cap used to be consulted only where a batch was put back, so while
// delivery was held -- 30 seconds after a rejected credential, or as long
// as a Retry-After the store chose -- Add kept appending with nothing
// bounding it at all.
func TestAHeldDeliveryStillHonoursTheBufferCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(wire.APIError{Error: "shedding"})
	}))
	defer srv.Close()

	client := wire.NewClient(srv.URL, "")
	client.Compress, client.MaxRetries = false, 0
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour
	cfg.BatchSize = 4
	cfg.MaxBufferedSamples = 16
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	obs := &countingObserver{began: new(int), ended: new(int), drops: new(int), mu: &sync.Mutex{}}
	sink.Observe(obs)

	ctx := context.Background()
	for i := 0; i < 400; i++ {
		if err := sink.AddSample(ctx, "lines", sampleN(int64(i))); err != nil && i == 0 {
			t.Fatalf("add: %v", err)
		}
	}
	if got := sink.buffered(); got > cfg.MaxBufferedSamples {
		t.Errorf("buffered %d samples against a cap of %d: the hold ignores the limit", got, cfg.MaxBufferedSamples)
	}
	if got := sink.Snapshot().Dropped; got == 0 {
		t.Error("samples were discarded past the cap without being counted")
	}
	obs.mu.Lock()
	drops := *obs.drops
	obs.mu.Unlock()
	if drops == 0 {
		t.Error("the observers were not told the samples were lost, so a checkpoint could advance over them")
	}
}

// BatchBytes was documented, defaulted and never read, so one request was
// bounded only by a sample count. A buffer that filled during an outage
// then went out as a single body, and a body past the store's
// max_request_bytes comes back 413 -- a status the client classifies as
// fatal, so the whole buffer was dropped rather than delivered in pieces.
func TestABigBufferIsDeliveredInBoundedRequests(t *testing.T) {
	var mu sync.Mutex
	var sizes []int
	var got []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req wire.WriteRequest
		if err := json.Unmarshal(body, &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n := 0
		mu.Lock()
		sizes = append(sizes, len(body))
		for _, b := range req.Batches {
			for _, s := range b.Samples {
				v, _ := s.Fields["n"].AsInt()
				got = append(got, v)
				n++
			}
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(wire.WriteResponse{Accepted: n})
	}))
	defer srv.Close()

	client := wire.NewClient(srv.URL, "")
	client.Compress, client.MaxRetries = false, 0
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour
	cfg.BatchSize = 1 << 20 // never triggers; the size bound is what is under test
	cfg.BatchBytes = 4096
	sink := NewSink(client, cfg, testLogger{t})
	ctx := context.Background()
	const n = 200
	for i := 0; i < n; i++ {
		if err := sink.AddSample(ctx, "lines", sampleN(int64(i))); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	for i := 0; i < 20 && sink.buffered() > 0; i++ {
		if err := sink.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	if err := sink.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sizes) < 2 {
		t.Fatalf("%d request(s) for %d samples: the byte budget was not applied", len(sizes), n)
	}
	for _, sz := range sizes {
		if sz > 4*cfg.BatchBytes {
			t.Errorf("a request of %d bytes went out against a %d-byte budget", sz, cfg.BatchBytes)
		}
	}
	if len(got) != n {
		t.Fatalf("delivered %d of %d samples", len(got), n)
	}
	seen := map[int64]bool{}
	for _, v := range got {
		if seen[v] {
			t.Fatalf("sample %d was delivered twice by the split", v)
		}
		seen[v] = true
	}
}

// A partial take must not let a checkpoint advance: the observers' own
// high-water mark covers every byte handed to the sink, including the
// samples still waiting in the buffer.
//
// One round, not a whole Flush: Flush keeps sending rounds until the
// buffer is empty, and it is the round that leaves samples behind whose
// silence is being asserted.
func TestAPartialTakeDoesNotAcknowledgeWhatItLeftBehind(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.FlushEvery = time.Hour
	cfg.BatchSize = 1 << 20
	cfg.BatchBytes = 256
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	obs := &countingObserver{began: new(int), ended: new(int), drops: new(int), mu: &sync.Mutex{}}
	sink.Observe(obs)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if err := sink.AddSample(ctx, "lines", sampleN(int64(i))); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	partial, err := sink.flushRound(ctx, sink.snapshotObservers())
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !partial || sink.buffered() == 0 {
		t.Fatal("the whole buffer went out in one request; the test is not exercising a partial take")
	}
	obs.mu.Lock()
	began := *obs.began
	obs.mu.Unlock()
	if began != 0 {
		t.Errorf("BeginFlush ran %d time(s) for a batch that left samples behind: the checkpoint would cover bytes the store does not hold", began)
	}
}

// A record truncated at the cap is counted as oversize even when the
// truncation point falls inside the chunk that also carries the newline --
// the common case of one long line. Truncation nothing counts is
// indistinguishable from data that was never there.
func TestTruncationIsAlwaysCounted(t *testing.T) {
	cases := []struct {
		name     string
		line     string
		max      int
		oversize bool
		want     string
	}{
		{"fits", "hello\n", 10, false, "hello"},
		{"exactly at the cap", "hello\n", 5, false, "hello"},
		{"exactly at the cap with crlf", "hello\r\n", 5, false, "hello"},
		{"truncated in the terminating chunk", "hello world\n", 5, true, "hello"},
		{"truncated with no terminator", "hello world", 5, true, "hello"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec, _ := readRecord(bufio.NewReaderSize(strings.NewReader(c.line), 64), c.max)
			if rec.Oversize != c.oversize {
				t.Errorf("Oversize = %v, want %v (kept %q of %q)", rec.Oversize, c.oversize, rec.Line, c.line)
			}
			if string(rec.Line) != c.want {
				t.Errorf("Line = %q, want %q", rec.Line, c.want)
			}
			if rec.Consumed != len(c.line) {
				t.Errorf("Consumed = %d, want %d: byte offsets are checkpoints", rec.Consumed, len(c.line))
			}
		})
	}
}

// The progress document is what an operator reads after the crash it
// describes, so it is synced before the rename and the directory after it,
// the way the checkpoints are. A document that survived only in the page
// cache describes nothing.
func TestProgressFileIsWrittenAtomically(t *testing.T) {
	p := NewProgress()
	p.AddRecord(120)
	p.Unmatched()
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "progress.json")
	if err := p.WriteFile(path); err != nil {
		t.Fatalf("write: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		t.Fatalf("the progress document is not readable: %v", err)
	}
	if snap.BytesRead != 120 || snap.UnmatchedLines != 1 {
		t.Errorf("progress document does not carry the counters: %+v", snap)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("the temporary file was left behind")
	}
	// Overwriting an existing document must work too.
	p.AddRecord(80)
	if err := p.WriteFile(path); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
}

// A CRLF can be split by the reader's own buffer. The terminator is
// stripped either way, so a label built from the record does not carry a
// stray carriage return.
func TestSplitCRLFIsStillATerminator(t *testing.T) {
	// A 16-byte reader buffer makes ReadSlice hand back "0123456789abcde\r"
	// and then "\n".
	r := bufio.NewReaderSize(strings.NewReader("0123456789abcde\r\nnext\n"), 16)
	rec, err := readRecord(r, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := string(rec.Line); got != "0123456789abcde" {
		t.Errorf("Line = %q, want the record without its terminator", got)
	}
	if !rec.Terminated || rec.Consumed != 17 {
		t.Errorf("Terminated = %v, Consumed = %d, want true and 17", rec.Terminated, rec.Consumed)
	}
}

// The buffer cap takes its cut from the oldest end of every buffered set
// in proportion to its size.
//
// Draining the sorted set list in order was the worst available
// approximation of "the oldest samples": with sets `access` and `zzz`
// over the cap, the whole of `access` was discarded before `zzz` lost a
// single sample, so one stream went dark on the dashboard while another
// was untouched.
func TestBufferCapDoesNotEmptyOneSetBeforeTouchingAnother(t *testing.T) {
	rs := newRejectingStore(false)
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false

	cfg := DefaultSinkConfig()
	cfg.BatchSize = 1 << 20 // never auto-flush
	cfg.FlushEvery = time.Hour
	cfg.MaxBufferedSamples = 100
	s := NewSink(client, cfg, testLogger{t})
	defer func() { _ = s.Close(context.Background()) }()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := s.AddSample(ctx, "access", sampleN(int64(i))); err != nil {
			t.Fatalf("add access: %v", err)
		}
		if err := s.AddSample(ctx, "zzz", sampleN(int64(1000+i))); err != nil {
			t.Fatalf("add zzz: %v", err)
		}
	}
	// Delivery is held, so Flush enforces the cap instead of writing.
	s.holdDelivery(time.Hour)
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	s.mu.Lock()
	access, zzz := len(s.buffers["access"]), len(s.buffers["zzz"])
	pending := s.pending
	s.mu.Unlock()
	if pending != cfg.MaxBufferedSamples {
		t.Fatalf("expected the buffer to be trimmed to %d, got %d", cfg.MaxBufferedSamples, pending)
	}
	if access == 0 || zzz == 0 {
		t.Fatalf("one set was emptied while the other was spared: access=%d zzz=%d", access, zzz)
	}
	// Each set gave up roughly the same share, and each kept its newest.
	if access < 40 || zzz < 40 {
		t.Fatalf("the cut was not proportional: access=%d zzz=%d", access, zzz)
	}
	s.mu.Lock()
	first, _ := s.buffers["access"][0].Fields["n"].AsInt()
	s.mu.Unlock()
	if first == 0 {
		t.Fatal("the cut must come off the oldest end of each set")
	}
}

// A flush that could only take part of the buffer announces neither half.
// The mark BeginFlush would take covers samples the batch does not carry,
// so there is nothing it entitles an observer to acknowledge. Ending a
// flush that was never begun leaned on commitInflight happening to no-op,
// an invariant no observer contract states.
func TestPartialFlushReportsNeitherHalfToObservers(t *testing.T) {
	rs := newRejectingStore(false)
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false

	cfg := DefaultSinkConfig()
	cfg.BatchSize = 1 << 20
	cfg.FlushEvery = time.Hour
	// Small enough that a hundred samples cannot leave in one batch.
	cfg.BatchBytes = 256
	s := NewSink(client, cfg, testLogger{t})
	defer func() { _ = s.Close(context.Background()) }()

	var mu sync.Mutex
	var began, ended, drops int
	s.Observe(countingObserver{mu: &mu, began: &began, ended: &ended, drops: &drops})

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if err := s.AddSample(ctx, "a", sampleN(int64(i))); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	partial, err := s.flushRound(ctx, s.snapshotObservers())
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !partial || s.buffered() == 0 {
		t.Fatal("this test needs a round that leaves samples behind")
	}
	mu.Lock()
	b, e := began, ended
	mu.Unlock()
	if b != 0 || e != 0 {
		t.Fatalf("a partial take must announce neither half, got began=%d ended=%d", b, e)
	}

	// Drain to empty; the take that clears the buffer is a matched pair.
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("drain flush: %v", err)
	}
	if s.buffered() != 0 {
		t.Fatalf("Flush left %d sample(s) buffered; it drains the backlog it split", s.buffered())
	}
	mu.Lock()
	b, e = began, ended
	mu.Unlock()
	if b == 0 || b != e {
		t.Fatalf("Begin and End must pair, got began=%d ended=%d", b, e)
	}
}

// retire() rewinds the checkpoint for the replacement file. The
// fingerprint has to go with the offsets: it described the file that was
// just rotated away, so the record left on disk named an offset of zero
// in the new file alongside a content hash of the old one -- and if the
// process stopped in that window, the next start compared the
// replacement against a hash belonging to a file that no longer exists.
//
// This drives retire() directly rather than racing a live follow, because
// the stale record only exists between the rotation and the next
// successful flush, which promptly overwrites it.
func TestRotationClearsTheCheckpointFingerprintWithTheOffset(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	path := filepath.Join(dir, "app.log")
	appendLines(t, path, 0, 20)

	rs := newRejectingStore(false)
	defer rs.srv.Close()
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()
	ing, err := New(Config{Spec: spec, Sink: sink, StateDir: stateDir, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	cps, err := NewCheckpointStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	f := &follower{ing: ing, cps: cps, tailers: map[string]*tailer{}, noProfile: map[string]time.Time{}}

	tl, err := f.ensure(path)
	if err != nil || tl == nil {
		t.Fatalf("ensure: %v", err)
	}
	// Read the file so the tailer carries a real offset and fingerprint,
	// which is the state a rotation finds it in.
	if err := f.read(context.Background(), tl); err != nil {
		t.Fatalf("read: %v", err)
	}
	if width := fingerprintWidth(tl.offset); width > 0 {
		if fp, n := fingerprintAt(tl.file, width); n == width {
			tl.setFingerprint(fp, width)
		}
	}
	if tl.fingerprint == "" {
		t.Fatal("this test needs a fingerprinted tailer")
	}

	f.retire(context.Background(), tl)

	cp, ok := cps.Load(StreamID(path))
	if !ok {
		t.Fatal("retire must leave a checkpoint behind")
	}
	if cp.AckedOffset != 0 || cp.Offset != 0 {
		t.Fatalf("retire must rewind the offsets for the replacement file: %+v", cp)
	}
	if cp.Fingerprint != "" || cp.FingerprintBytes != 0 {
		t.Fatalf("retire left the rotated-away file's fingerprint on a rewound checkpoint: %+v", cp)
	}
}

// --start-at is honoured where it is applied, not where the loop happens
// to reach the bottom. Handling it inside the else arm of the size probe
// meant a single transient SSH failure on the first pass -- which the
// loop logs and carries on from -- silently dropped the flag for the life
// of the process.
func TestRemoteStartAtSurvivesAFailedFirstSizeProbe(t *testing.T) {
	dir := t.TempDir()
	remote := filepath.Join(dir, "remote.log")
	if err := os.WriteFile(remote, []byte("1000 v=1\n2000 v=2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// An ssh stand-in whose first `wc -c` fails and whose later ones work.
	fake := filepath.Join(dir, "ssh")
	countFile := filepath.Join(dir, "probes")
	script := "#!/bin/sh\n" +
		"cmd=\"$#\"\n" +
		"eval last=\\${$cmd}\n" +
		"case \"$last\" in\n" +
		"  *'wc -c'*)\n" +
		"    n=$(cat " + countFile + " 2>/dev/null || echo 0)\n" +
		"    echo $((n+1)) > " + countFile + "\n" +
		"    if [ \"$n\" = \"0\" ]; then echo 'probe failed' >&2; exit 3; fi\n" +
		"    wc -c < " + remote + "\n" +
		"    exit 0;;\n" +
		"  *tail*)\n" +
		"    sleep 0.4; exit 0;;\n" +
		"esac\n" +
		"exit 0\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	rs := newRejectingStore(false)
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	stateDir := filepath.Join(dir, "state")
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, StateDir: stateDir, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = ing.FollowRemote(ctx, RemoteOptions{
		Host: "h", Paths: []string{remote}, StartAt: "end",
		SSHBinary: fake, ProbeInterval: 100 * time.Millisecond,
		ReconnectBackoff: 50 * time.Millisecond,
	})
	_ = sink.Close(context.Background())

	cps, err := NewCheckpointStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	cp, ok := cps.Load(StreamID("h:" + remote))
	if !ok {
		t.Fatal("expected a checkpoint once the probe succeeded")
	}
	size, err := os.Stat(remote)
	if err != nil {
		t.Fatal(err)
	}
	if cp.AckedOffset != size.Size() {
		t.Fatalf("--start-at end was dropped after the first probe failed: checkpoint at %d, file is %d bytes", cp.AckedOffset, size.Size())
	}
}

// Progress.Samples used to be written only by MergeStream, and only the
// batch importer calls that -- so follow, SSH follow and receive reported
// "0 samples" forever: on the console, in the progress document, and in
// the samples field the _mensura_ingest set publishes for dashboards.
func TestFollowCountsTheSamplesItDelivers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

	appendLines(t, path, 0, 12)
	runFollowFor(t, ing, sink, path, func() {
		if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 12 }) {
			t.Fatalf("delivery incomplete: %d", len(rs.counts()))
		}
	})
	snap := ing.Progress().Snapshot()
	if snap.Samples < 12 {
		t.Fatalf("a follow that delivered %d sample(s) reported %d", len(rs.counts()), snap.Samples)
	}
}

// The HTTP lines endpoint answered 200 {"accepted": n} with no
// denominator, so a sender that was entirely blocked by --allow-source,
// or whose lines matched no pattern, could not tell that apart from an
// empty post.
func TestReceivedLinesReportWhatWasRefused(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, "")
	defer func() { _ = sink.Close(context.Background()) }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = ing.Receive(ctx, ReceiveOptions{HTTPAddr: addr, Listener: "test", AllowedSources: []string{"10.0.0.1"}})
	}()

	url := "http://" + addr + "/ingest/v1/lines"
	var resp *http.Response
	if !waitFor(t, 5*time.Second, func() bool {
		r, perr := http.Post(url, "text/plain", strings.NewReader("1756382400000 n=1\n"))
		if perr != nil {
			return false
		}
		resp = r
		return true
	}) {
		t.Fatal("listener never came up")
	}
	defer resp.Body.Close()
	// The sender is not in allowed_sources, which is the same verdict the
	// samples endpoint gives it.
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("a disallowed sender got %d, want 403", resp.StatusCode)
	}
}

// The same endpoint, from a permitted sender: lines the spec cannot use
// are counted rather than dropped in silence.
func TestReceivedLinesCountTheUnmatched(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, "")
	defer func() { _ = sink.Close(context.Background()) }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ing.Receive(ctx, ReceiveOptions{HTTPAddr: addr, Listener: "test"}) }()

	url := "http://" + addr + "/ingest/v1/lines"
	var body map[string]any
	if !waitFor(t, 5*time.Second, func() bool {
		r, perr := http.Post(url, "text/plain", strings.NewReader("1756382400000 n=1\nnot a record at all\n"))
		if perr != nil {
			return false
		}
		defer r.Body.Close()
		return json.NewDecoder(r.Body).Decode(&body) == nil
	}) {
		t.Fatal("listener never came up")
	}
	if got, _ := body["accepted"].(float64); got != 1 {
		t.Errorf("accepted = %v, want 1 (%v)", body["accepted"], body)
	}
	if got, _ := body["refused"].(float64); got != 1 {
		t.Fatalf("a line the spec could not use was counted nowhere: %v", body)
	}
	if _, ok := body["reason"]; !ok {
		t.Error("the response says how many were refused but not why")
	}
}
