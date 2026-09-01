package ingest

import (
	"context"
	"encoding/json"
	"fmt"
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

	// With the store healthy the same bytes are read again, whole.
	refuse.Store(false)
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
