package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// rejectingStore fatally rejects the first write that carries samples and
// accepts everything afterwards, which is the shape of a dropped batch.
type rejectingStore struct {
	mu     sync.Mutex
	seen   map[int64]int
	srv    *httptest.Server
	reject bool
}

func newRejectingStore(reject bool) *rejectingStore {
	rs := &rejectingStore{seen: map[int64]int{}, reject: reject}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/write", func(w http.ResponseWriter, r *http.Request) {
		var req wire.WriteRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		carries := false
		for _, b := range req.Batches {
			if len(b.Samples) > 0 {
				carries = true
			}
		}
		rs.mu.Lock()
		if rs.reject && carries {
			rs.reject = false
			rs.mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(wire.APIError{Error: "rejected"})
			return
		}
		n := 0
		for _, b := range req.Batches {
			for _, s := range b.Samples {
				if v, ok := s.Fields["n"]; ok {
					iv, _ := v.AsInt()
					rs.seen[iv]++
					n++
				}
			}
		}
		rs.mu.Unlock()
		_ = json.NewEncoder(w).Encode(wire.WriteResponse{Accepted: n})
	})
	rs.srv = httptest.NewServer(mux)
	return rs
}

func (rs *rejectingStore) has(n int64) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.seen[n] > 0
}

func followOnce(t *testing.T, srvURL, path, stateDir string, d time.Duration) {
	t.Helper()
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(srvURL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.BatchSize = 8
	cfg.FlushEvery = 20 * time.Millisecond
	sink := NewSink(client, cfg, testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, StateDir: stateDir, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ing.Follow(ctx, FollowOptions{Paths: []string{path}, StartAt: "checkpoint", PollInterval: 20 * time.Millisecond})
	}()
	time.Sleep(d)
	cancel()
	<-done
	_ = sink.Close(context.Background())
}

// A batch the sink gives up on leaves a hole. The checkpoint must not
// advance past it on a later successful flush, or those records are lost
// permanently -- not even a restart recovers them.
func TestDroppedBatchIsNotAcknowledgedByALaterFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	stateDir := filepath.Join(dir, "state")

	appendLines(t, path, 0, 40)
	first := newRejectingStore(true)
	defer first.srv.Close()
	followOnce(t, first.srv.URL, path, stateDir, 2*time.Second)

	// Restart against a healthy store, same state directory.
	second := newRejectingStore(false)
	defer second.srv.Close()
	followOnce(t, second.srv.URL, path, stateDir, 2*time.Second)

	var lost []int64
	for i := int64(0); i < 40; i++ {
		if !first.has(i) && !second.has(i) {
			lost = append(lost, i)
		}
	}
	if len(lost) > 0 {
		t.Fatalf("%d line(s) were never stored by either pass: %v", len(lost), lost)
	}
}

// Every followed path gets its own delivery observer. A single-slot
// callback silently kept only the last registration.
func TestObserversAccumulate(t *testing.T) {
	rs := newRejectingStore(false)
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})
	defer sink.Close(context.Background())

	var mu sync.Mutex
	began, ended := 0, 0
	for i := 0; i < 3; i++ {
		sink.Observe(countingObserver{mu: &mu, began: &began, ended: &ended})
	}
	if err := sink.AddSample(context.Background(), "s", sampleN(1)); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if began != 3 || ended != 3 {
		t.Fatalf("expected all 3 observers to run, got began=%d ended=%d", began, ended)
	}
}

func sampleN(n int64) model.Sample {
	return model.Sample{
		TSMs:   time.Now().UnixMilli(),
		Labels: map[string]string{"host": "t"},
		Fields: map[string]model.Value{"n": model.Int(n)},
	}
}

type countingObserver struct {
	mu                  *sync.Mutex
	began, ended, drops *int
}

func (c countingObserver) BeginFlush() { c.mu.Lock(); *c.began++; c.mu.Unlock() }
func (c countingObserver) EndFlush(_ int, dropped bool) {
	c.mu.Lock()
	*c.ended++
	if dropped && c.drops != nil {
		*c.drops++
	}
	c.mu.Unlock()
}

// A cancelled flush is a shutdown, not a loss: the batch goes back so the
// final flush on a live context still delivers it.
func TestCancelledFlushRequeuesRatherThanDrops(t *testing.T) {
	rs := newRejectingStore(false)
	defer rs.srv.Close()
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})

	if err := sink.AddSample(context.Background(), "s", sampleN(7)); err != nil {
		t.Fatalf("add: %v", err)
	}
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sink.Flush(dead); err == nil {
		t.Fatal("expected the cancelled flush to report an error")
	}
	if err := sink.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !rs.has(7) {
		t.Fatal("the sample queued before cancellation never reached the store")
	}
	if got := sink.Snapshot().Dropped; got != 0 {
		t.Fatalf("a cancellation was counted as %d dropped samples", got)
	}
}

// A file with no newline at all must not be re-buffered on every poll, and
// must not stall the tail behind it.
func TestOversizeRecordDoesNotStallTheTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	blob := make([]byte, 40<<10)
	for i := range blob {
		blob[i] = 'x'
	}
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	appendLines(t, path, 0, 5)

	rs := newRejectingStore(false)
	defer rs.srv.Close()
	followOnce(t, rs.srv.URL, path, filepath.Join(dir, "state"), 2*time.Second)

	// The records after the pathological prefix still arrive.
	for i := int64(1); i < 5; i++ {
		if !rs.has(i) {
			t.Fatalf("line %d never arrived: the oversize prefix stalled the tail", i)
		}
	}
}

// Two different files sharing a long identical prefix must not be mistaken
// for one another across a restart. The old fingerprint was 64 bits over
// the first 256 bytes, which log files routinely share -- a banner, a
// header, a templated first line -- and a false match made the follower
// seek into the middle of the new file and skip everything before it.
func TestFingerprintDistinguishesFilesSharingAPrefix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	stateDir := filepath.Join(dir, "state")

	// A banner far longer than the legacy 256-byte window.
	banner := "# " + strings.Repeat("mensura banner ", 40) + "\n"
	writeWithBanner := func(from, to int) {
		if err := os.WriteFile(path, []byte(banner), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		appendLines(t, path, from, to)
	}

	writeWithBanner(0, 30)
	first := newRejectingStore(false)
	defer first.srv.Close()
	followOnce(t, first.srv.URL, path, stateDir, 1500*time.Millisecond)
	for i := int64(0); i < 30; i++ {
		if !first.has(i) {
			t.Fatalf("first pass missed line %d", i)
		}
	}

	// While we were down the file was replaced by a different one that
	// begins with the same banner.
	writeWithBanner(100, 130)
	second := newRejectingStore(false)
	defer second.srv.Close()
	followOnce(t, second.srv.URL, path, stateDir, 1500*time.Millisecond)

	for i := int64(100); i < 130; i++ {
		if !second.has(i) {
			t.Fatalf("line %d of the replacement file was skipped: the follower resumed at a stale offset", i)
		}
	}
}
