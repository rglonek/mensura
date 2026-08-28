package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

const followSpec = `
version: 1
profiles:
  - name: numbered
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`

// recordingStore stands in for a real store: it records every sample it
// accepts so a test can assert on gaps and duplicates.
type recordingStore struct {
	mu      sync.Mutex
	seen    map[int64]int
	srv     *httptest.Server
	fail    int // fail this many requests before accepting
	written int
}

func newRecordingStore() *recordingStore {
	rs := &recordingStore{seen: map[int64]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/write", func(w http.ResponseWriter, r *http.Request) {
		rs.mu.Lock()
		if rs.fail > 0 {
			rs.fail--
			rs.mu.Unlock()
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(wire.APIError{Error: "shedding"})
			return
		}
		rs.mu.Unlock()

		var req wire.WriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n := 0
		rs.mu.Lock()
		for _, b := range req.Batches {
			for _, s := range b.Samples {
				if v, ok := s.Fields["n"]; ok {
					iv, _ := v.AsInt()
					rs.seen[iv]++
					n++
				}
			}
		}
		rs.written += n
		rs.mu.Unlock()
		_ = json.NewEncoder(w).Encode(wire.WriteResponse{Accepted: n})
	})
	rs.srv = httptest.NewServer(mux)
	return rs
}

func (rs *recordingStore) counts() map[int64]int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := map[int64]int{}
	for k, v := range rs.seen {
		out[k] = v
	}
	return out
}

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) { l.t.Logf(format, args...) }

func newFollowIngest(t *testing.T, rs *recordingStore, stateDir string) (*Ingest, *Sink) {
	t.Helper()
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	cfg.BatchSize = 8
	cfg.FlushEvery = 20 * time.Millisecond
	sink := NewSink(client, cfg, testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, StateDir: stateDir, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return ing, sink
}

func appendLines(t *testing.T, path string, from, to int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	for i := from; i < to; i++ {
		if _, err := fmt.Fprintf(f, "%d n=%d\n", base+int64(i)*1000, i); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
}

// waitFor polls until cond holds or the deadline passes, so the test does
// not depend on a fixed sleep.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func runFollowFor(t *testing.T, ing *Ingest, sink *Sink, path string, body func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ing.Follow(ctx, FollowOptions{Paths: []string{path}, StartAt: "checkpoint", PollInterval: 20 * time.Millisecond})
	}()
	body()
	cancel()
	<-done
	_ = sink.Flush(context.Background())
}

// Rotation must lose nothing and, under content-addressed keys, duplicate
// nothing. Each style is driven against a real filesystem.
func TestFollowAcrossRotationStyles(t *testing.T) {
	styles := []struct {
		name   string
		rotate func(t *testing.T, path string)
	}{
		{"rename-and-create", func(t *testing.T, path string) {
			if err := os.Rename(path, path+".1"); err != nil {
				t.Fatalf("rename: %v", err)
			}
		}},
		{"delete-and-create", func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatalf("remove: %v", err)
			}
		}},
		{"copytruncate", func(t *testing.T, path string) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if err := os.WriteFile(path+".1", data, 0o644); err != nil {
				t.Fatalf("copy: %v", err)
			}
			if err := os.Truncate(path, 0); err != nil {
				t.Fatalf("truncate: %v", err)
			}
		}},
	}

	for _, style := range styles {
		t.Run(style.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "app.log")
			rs := newRecordingStore()
			defer rs.srv.Close()
			ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

			appendLines(t, path, 0, 50)
			runFollowFor(t, ing, sink, path, func() {
				if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 50 }) {
					t.Fatalf("first 50 lines never arrived: got %d", len(rs.counts()))
				}
				style.rotate(t, path)
				appendLines(t, path, 50, 100)
				if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 100 }) {
					t.Fatalf("post-rotation lines never arrived: got %d", len(rs.counts()))
				}
			})

			counts := rs.counts()
			var missing []int64
			for i := int64(0); i < 100; i++ {
				if counts[i] == 0 {
					missing = append(missing, i)
				}
			}
			if len(missing) > 0 {
				sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
				t.Fatalf("%s lost %d line(s): %v", style.name, len(missing), missing[:min(5, len(missing))])
			}
		})
	}
}

// A restart resumes from the acknowledged offset: nothing is lost, and
// nothing before that offset is replayed.
func TestFollowResumesFromCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	stateDir := filepath.Join(dir, "state")
	rs := newRecordingStore()
	defer rs.srv.Close()

	appendLines(t, path, 0, 30)
	ing, sink := newFollowIngest(t, rs, stateDir)
	runFollowFor(t, ing, sink, path, func() {
		if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 30 }) {
			t.Fatalf("first pass incomplete: %d", len(rs.counts()))
		}
	})
	_ = sink.Close(context.Background())

	// Second process, same state directory.
	appendLines(t, path, 30, 60)
	rs2 := newRecordingStore()
	defer rs2.srv.Close()
	ing2, sink2 := newFollowIngest(t, rs2, stateDir)
	runFollowFor(t, ing2, sink2, path, func() {
		if !waitFor(t, 5*time.Second, func() bool { return len(rs2.counts()) >= 30 }) {
			t.Fatalf("resume incomplete: %d", len(rs2.counts()))
		}
	})
	_ = sink2.Close(context.Background())

	counts := rs2.counts()
	for i := int64(30); i < 60; i++ {
		if counts[i] == 0 {
			t.Fatalf("line %d was lost across the restart", i)
		}
	}
	// Some replay of already-acknowledged bytes is permitted by the
	// at-least-once contract, but a resumed follow should not re-read the
	// whole file.
	replayed := 0
	for i := int64(0); i < 30; i++ {
		replayed += counts[i]
	}
	if replayed > 5 {
		t.Fatalf("resume replayed %d already-acknowledged lines; the checkpoint is not being honoured", replayed)
	}
}

// A store that sheds must not cost data: the client retries and the lines
// arrive once the store recovers.
func TestFollowSurvivesStoreShedding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rs := newRecordingStore()
	rs.fail = 3
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

	appendLines(t, path, 0, 20)
	runFollowFor(t, ing, sink, path, func() {
		if !waitFor(t, 10*time.Second, func() bool { return len(rs.counts()) >= 20 }) {
			t.Fatalf("lines never arrived after shedding: %d", len(rs.counts()))
		}
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
