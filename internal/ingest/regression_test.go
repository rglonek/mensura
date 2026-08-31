package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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
