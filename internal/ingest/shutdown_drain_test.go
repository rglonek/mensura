package ingest

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

// TestReceiveHTTPWaitsForInFlightRequests pins the drain the HTTP listener
// owes the requests it has already accepted.
//
// http.Server.Shutdown closes the listener first and only then waits for
// those requests, so Serve returns straight away while handlers are still
// running. serveHTTP used to return on that, which let Receive run
// flushAll and its final Sink.Flush behind a handler that was still
// buffering samples: a sender answered `"accepted": n` lost every record
// it sent in that window, with no checkpoint to re-read them from. The TCP
// and UDP listeners both join their workers for exactly this reason.
func TestReceiveHTTPWaitsForInFlightRequests(t *testing.T) {
	rs := newRecordingStore()
	t.Cleanup(rs.srv.Close)

	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	cfg := DefaultSinkConfig()
	// Nothing flushes on its own: the only delivery is the one Receive
	// performs on its way out, which is the thing being tested.
	cfg.BatchSize = 1000
	cfg.FlushEvery = time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ing.Receive(ctx, ReceiveOptions{HTTPAddr: addr, Listener: "test"}) }()
	if !waitFor(t, 5*time.Second, func() bool {
		c, derr := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if derr != nil {
			return false
		}
		_ = c.Close()
		return true
	}) {
		t.Fatal("listener never came up")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	writeChunk := func(s string) {
		if _, werr := fmt.Fprintf(conn, "%x\r\n%s\r\n", len(s), s); werr != nil {
			t.Fatalf("write chunk: %v", werr)
		}
	}
	// A chunked body so the handler stays inside readRecord between the
	// two records, which is where a shutdown has to find it.
	if _, err := fmt.Fprint(conn, "POST /ingest/v1/lines HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"); err != nil {
		t.Fatalf("write head: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	writeChunk(fmt.Sprintf("%d n=1\n", base))
	if !waitFor(t, 5*time.Second, func() bool { return sink.buffered() == 1 }) {
		t.Fatalf("the handler never took the first record (buffered %d)", sink.buffered())
	}

	cancel()
	// The request is still being served, so Receive owes it the rest of
	// its body before it may flush for the last time.
	select {
	case rerr := <-done:
		t.Fatalf("Receive returned with a request still in flight: %v", rerr)
	case <-time.After(500 * time.Millisecond):
	}

	writeChunk(fmt.Sprintf("%d n=2\n", base+1000))
	writeChunk("") // the terminating chunk
	if !waitFor(t, 5*time.Second, func() bool { return sink.buffered() == 2 }) {
		t.Fatalf("the handler never took the second record (buffered %d)", sink.buffered())
	}

	select {
	case rerr := <-done:
		if rerr != nil {
			t.Fatalf("Receive: %v", rerr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Receive never returned after the request finished")
	}

	// Receive's own final flush has to cover both records, which is the
	// whole point of waiting for the handler.
	got := rs.counts()
	for _, n := range []int64{1, 2} {
		if got[n] != 1 {
			t.Fatalf("sample n=%d reached the store %d time(s); counts=%v", n, got[n], got)
		}
	}
}

// TestFollowRefusesMalformedPathGlob pins the startup check on --path.
//
// filepath.Glob reports a malformed pattern as an error and poll returned
// on the first one, before reading a file, retiring a tailer or writing a
// checkpoint. A pattern is a constant, so that never cleared: one bad
// --path stopped every followed path for the life of the process while
// printing an ERROR line at the poll rate.
func TestFollowRefusesMalformedPathGlob(t *testing.T) {
	rs := newRecordingStore()
	t.Cleanup(rs.srv.Close)
	ing, sink := newFollowIngest(t, rs, t.TempDir())
	t.Cleanup(func() { _ = sink.Close(context.Background()) })

	dir := t.TempDir()
	good := filepath.Join(dir, "app.log")
	appendLines(t, good, 0, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := ing.Follow(ctx, FollowOptions{Paths: []string{good, filepath.Join(dir, "[")}})
	if err == nil {
		t.Fatal("Follow accepted a malformed --path glob")
	}
	if !strings.Contains(err.Error(), "--path") {
		t.Fatalf("the refusal does not name the flag: %v", err)
	}
	t.Logf("refused: %v", err)
}

// TestFollowerWarnThrottle pins the collapsing of a repeating failure.
//
// The sweep retries a path that cannot be opened on every tick, which is
// right; reporting it on every tick is a WARNING four times a second for
// as long as the fault lasts, which buries every other line the process
// produces. A key that has not been seen recently -- including a
// different error about the same path -- is still reported at once.
func TestFollowerWarnThrottle(t *testing.T) {
	f := &follower{warnAt: map[string]time.Time{}}
	key := openWarnKey("/var/log/app.log")
	if !f.shouldWarn(key) {
		t.Fatal("the first occurrence was not reported")
	}
	if f.shouldWarn(key) {
		t.Fatal("an immediate repeat was reported again")
	}
	if !f.shouldWarn("poll:a different fault") {
		t.Fatal("a different fault was withheld")
	}
	f.clearWarn(key)
	if !f.shouldWarn(key) {
		t.Fatal("the failure was still throttled after it had cleared")
	}
	// The map is keyed partly by error text, so it is bounded.
	for i := 0; i < maxWarnKeys*2; i++ {
		f.shouldWarn(fmt.Sprintf("poll:fault %d", i))
	}
	f.mu.Lock()
	n := len(f.warnAt)
	f.mu.Unlock()
	if n > maxWarnKeys+1 {
		t.Fatalf("throttle map grew to %d entries, past the %d-entry bound", n, maxWarnKeys)
	}
}
