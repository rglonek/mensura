package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

// An aggregating profile, so the extractor really holds state between
// records: a window is what an early flush turns into a row nobody
// measured.
const rewindAggSpec = `
version: 1
profiles:
  - name: agg
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    labels: [op]
    patterns:
      - set: lines
        search: 'n='
        extract: ['op=(?P<op>\w+) n=(?P<n>\d+)']
        aggregate: {every: 1ms, on: [op], field: hits, mode: increment}
`

func newAggFollower(t *testing.T, dir string, opts FollowOptions) *follower {
	t.Helper()
	srv := acceptingStore(t)
	spec, err := extract.Parse([]byte(rewindAggSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(srv.URL, "")
	client.Compress = false
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	cps, err := NewCheckpointStore(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("checkpoints: %v", err)
	}
	if opts.MaxRecordBytes == 0 {
		opts.MaxRecordBytes = defaultMaxRecordBytes
	}
	return &follower{ing: ing, opts: opts, cps: cps,
		tailers: map[string]*tailer{}, noProfile: map[string]time.Time{}}
}

func aggFollowerWithOpenWindow(t *testing.T) (*follower, *tailer) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "a.log")
	if err := os.WriteFile(path, []byte("1700000000000 op=a n=1\n1700000000001 op=a n=2\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	f := newAggFollower(t, dir, FollowOptions{Paths: []string{filepath.Join(dir, "*.log")}})
	if err := f.poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	tailers := f.snapshotTailers()
	if len(tailers) != 1 {
		t.Fatalf("expected one tailer, got %d", len(tailers))
	}
	return f, tailers[0]
}

// The control: with nothing owed, the idle flush really does close the
// half-filled window, so the assertion below is about the guard rather
// than about a flush that was never going to emit.
func TestIdleFlushEmitsWhenNoRewindIsOwed(t *testing.T) {
	f, _ := aggFollowerWithOpenWindow(t)
	before := f.ing.cfg.Progress.Snapshot().Samples
	time.Sleep(10 * time.Millisecond)
	f.flushIdle(context.Background())
	if got := f.ing.cfg.Progress.Snapshot().Samples; got <= before {
		t.Fatalf("the idle flush emitted nothing (%d samples), so the guard below proves nothing", got)
	}
}

// A tailer that owes a rewind is about to be seeked back and re-read, and
// applyRewind then discards whatever the extractor holds because it was
// built from those bytes. Emitting it first delivers a window the replay
// is about to rebuild -- and one closed early by an idle tick covers
// fewer records than the rebuilt one, so the pair lands at one timestamp
// with two different values and no key collapses them.
func TestIdleFlushIsSkippedWhileARewindIsOwed(t *testing.T) {
	f, tl := aggFollowerWithOpenWindow(t)
	tl.mu.Lock()
	tl.rewind = true
	tl.mu.Unlock()

	before := f.ing.cfg.Progress.Snapshot().Samples
	time.Sleep(10 * time.Millisecond)
	f.flushIdle(context.Background())
	if got := f.ing.cfg.Progress.Snapshot().Samples; got != before {
		t.Fatalf("the idle flush delivered %d sample(s) from a tailer that is about to re-read those bytes", got-before)
	}
	// And the rewind is still owed, so the poll goroutine can still
	// discharge it.
	if !tl.rewindOwed() {
		t.Fatal("the idle flush consumed the rewind request")
	}
}

// The same guard on the SSH path, which had none at all: the watcher
// goroutine's select can reach the idle tick before it reaches the rewind
// signal that is asking it to kill the connection.
func TestRemoteIdleFlushIsSkippedWhileARewindIsOwed(t *testing.T) {
	srv := acceptingStore(t)
	spec, err := extract.Parse([]byte(rewindAggSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(srv.URL, "")
	client.Compress = false
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	profile := spec.Profile("agg")
	labels := map[string]string{"host": "h"}

	feed := func(rs *remoteStream) {
		ex, err := spec.NewStream(profile, extract.StreamOptions{RefTime: time.Now()})
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		rs.open(ex, 0)
		for i, line := range []string{"1700000000000 op=a n=1", "1700000000001 op=a n=2"} {
			_, derr := rs.process(int64(i*24), line, func([]extract.Result) error { return nil }, int64((i+1)*24))
			if derr != nil {
				t.Fatalf("process: %v", derr)
			}
		}
	}

	// Control: no rewind owed, so the idle flush closes the window.
	rs := &remoteStream{}
	feed(rs)
	p := newRemoteProgress()
	p.set(0)
	before := ing.cfg.Progress.Snapshot().Samples
	ing.remoteFlushIdle(context.Background(), rs, "h:/x", labels, p, time.Now().Add(time.Second))
	if got := ing.cfg.Progress.Snapshot().Samples; got <= before {
		t.Fatalf("the remote idle flush emitted nothing, so the guard below proves nothing")
	}

	// A hole that has cleared: the tail is about to be restarted from the
	// acknowledged offset, so nothing the extractor holds may be
	// delivered.
	rs2 := &remoteStream{}
	feed(rs2)
	p2 := newRemoteProgress()
	p2.set(0)
	p2.advance(48)
	p2.markInflight()
	if _, first := p2.markHoled(); !first {
		t.Fatal("markHoled did not freeze the offset")
	}
	if _, thawed := p2.clearHole(); !thawed {
		t.Fatal("clearHole did not thaw")
	}
	if !p2.rewindPending() {
		t.Fatal("clearHole left no rewind owed")
	}
	before = ing.cfg.Progress.Snapshot().Samples
	ing.remoteFlushIdle(context.Background(), rs2, "h:/x", labels, p2, time.Now().Add(time.Second))
	if got := ing.cfg.Progress.Snapshot().Samples; got != before {
		t.Fatalf("the remote idle flush delivered %d sample(s) from bytes the reconnect is about to read again", got-before)
	}
	if !p2.rewindPending() {
		t.Fatal("the idle flush discharged the rewind the reconnect owes")
	}
}
