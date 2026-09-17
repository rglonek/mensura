package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

// A tailer whose resume offset is frozen by a dropped batch is in the
// same position as one that has been asked to rewind: the acknowledged
// offset cannot move until a later flush thaws it, and the thaw asks for
// exactly this re-read. So an idle tick during the outage must not close
// the half-filled window -- the replay rebuilds it, longer, at the same
// timestamp and the same label set, and neither a content key nor an
// offset key collapses the pair.
func TestIdleFlushIsSkippedWhileTheOffsetIsFrozen(t *testing.T) {
	f, tl := aggFollowerWithOpenWindow(t)
	freeze(t, tl)

	before := f.ing.cfg.Progress.Snapshot().Samples
	time.Sleep(10 * time.Millisecond)
	f.flushIdle(context.Background())
	if got := f.ing.cfg.Progress.Snapshot().Samples; got != before {
		t.Fatalf("the idle flush delivered %d sample(s) from a tailer whose checkpoint is frozen; the replay rebuilds them", got-before)
	}
}

// A shutdown leaves every checkpoint where it is, so the next start
// re-reads from the frozen offset. closeAll used to hand the extractor's
// half-filled window to the sink anyway.
func TestCloseAllDiscardsAFrozenTailersBufferedState(t *testing.T) {
	f, tl := aggFollowerWithOpenWindow(t)
	freeze(t, tl)

	before := f.ing.cfg.Progress.Snapshot().Samples
	f.closeAll(context.Background())
	if got := f.ing.cfg.Progress.Snapshot().Samples; got != before {
		t.Fatalf("the shutdown drain delivered %d sample(s) that the next start will read again", got-before)
	}
}

// The control for both: with nothing frozen, the shutdown drain really
// does deliver, so the two assertions above are about the guard rather
// than about a flush that was never going to emit.
func TestCloseAllDeliversWhenNothingIsFrozen(t *testing.T) {
	f, _ := aggFollowerWithOpenWindow(t)
	before := f.ing.cfg.Progress.Snapshot().Samples
	f.closeAll(context.Background())
	if got := f.ing.cfg.Progress.Snapshot().Samples; got <= before {
		t.Fatalf("the shutdown drain emitted nothing (%d samples), so the guards above prove nothing", got)
	}
}

// A path that has left the followed glob keeps its resume record, so the
// same rule applies to it: the sweep that retires it must not emit a
// window the path's return will rebuild.
func TestRetireKeepingCheckpointDiscardsAFrozenTailersBufferedState(t *testing.T) {
	f, tl := aggFollowerWithOpenWindow(t)
	freeze(t, tl)

	before := f.ing.cfg.Progress.Snapshot().Samples
	f.retireKeepingCheckpoint(context.Background(), tl)
	if got := f.ing.cfg.Progress.Snapshot().Samples; got != before {
		t.Fatalf("retiring a frozen tailer delivered %d sample(s) that its return will read again", got-before)
	}
}

// A rotation is the other half of the rule and must keep delivering: the
// renamed file is gone, its checkpoint is rewound to zero for the
// replacement, and nothing re-reads the bytes the extractor was filled
// from. Discarding there would lose the old file's tail outright.
func TestRetireAfterRotationStillDeliversBufferedState(t *testing.T) {
	f, tl := aggFollowerWithOpenWindow(t)
	freeze(t, tl)

	before := f.ing.cfg.Progress.Snapshot().Samples
	f.retire(context.Background(), tl)
	if got := f.ing.cfg.Progress.Snapshot().Samples; got <= before {
		t.Fatalf("a rotation dropped the old file's buffered window; it is the only copy of those records")
	}
}

// freeze puts a tailer in the state a dropped batch leaves it in: bytes
// in flight past the acknowledged offset, and the offset frozen there.
func freeze(t *testing.T, tl *tailer) {
	t.Helper()
	tl.mu.Lock()
	tl.pending, tl.inflight, tl.acked = 48, 48, 0
	tl.mu.Unlock()
	if !tl.markHoled() {
		t.Fatal("markHoled did not freeze the offset")
	}
	if !tl.replayOwed() {
		t.Fatal("a holed tailer does not report a replay owed")
	}
}

// The SSH path's idle flush, on the same rule. The watcher goroutine
// keeps ticking for as long as the outage lasts, and until now every one
// of those ticks could close a window the reconnect rebuilds.
func TestRemoteIdleFlushIsSkippedWhileTheOffsetIsFrozen(t *testing.T) {
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

	rs := &remoteStream{}
	ex, err := spec.NewStream(profile, extract.StreamOptions{RefTime: time.Now()})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	rs.open(ex, 0)
	for i, line := range []string{"1700000000000 op=a n=1", "1700000000001 op=a n=2"} {
		if _, derr := rs.process(int64(i*24), line, func([]extract.Result) error { return nil }, int64((i+1)*24)); derr != nil {
			t.Fatalf("process: %v", derr)
		}
	}

	p := newRemoteProgress()
	p.set(0)
	p.advance(48)
	p.markInflight()
	if _, first := p.markHoled(); !first {
		t.Fatal("markHoled did not freeze the offset")
	}
	if p.rewindPending() {
		t.Fatal("a freeze is not a rewind yet; this test is about the window before it")
	}
	if !p.replayPending() {
		t.Fatal("a frozen remote path does not report a replay pending")
	}

	before := ing.cfg.Progress.Snapshot().Samples
	ing.remoteFlushIdle(context.Background(), rs, "h:/x", labels, p, time.Now().Add(time.Second))
	if got := ing.cfg.Progress.Snapshot().Samples; got != before {
		t.Fatalf("the remote idle flush delivered %d sample(s) from bytes the reconnect will read again", got-before)
	}
}
