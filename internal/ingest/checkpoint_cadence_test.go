package ingest

import (
	"testing"
	"time"
)

// A resume record is a write, an fsync, a rename and an fsync of the
// directory, and the delivery observer fires on every acked flush -- as
// often as the batch fills, which on a busy follow is many times a
// second, per file, serialised on one mutex on the goroutine that also
// holds the sink's delivery lock. 02-ingest.md section 5 already says the
// record is "fsynced on a timer": the timer is what keeps the state
// directory from becoming the pipeline's throughput ceiling.
func TestCheckpointWritesAreCoalesced(t *testing.T) {
	tl := &tailer{path: "/x", cp: &Checkpoint{Stream: "s", Path: "/x"}}

	tl.pending = 100
	tl.markInflight()
	cp, ok := tl.commitInflight(1)
	if !ok {
		t.Fatal("the first commit must reach disk; there is no earlier record to lean on")
	}
	if cp.AckedOffset != 100 {
		t.Fatalf("acked %d, want 100", cp.AckedOffset)
	}

	// Everything inside the interval is folded into one owed write.
	for _, at := range []int64{200, 300, 400} {
		tl.pending = at
		tl.markInflight()
		if _, ok := tl.commitInflight(2); ok {
			t.Fatalf("offset %d was written inside the interval", at)
		}
	}
	// The in-memory position still advanced, which is what BeginFlush and
	// EndFlush reason about.
	if got := tl.ackedOffset(); got != 400 {
		t.Fatalf("in-memory acked %d, want 400", got)
	}
	// And the owed record carries the latest of them, not the first.
	cp, ok = tl.dueSave(true)
	if !ok {
		t.Fatal("the deferred record was dropped rather than owed")
	}
	if cp.AckedOffset != 400 {
		t.Fatalf("owed record says %d, want the latest committed 400", cp.AckedOffset)
	}
	if _, ok := tl.dueSave(true); ok {
		t.Fatal("the same record was owed twice")
	}

	// Once the interval has passed, a commit writes again.
	tl.mu.Lock()
	tl.savedAt = time.Now().Add(-2 * checkpointSaveInterval)
	tl.mu.Unlock()
	tl.pending = 500
	tl.markInflight()
	if _, ok := tl.commitInflight(3); !ok {
		t.Fatal("a commit past the interval must reach disk")
	}

	// A frozen tailer still writes nothing at all.
	tl.pending = 600
	tl.markInflight()
	tl.mu.Lock()
	tl.holed = true
	tl.mu.Unlock()
	if _, ok := tl.commitInflight(4); ok {
		t.Fatal("a holed tailer committed")
	}
	if _, ok := tl.dueSave(true); ok {
		t.Fatal("a holed tailer owed a record")
	}
}

// The remote follower keeps its resume record in a checkpointBox rather
// than on a tailer, and its observer fires just as often.
func TestRemoteCheckpointWritesAreCoalesced(t *testing.T) {
	box := &checkpointBox{cp: &Checkpoint{Stream: "s", Path: "h:/x"}}
	if _, ok := box.advance(100); !ok {
		t.Fatal("the first advance must reach disk")
	}
	if _, ok := box.advance(200); ok {
		t.Fatal("an advance inside the interval was written")
	}
	if got := box.ackedOffset(); got != 200 {
		t.Fatalf("in-memory acked %d, want 200", got)
	}
	cp, ok := box.dueSave(true)
	if !ok || cp.AckedOffset != 200 {
		t.Fatalf("owed record %v/%v, want offset 200", cp.AckedOffset, ok)
	}

	// A rotation rewind is never deferred: the offsets on disk stop
	// describing the file the moment it happens.
	if cp := box.forceAdvance(0); cp.AckedOffset != 0 {
		t.Fatalf("forceAdvance gave %d, want 0", cp.AckedOffset)
	}
	if _, ok := box.dueSave(true); ok {
		t.Fatal("a forced advance left a record owed behind it")
	}

	// Recording the file identity writes the whole record, offsets and
	// all, so it discharges anything owed.
	if _, ok := box.advance(300); ok {
		t.Fatal("an advance inside the interval was written")
	}
	if cp := box.setIdentity("ino:7"); cp.AckedOffset != 300 || cp.Fingerprint != "ino:7" {
		t.Fatalf("setIdentity gave %+v", cp)
	}
	if _, ok := box.dueSave(true); ok {
		t.Fatal("setIdentity left a record owed behind it")
	}
}
