package ingest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A writer that is killed mid-line, or a rotation that lands between a
// record and its newline, leaves the last bytes of a file with no
// terminator. The read loop holds those back on purpose -- more may be
// coming -- but once the handle has been rotated away nothing more is
// coming, and every other acquisition path extracts such a record: batch
// import processes it, the TCP listener processes it when the sender
// closes cleanly. The follower used to close the handle over it, so the
// same file produced different data depending on how it was read.
func TestRotationDeliversTheFinalUnterminatedLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

	appendLines(t, path, 0, 5)
	// The sixth record arrives without its newline, as a killed writer
	// leaves one.
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(f, "%d n=5", base+5000); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	runFollowFor(t, ing, sink, path, func() {
		if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 5 }) {
			t.Fatalf("the terminated lines never arrived: got %d", len(rs.counts()))
		}
		// Rename the handle away: those bytes can never grow again.
		if err := os.Rename(path, path+".1"); err != nil {
			t.Fatalf("rename: %v", err)
		}
		appendLines(t, path, 6, 10)
		if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 10 }) {
			t.Logf("post-rotation lines: %d", len(rs.counts()))
		}
	})

	counts := rs.counts()
	if counts[5] == 0 {
		t.Fatalf("the final unterminated record of the rotated file was dropped; delivered %v", counts)
	}
	if counts[5] > 1 {
		t.Fatalf("the final unterminated record was delivered %d times", counts[5])
	}
}

// The mirror property: a partial line on a file that is still being
// written must *not* be delivered early. Only a handle that has been
// rotated away is final.
func TestALivePartialLineIsNotDeliveredEarly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

	appendLines(t, path, 0, 3)
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Half of record 3: the value is still being written.
	if _, err := fmt.Fprintf(f, "%d n=3", base+3000); err != nil {
		t.Fatal(err)
	}
	runFollowFor(t, ing, sink, path, func() {
		if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 3 }) {
			t.Fatalf("the terminated lines never arrived: got %d", len(rs.counts()))
		}
		// Finish the record: 3 becomes 37, so delivering the prefix early
		// would store a number the source never reported.
		if _, err := fmt.Fprintf(f, "7\n"); err != nil {
			t.Fatal(err)
		}
		if !waitFor(t, 5*time.Second, func() bool { return rs.counts()[37] > 0 }) {
			t.Fatalf("the completed record never arrived: %v", rs.counts())
		}
	})
	_ = f.Close()
	if n := rs.counts()[3]; n != 0 {
		t.Fatalf("a partial line on a live file was delivered %d time(s) as the value 3", n)
	}
}
