package ingest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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
