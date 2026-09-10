package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
)

// The unmatched-line sample is the single most useful spec-debugging
// output there is, and only batch import ever collected it: MergeStream
// was called from processFile and nowhere else, so `first_unmatched` was
// empty for the whole life of a follow -- the mode that runs unattended,
// where "the spec matches nothing" is hardest to notice.
func TestFollowReportsUnmatchedLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// A timestamp the profile can read, and a body no pattern claims.
	for i := 0; i < 3; i++ {
		if _, err := fmt.Fprintf(f, "%d nothing here %d\n", base+int64(i)*1000, i); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	_ = f.Close()

	runFollowFor(t, ing, sink, path, func() {
		waitFor(t, 3*time.Second, func() bool {
			return ing.Progress().Snapshot().UnmatchedLines >= 3
		})
	})

	snap := ing.Progress().Snapshot()
	if snap.UnmatchedLines == 0 {
		t.Fatal("nothing was counted as unmatched, so the fixture is wrong")
	}
	if len(snap.FirstUnmatched) == 0 {
		t.Fatal("first_unmatched is empty: a followed file's unmatched lines never reach the progress document")
	}
	// Repeated merges must not multiply the sample: the tailer's extractor
	// keeps its own copy for the life of the stream, and the sweep merges
	// on every poll.
	seen := map[string]int{}
	for _, l := range snap.FirstUnmatched {
		seen[l]++
		if seen[l] > 1 {
			t.Fatalf("first_unmatched repeats %q: merging a live stream is not idempotent", l)
		}
	}
}

// MergeStream is called repeatedly by every continuous acquisition path,
// so merging one stream twice must add nothing the second time.
func TestMergeStreamIsIdempotent(t *testing.T) {
	p := NewProgress()
	st := &extract.Stats{FirstUnmatched: []string{"a", "b"}}
	for i := 0; i < 20; i++ {
		p.MergeStream(st)
	}
	got := p.Snapshot().FirstUnmatched
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("first_unmatched is %v, want [a b]: repeated merges accumulate duplicates", got)
	}
}

// Batch import classifies a file holding a NUL byte in its first block as
// binary and skips it (02-ingest.md section 4.2). Follow did not, so a
// glob like /var/log/* fed wtmp and every journal fragment to the
// profile's regexes on every poll.
func TestFollowSkipsBinaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	body := []byte(fmt.Sprintf("%d n=1\x00\n%d n=2\n", base, base+1000))
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))

	runFollowFor(t, ing, sink, path, func() {
		waitFor(t, 2*time.Second, func() bool {
			return ing.Progress().Snapshot().BinarySkipped > 0
		})
	})

	if n := ing.Progress().Snapshot().BinarySkipped; n == 0 {
		t.Error("a binary file was followed rather than skipped and counted")
	}
	if got := rs.counts(); len(got) > 0 {
		t.Errorf("samples were extracted from a binary file: %v", got)
	}
}

// The remote follower knows the file's size on every probe and its own
// read position, and published neither: lag_bytes read 0 for the whole
// life of an SSH follow, which is exactly what a caught-up tail reads
// like -- on the one number an operator watches to find out that it is
// not.
func TestRemoteFollowPublishesLag(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	appendLines(t, logPath, 0, 40)

	stateDir := filepath.Join(dir, "state")
	cps, err := NewCheckpointStore(stateDir)
	if err != nil {
		t.Fatalf("checkpoints: %v", err)
	}
	// A checkpoint at the head of the file, so the tail starts with the
	// whole of it still to read and the first probe has a backlog to
	// report.
	target := "h:" + logPath
	if err := cps.Save(&Checkpoint{Stream: StreamID(target), Path: target}); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	bin, _ := fakeSSH(t, dir, logPath)
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, stateDir)
	defer func() { _ = sink.Close(context.Background()) }()

	sawLag := false
	runRemoteFollowFor(t, ing, RemoteOptions{
		Host: "h", Paths: []string{logPath}, SSHBinary: bin,
		ProbeInterval: 20 * time.Millisecond, ReconnectBackoff: 20 * time.Millisecond,
	}, func() bool {
		if ing.Progress().Snapshot().LagBytes > 0 {
			sawLag = true
		}
		return sawLag && rs.counts()[39] > 0
	})

	if !sawLag {
		t.Error("lag_bytes never moved on an SSH follow with a whole file still to read")
	}
}
