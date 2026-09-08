package ingest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

// fakeSSH writes a stand-in for the ssh client: it records the remote
// command it was asked to run and then serves it locally, so the remote
// follower can be exercised without a host to reach.
func fakeSSH(t *testing.T, dir, file string) (bin, log string) {
	t.Helper()
	for _, tool := range []string{"sh", "dd", "sed", "wc"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not available, so the ssh stand-in cannot run", tool)
		}
	}
	log = filepath.Join(dir, "ssh.log")
	bin = filepath.Join(dir, "fake-ssh")
	// `dd` rather than `tail -F`: the offset is what this stands in for,
	// and a real follow would never exit, which a test cannot wait on.
	script := fmt.Sprintf(`#!/bin/sh
last=""
for a in "$@"; do last="$a"; done
echo "$last" >> %q
case "$last" in
  *"wc -c"*) wc -c < %q ;;
  *) off=$(echo "$last" | sed -n 's/^tail -c +\([0-9][0-9]*\).*/\1/p')
     [ -z "$off" ] && off=1
     dd bs=1 skip=$((off-1)) if=%q 2>/dev/null ;;
esac
`, log, file, file)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write ssh stand-in: %v", err)
	}
	return bin, log
}

func sshCommands(t *testing.T, log string) []string {
	t.Helper()
	b, err := os.ReadFile(log)
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func runRemoteFollowFor(t *testing.T, ing *Ingest, opts RemoteOptions, until func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ing.FollowRemote(ctx, opts) }()
	waitFor(t, 5*time.Second, until)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("FollowRemote did not return after its context was cancelled")
	}
}

// --start-at end used to be honoured on every start of a remote follow,
// discarding a perfectly good checkpoint: a restart skipped everything
// written while the follower was down, while the identical local command
// resumed. The flag now means what it means locally -- where to start when
// there is nothing to resume from.
func TestRemoteFollowResumesFromItsCheckpoint(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	appendLines(t, logPath, 0, 5)
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Resume after the first two lines.
	acked, seen := int64(0), 0
	for i, b := range body {
		if b == '\n' {
			seen++
			if seen == 2 {
				acked = int64(i + 1)
				break
			}
		}
	}
	stateDir := filepath.Join(dir, "state")
	cps, err := NewCheckpointStore(stateDir)
	if err != nil {
		t.Fatalf("checkpoints: %v", err)
	}
	target := "h:" + logPath
	if err := cps.Save(&Checkpoint{Stream: StreamID(target), Path: target, Offset: acked, AckedOffset: acked}); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	bin, cmdLog := fakeSSH(t, dir, logPath)
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, stateDir)
	defer func() { _ = sink.Close(context.Background()) }()

	runRemoteFollowFor(t, ing, RemoteOptions{
		Host: "h", Paths: []string{logPath}, StartAt: "end", SSHBinary: bin,
		ProbeInterval: time.Hour, ReconnectBackoff: 50 * time.Millisecond,
	}, func() bool {
		c := rs.counts()
		return c[2] > 0 && c[3] > 0 && c[4] > 0
	})

	want := fmt.Sprintf("tail -c +%d", acked+1)
	var first string
	for _, c := range sshCommands(t, cmdLog) {
		if strings.HasPrefix(c, "tail ") {
			first = c
			break
		}
	}
	if !strings.HasPrefix(first, want) {
		t.Errorf("the tail started with %q, want %q: the checkpoint was discarded", first, want)
	}
	got := rs.counts()
	for _, n := range []int64{0, 1} {
		if got[n] > 0 {
			t.Errorf("record %d was re-read even though the checkpoint was past it", n)
		}
	}
}

// A remote record longer than the cap with no terminator in it must be
// consumed, exactly as the local follower consumes one: waiting for a
// newline that is not coming re-read the same bytes on every reconnect and
// never advanced.
func TestRemoteFollowConsumesAnUnterminatedOversizeRecord(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	line := fmt.Sprintf("%d n=7", base) + strings.Repeat("z", 400)
	if err := os.WriteFile(logPath, []byte(line), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	bin, _ := fakeSSH(t, dir, logPath)
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))
	defer func() { _ = sink.Close(context.Background()) }()

	runRemoteFollowFor(t, ing, RemoteOptions{
		Host: "h", Paths: []string{logPath}, SSHBinary: bin,
		MaxRecordBytes: 24, ProbeInterval: time.Hour, ReconnectBackoff: 50 * time.Millisecond,
	}, func() bool { return rs.counts()[7] > 0 })

	if rs.counts()[7] == 0 {
		t.Fatal("a record longer than the cap with no newline was never delivered")
	}
	if n := ing.Progress().Snapshot().OversizeRecords; n == 0 {
		t.Error("the truncation was not counted")
	}
}

// fakeSSHWithIdentity is fakeSSH plus the file identity the rotation probe
// reads: the stand-in answers the size and the inode of whatever the path
// names right now, which is what a rename-and-create rotation changes.
func fakeSSHWithIdentity(t *testing.T, dir, file string) string {
	t.Helper()
	for _, tool := range []string{"sh", "dd", "sed", "wc", "ls"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not available, so the ssh stand-in cannot run", tool)
		}
	}
	bin := filepath.Join(dir, "fake-ssh-ident")
	script := fmt.Sprintf(`#!/bin/sh
last=""
for a in "$@"; do last="$a"; done
case "$last" in
  *"wc -c"*) wc -c < %q; ls -Li %q ;;
  *) off=$(echo "$last" | sed -n 's/^tail -c +\([0-9][0-9]*\).*/\1/p')
     [ -z "$off" ] && off=1
     dd bs=1 skip=$((off-1)) if=%q 2>/dev/null ;;
esac
`, file, file, file)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write ssh stand-in: %v", err)
	}
	return bin
}

// A rename-and-create rotation replaces the file under the tail while the
// byte offsets keep climbing from the old one, so the acknowledged offset
// ends up naming a position in a file that never held those bytes. Size
// alone cannot see it: by the time the next probe runs the replacement is
// usually already longer than the offset, so the "shorter than what we
// read" test finds nothing wrong and the reconnect resumes past the head
// of the new file -- a silent skip.
func TestRemoteFollowDetectsARotationThatGrewPastTheOffset(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	appendLines(t, logPath, 0, 30)
	bin := fakeSSHWithIdentity(t, dir, logPath)
	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newFollowIngest(t, rs, filepath.Join(dir, "state"))
	defer func() { _ = sink.Close(context.Background()) }()

	opts := RemoteOptions{
		Host: "h", Paths: []string{logPath}, StartAt: "beginning", SSHBinary: bin,
		ProbeInterval: 50 * time.Millisecond, ReconnectBackoff: 50 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ing.FollowRemote(ctx, opts) }()

	if !waitFor(t, 5*time.Second, func() bool { return len(rs.counts()) >= 30 }) {
		cancel()
		<-done
		t.Fatalf("first pass incomplete: %d line(s)", len(rs.counts()))
	}
	// Rotate: the old file is renamed away and a longer one takes its
	// name, so every offset the checkpoint holds is meaningless and the
	// new file is *not* shorter than the bytes already read.
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	appendLines(t, logPath, 100, 160)

	ok := waitFor(t, 10*time.Second, func() bool {
		counts := rs.counts()
		for i := int64(100); i < 160; i++ {
			if counts[i] == 0 {
				return false
			}
		}
		return true
	})
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("FollowRemote did not return after its context was cancelled")
	}
	if !ok {
		counts := rs.counts()
		var missing []int64
		for i := int64(100); i < 160; i++ {
			if counts[i] == 0 {
				missing = append(missing, i)
			}
		}
		t.Fatalf("%d line(s) of the replacement file were skipped, starting at %d", len(missing), missing[0])
	}
}

// The spec used by the checkpoint test below: one aggregation window an
// hour wide, so it never closes on its own and the extractor is still
// holding the records when the connection ends.
const remoteAggSpec = `
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
        aggregate: {every: 1h, field: total, mode: increment}
`

// droppingSSH is fakeSSH with a tail that streams and then fails, which
// is what a dropped connection looks like from this side.
func droppingSSH(t *testing.T, dir, file string) string {
	t.Helper()
	for _, tool := range []string{"sh", "dd", "sed", "wc"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not available, so the ssh stand-in cannot run", tool)
		}
	}
	bin := filepath.Join(dir, "dropping-ssh")
	script := fmt.Sprintf(`#!/bin/sh
last=""
for a in "$@"; do last="$a"; done
case "$last" in
  *"wc -c"*) wc -c < %q ;;
  *) off=$(echo "$last" | sed -n 's/^tail -c +\([0-9][0-9]*\).*/\1/p')
     [ -z "$off" ] && off=1
     dd bs=1 skip=$((off-1)) if=%q 2>/dev/null
     exit 1 ;;
esac
`, file, file)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write ssh stand-in: %v", err)
	}
	return bin
}

// A dropped connection must still checkpoint the bytes behind the flush
// it just delivered.
//
// The resume offset was only published when the tail exited cleanly, and
// a tail that exits cleanly is the rare case: `tail -F` does not return.
// So the ordinary path -- the connection drops, the extractor is flushed
// into the sink, the loop reconnects -- delivered a window and then
// resumed from before the records it was built from, rebuilt it and
// delivered it again. Content keying collapses the two; under
// `key: offset` the second copy carries a different flush hint and lands
// as a duplicate row. Every clean shutdown did the same thing.
func TestRemoteFollowCheckpointsWhatItFlushedAfterADroppedConnection(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	appendLines(t, logPath, 0, 5)
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	stateDir := filepath.Join(dir, "state")
	cps, err := NewCheckpointStore(stateDir)
	if err != nil {
		t.Fatalf("checkpoints: %v", err)
	}
	target := "h:" + logPath
	stream := StreamID(target)

	rs := newRecordingStore()
	defer rs.srv.Close()
	ing, sink := newRemoteIngest(t, remoteAggSpec, rs, stateDir)
	defer func() { _ = sink.Close(context.Background()) }()

	bin := droppingSSH(t, dir, logPath)
	runRemoteFollowFor(t, ing, RemoteOptions{
		Host: "h", Paths: []string{logPath}, SSHBinary: bin,
		ProbeInterval: time.Hour, ReconnectBackoff: 50 * time.Millisecond,
	}, func() bool {
		cp, ok := cps.Load(stream)
		return ok && cp.AckedOffset >= info.Size()
	})

	cp, ok := cps.Load(stream)
	if !ok {
		t.Fatal("no checkpoint was written at all, so every reconnect re-reads the whole file and re-emits the window it just delivered")
	}
	if cp.AckedOffset != info.Size() {
		t.Fatalf("checkpoint is at %d of %d bytes: the window was delivered but the bytes it came from were never acknowledged, so the reconnect rebuilds and re-delivers it", cp.AckedOffset, info.Size())
	}
}

// newRemoteIngest is newFollowIngest with the spec left to the caller, so
// a test can exercise a profile that buffers records rather than emitting
// one per line.
func newRemoteIngest(t *testing.T, specBody string, rs *recordingStore, stateDir string) (*Ingest, *Sink) {
	t.Helper()
	spec, err := extract.Parse([]byte(specBody))
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
