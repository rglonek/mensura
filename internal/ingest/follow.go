package ingest

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
)

// FollowOptions configure a continuous tail.
type FollowOptions struct {
	Paths []string
	// StartAt is "checkpoint" (default), "beginning" or "end".
	StartAt string
	// PollInterval is how often a quiet file is re-checked.
	PollInterval time.Duration
	// IdleFlush bounds how long a partial multiline record or a
	// half-filled aggregation window may wait.
	IdleFlush time.Duration
	// MaxRecordBytes bounds one record. A longer one is truncated and
	// counted rather than buffered without limit.
	MaxRecordBytes int
}

// Follow tails files continuously, handling rotation, until the context is
// cancelled. Delivery is at-least-once; with content-addressed row keys the
// observable result for a replayed line is exactly-once.
func (i *Ingest) Follow(ctx context.Context, opts FollowOptions) error {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 250 * time.Millisecond
	}
	if opts.IdleFlush <= 0 {
		opts.IdleFlush = 30 * time.Second
	}
	if opts.MaxRecordBytes <= 0 {
		opts.MaxRecordBytes = defaultMaxRecordBytes
	}
	cps, err := NewCheckpointStore(i.cfg.StateDir)
	if err != nil {
		return err
	}
	cps.Log = i.cfg.Log
	f := &follower{
		ing: i, opts: opts, cps: cps,
		tailers:   map[string]*tailer{},
		noProfile: map[string]time.Time{},
	}
	// Checkpoints advance only for bytes the store has accepted.
	i.cfg.Sink.Observe(f)

	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()
	idle := time.NewTicker(opts.IdleFlush)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
			// Drain the extractors and close the handles, then flush. The
			// flush is what advances the checkpoints, through the
			// observer, so nothing is acknowledged before it is
			// delivered. Committing here and flushing afterwards -- the
			// old order -- marked the final bytes as accepted even when
			// that last flush failed against a store that was already
			// gone, which is the normal ordering in a rolling restart.
			f.closeAll(ctx)
			return i.cfg.Sink.Flush(context.Background())
		case <-idle.C:
			f.flushIdle(ctx)
		case <-ticker.C:
			if err := f.poll(ctx); err != nil {
				i.cfg.Log.Printf("ERROR follow: %v", err)
			}
		}
	}
}

// noProfileRetry is how long a path that matched no profile is left alone
// before it is looked at again.
const noProfileRetry = time.Minute

type follower struct {
	ing  *Ingest
	opts FollowOptions
	cps  *CheckpointStore

	mu      sync.Mutex
	tailers map[string]*tailer
	// noProfile maps a path to the time it may be reconsidered.
	noProfile map[string]time.Time
}

// tailer follows one file.
//
// offset is touched only by the poll goroutine, but pending and acked are
// read by commit(), which runs on the sink's flush goroutine, so those two
// are guarded.
type tailer struct {
	path        string
	stream      string
	file        *os.File
	fingerprint string
	// fingerprintAt is how many bytes fingerprint covers. It is the
	// number of bytes already consumed, capped, so the hash is stable
	// under append and changes the moment consumed bytes are rewritten.
	fingerprintAt int
	offset        int64
	labels        map[string]string
	ex            *extract.Stream
	cp            *Checkpoint
	// flushSeq numbers the flushes of buffered extractor state, which
	// have no byte offset of their own to key on.
	flushSeq int

	mu       sync.Mutex
	pending  int64
	inflight int64
	acked    int64
	// holed records that a batch was dropped while these bytes were in
	// flight. Once that has happened the resume offset may never move
	// again for this tailer: a checkpoint is a single offset, so
	// advancing it past the gap would bury the lost records for good.
	holed bool
}

func (t *tailer) setPending(n int64) {
	t.mu.Lock()
	t.pending = n
	t.mu.Unlock()
}

// markInflight snapshots how far the reader had got at the moment the sink
// took the batch. Only these bytes may be acknowledged when that batch
// commits.
//
// The snapshot is what makes the acknowledgement honest. Promoting the
// live read offset after the write instead acknowledged bytes whose
// samples were still buffered for the *next* batch, so a crash in that
// window lost them.
func (t *tailer) markInflight() {
	t.mu.Lock()
	t.inflight = t.pending
	t.mu.Unlock()
}

// commitInflight promotes the snapshot to acked and returns the checkpoint
// to write, or reports false when there is nothing new to persist. The
// record is built and copied under the lock: two flushes can land at once,
// and mutating the shared Checkpoint while another goroutine marshals it
// is a race.
func (t *tailer) commitInflight(now int64) (Checkpoint, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.holed || t.inflight <= t.acked {
		return Checkpoint{}, false
	}
	t.acked = t.inflight
	t.cp.Offset = t.acked
	t.cp.AckedOffset = t.acked
	t.cp.UpdatedUnix = now
	return *t.cp, true
}

// markHoled freezes the resume offset after a dropped batch, and reports
// whether this was the first time.
//
// A tailer that had nothing in flight cannot have contributed to the lost
// batch, so it is left alone. Freezing every live tailer stopped
// checkpointing for the whole process -- permanently, since holed is
// never cleared -- because one file produced one bad record.
func (t *tailer) markHoled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.holed || t.inflight <= t.acked {
		return false
	}
	t.holed = true
	return true
}

// checkpointSnapshot copies the checkpoint under the lock. The flush
// goroutine writes these same fields in commitInflight and
// setFingerprint, so copying the struct without the lock is a race.
func (t *tailer) checkpointSnapshot() Checkpoint {
	t.mu.Lock()
	defer t.mu.Unlock()
	return *t.cp
}

// setFingerprint records a new content fingerprint, and the width it
// covers, on the checkpoint.
func (t *tailer) setFingerprint(fp string, width int) {
	t.mu.Lock()
	t.fingerprint, t.fingerprintAt = fp, width
	t.cp.Fingerprint, t.cp.FingerprintBytes = fp, width
	t.mu.Unlock()
}

func (t *tailer) reset(to int64) {
	t.mu.Lock()
	t.pending, t.inflight, t.acked = to, to, to
	t.mu.Unlock()
	t.offset = to
}

func (f *follower) poll(ctx context.Context) error {
	paths := map[string]struct{}{}
	for _, pattern := range f.opts.Paths {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return err
		}
		for _, m := range matches {
			paths[m] = struct{}{}
		}
	}
	// One failing file must not stop the others from being read: the
	// first error is reported, the rest of the sweep still happens.
	ordered := make([]string, 0, len(paths))
	for p := range paths {
		ordered = append(ordered, p)
	}
	sort.Strings(ordered)
	var firstErr error
	for _, p := range ordered {
		t, err := f.ensure(p)
		if err != nil {
			f.ing.cfg.Log.Printf("WARNING cannot follow %s: %v", p, err)
			continue
		}
		if t == nil {
			continue
		}
		// Rotation is checked before reading: a copytruncate followed
		// quickly by fresh appends can restore the size, so "smaller than
		// our offset" is not a reliable test on its own.
		if err := f.checkRotation(ctx, t); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := f.read(ctx, t); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}
	return firstErr
}

func (f *follower) ensure(path string) (*tailer, error) {
	f.mu.Lock()
	t, ok := f.tailers[path]
	if ok && t != nil {
		f.mu.Unlock()
		return t, nil
	}
	// A path that matched no profile is retried on a slow cadence rather
	// than abandoned: the file may be replaced by one that does match, or
	// the spec may be different next time this process starts.
	if until, skipping := f.noProfile[path]; skipping {
		if time.Now().Before(until) {
			f.mu.Unlock()
			return nil, nil
		}
		delete(f.noProfile, path)
	}
	f.mu.Unlock()
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := fh.Stat()
	if err != nil {
		_ = fh.Close()
		return nil, err
	}
	stream := StreamID(path)
	cp, had := f.cps.Load(stream)
	if !had {
		cp = &Checkpoint{Stream: stream, Path: path}
	}
	var start int64
	switch {
	case had && f.opts.StartAt != "beginning" && fingerprintMatches(fh, cp):
		// Same file as last time: resume where the store last acknowledged.
		start = cp.AckedOffset
	case f.opts.StartAt == "end":
		start = info.Size()
	default:
		start = 0
	}
	if start > info.Size() {
		start = 0
	}
	if _, err := fh.Seek(start, io.SeekStart); err != nil {
		_ = fh.Close()
		return nil, err
	}

	full := make([]byte, 64<<10)
	hn, _ := fh.ReadAt(full, 0)
	profile := f.ing.cfg.Spec.SelectProfile(path, full[:hn], f.ing.cfg.Labels, "")
	if profile == nil {
		_ = fh.Close()
		f.ing.cfg.Progress.NoProfile(path)
		f.ing.cfg.Log.Printf("WARNING no profile matched %s; not following it (retrying in %s)", path, noProfileRetry)
		f.mu.Lock()
		f.noProfile[path] = time.Now().Add(noProfileRetry)
		f.mu.Unlock()
		return nil, nil
	}
	ex, err := f.ing.cfg.Spec.NewStream(profile, extract.StreamOptions{
		RefTime: info.ModTime(), From: f.ing.cfg.From, To: f.ing.cfg.To,
	})
	if err != nil {
		_ = fh.Close()
		return nil, err
	}
	f.ing.cfg.Sink.DeclareFields(profile)

	t = &tailer{
		path: path, stream: stream, file: fh,
		labels: f.ing.streamLabels(path, full[:hn]), ex: ex, cp: cp,
	}
	t.reset(start)
	// The fingerprint covers what has been consumed, so it is taken from
	// the resume point rather than from a fixed prefix.
	if width := fingerprintWidth(start); width > 0 {
		fp, n := fingerprintAt(fh, width)
		if n == width {
			t.setFingerprint(fp, width)
		}
	}
	f.mu.Lock()
	f.tailers[path] = t
	f.mu.Unlock()
	return t, nil
}

// read consumes whatever new bytes exist, holding back a trailing partial
// line until its newline arrives.
//
// A partial line is held back by *rewinding only*: the offset stays before
// the incomplete bytes and the next pass re-reads them whole. Buffering
// them as well would prepend a stale copy to the completed line, so a line
// that was read mid-write would be delivered with a duplicated prefix.
func (f *follower) read(ctx context.Context, t *tailer) error {
	if t == nil || t.file == nil {
		return nil
	}
	if _, err := t.file.Seek(t.offset, io.SeekStart); err != nil {
		return err
	}
	max := f.opts.MaxRecordBytes
	r := bufio.NewReaderSize(t.file, 64<<10)
	for {
		rec, err := readRecord(r, max)
		oversizeUnterminated := false
		if !rec.Terminated {
			if rec.Consumed <= max {
				// Incomplete: leave the offset where it was and stop.
				return f.atEOF(t)
			}
			// No newline within the cap. Consuming it is the only way
			// out: holding it back for a terminator that is not
			// coming would re-buffer the same bytes on every poll,
			// which for a large newline-free file means reading it
			// into memory again and again. The truncated prefix is
			// still extracted, which is what batch import does with
			// the same record; discarding it here meant the same file
			// produced different data depending on how it was read.
			oversizeUnterminated = true
		}
		recStart := t.offset
		t.offset += int64(rec.Consumed)
		if rec.Oversize || oversizeUnterminated {
			f.ing.cfg.Progress.OversizeRecord()
		}
		f.ing.cfg.Progress.AddBytes(int64(rec.Consumed))
		results, perr := t.ex.Process(string(rec.Line))
		f.ing.recordOutcome(perr)
		for n, res := range results {
			if err := f.ing.cfg.Sink.Add(ctx, res, t.labels, keyHint(t.stream, offsetPos(recStart), n)); err != nil {
				return err
			}
		}
		if oversizeUnterminated {
			t.setPending(t.offset)
			continue
		}
		// pending is only advanced once every sample from these bytes
		// has been handed to the sink, so a commit can never
		// acknowledge a byte whose samples are still unqueued.
		t.setPending(t.offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return f.atEOF(t)
			}
			return err
		}
	}
}

func (f *follower) atEOF(t *tailer) error {
	if info, serr := t.file.Stat(); serr == nil {
		f.ing.cfg.Progress.SetLag(info.Size() - t.offset)
	}
	return nil
}

// checkRotation detects the five rotation shapes: rename+create,
// create+delete, truncate, copytruncate, and a fresh file at the same
// path. The open handle is always drained to EOF before it is replaced,
// so no tail is lost.
func (f *follower) checkRotation(ctx context.Context, t *tailer) error {
	if t == nil || t.file == nil {
		return nil
	}
	openInfo, err := t.file.Stat()
	if err != nil {
		return err
	}
	// The bytes behind the read offset are the ones that must not change.
	// Comparing at the stored width -- not at whatever the file is now --
	// is what lets this work on a file of any size, including one that
	// was copytruncated and immediately grew back to its old length.
	rewritten := openInfo.Size() < t.offset
	if !rewritten && t.fingerprintAt > 0 {
		current, n := fingerprintAt(t.file, t.fingerprintAt)
		if n < t.fingerprintAt || current != t.fingerprint {
			rewritten = true
		}
	}
	if rewritten {
		// A truncate, a copytruncate, or a rewrite in place: the bytes we
		// were reading are gone. Content is what decides, because a
		// truncate followed by fresh appends restores the size.
		f.ing.cfg.Log.Printf("INFO %s was truncated or rewritten; re-reading from the start", t.path)
		// Drain the extractor first. Carrying a half-built multiline
		// record across the boundary concatenated the old file's tail
		// onto the new file's first lines.
		f.drainExtractor(ctx, t)
		t.reset(0)
		t.setFingerprint("", 0)
		return nil
	}
	// Widen the window as more of the file is consumed, up to the cap.
	if width := fingerprintWidth(t.offset); width > t.fingerprintAt {
		if fp, n := fingerprintAt(t.file, width); n == width {
			t.setFingerprint(fp, width)
		}
	}
	pathInfo, err := os.Stat(t.path)
	if os.IsNotExist(err) {
		// The file was unlinked. Drain what is left, then wait for the path
		// to come back.
		if err := f.read(ctx, t); err != nil {
			return err
		}
		f.retire(ctx, t)
		return nil
	}
	if err != nil {
		return err
	}
	if !os.SameFile(openInfo, pathInfo) {
		// rename+create: drain the old handle first, then start the new
		// file from its beginning.
		if err := f.read(ctx, t); err != nil {
			return err
		}
		f.ing.cfg.Log.Printf("INFO %s rotated; switching to the new file", t.path)
		f.retire(ctx, t)
	}
	return nil
}

// drainExtractor flushes whatever the extractor still holds -- an open
// multiline record, a half-filled aggregation window -- into the sink.
func (f *follower) drainExtractor(ctx context.Context, t *tailer) {
	results := t.ex.Flush()
	if len(results) == 0 {
		return
	}
	t.flushSeq++
	pos := flushPos(t.flushSeq)
	for n, r := range results {
		_ = f.ing.cfg.Sink.Add(ctx, r, t.labels, keyHint(t.stream, pos, n))
	}
}

func (f *follower) retire(ctx context.Context, t *tailer) {
	f.drainExtractor(ctx, t)
	_ = t.file.Close()
	// Clearing the handle is what makes the retired tailer inert. poll
	// calls read() again right after checkRotation returns, and a closed
	// but non-nil handle turned every single rotation into a bogus
	// "ERROR follow: seek ...: file already closed" -- which also became
	// the sweep's first error and hid any real failure behind it.
	t.file = nil
	f.mu.Lock()
	delete(f.tailers, t.path)
	f.mu.Unlock()
	// The replacement file starts from its beginning. The checkpoint is
	// rewritten by the next successful flush; it is not forced here,
	// because nothing has been delivered yet.
	t.reset(0)
	cp := t.checkpointSnapshot()
	cp.Offset, cp.AckedOffset, cp.UpdatedUnix = 0, 0, time.Now().Unix()
	if err := f.cps.Save(&cp); err != nil {
		f.ing.cfg.Log.Printf("ERROR saving checkpoint for %s: %v", t.path, err)
	}
}

// BeginFlush snapshots every tailer's read position at the moment the sink
// takes a batch. It runs on the flushing goroutine with the sink's buffer
// lock held, so it must not call back into the sink.
func (f *follower) BeginFlush() {
	for _, t := range f.snapshotTailers() {
		t.markInflight()
	}
}

// EndFlush turns a delivery outcome into checkpoints.
//
// A committed batch advances each tailer to the offset BeginFlush
// snapshotted, which is the only offset the store is known to hold. A
// dropped batch does the opposite: it freezes those tailers, because the
// records are gone and a resume offset that moved past them would mean
// they are never re-read, not even after a restart. Which tailer fed the
// lost batch is not knowable here, so every live one is frozen; the cost
// is replaying some already-stored records on the next start, which
// content-addressed row keys collapse back to one row.
func (f *follower) EndFlush(_ int, dropped bool) {
	tailers := f.snapshotTailers()
	if dropped {
		for _, t := range tailers {
			if t.markHoled() {
				f.ing.cfg.Log.Printf("ERROR a batch was lost, so the checkpoint for %s is frozen at offset %d; restart to re-read from there",
					t.path, t.ackedOffset())
			}
		}
		return
	}
	now := time.Now().Unix()
	for _, t := range tailers {
		cp, ok := t.commitInflight(now)
		if !ok {
			continue
		}
		if err := f.cps.Save(&cp); err != nil {
			f.ing.cfg.Log.Printf("ERROR saving checkpoint for %s: %v", t.path, err)
		}
	}
}

func (t *tailer) ackedOffset() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.acked
}

func (f *follower) snapshotTailers() []*tailer {
	f.mu.Lock()
	defer f.mu.Unlock()
	tailers := make([]*tailer, 0, len(f.tailers))
	for _, t := range f.tailers {
		if t != nil {
			tailers = append(tailers, t)
		}
	}
	return tailers
}

func (f *follower) flushIdle(ctx context.Context) {
	tailers := f.snapshotTailers()
	now := time.Now()
	for _, t := range tailers {
		results := t.ex.FlushIdle(now)
		if len(results) == 0 {
			continue
		}
		t.flushSeq++
		pos := flushPos(t.flushSeq)
		for n, r := range results {
			_ = f.ing.cfg.Sink.Add(ctx, r, t.labels, keyHint(t.stream, pos, n))
		}
	}
}

func (f *follower) closeAll(ctx context.Context) {
	for _, t := range f.snapshotTailers() {
		f.drainExtractor(ctx, t)
		if t.file != nil {
			_ = t.file.Close()
			t.file = nil
		}
	}
	// No commit here on purpose: the caller flushes next, and the
	// observer commits exactly what that flush delivers.
}

// fingerprintBytes caps the width of the rotation fingerprint. Beyond this
// the hash costs more than it proves.
//
// The width matters. The original fingerprint was 64 bits over a fixed
// 256-byte window, which is far too narrow to identify a file: rotated
// logs routinely share their first 256 bytes -- a fixed startup banner, a
// constant-width header, a templated first line -- and a fingerprint match
// is what makes a restarting follower resume at the stored offset. Two
// different files agreeing on that window made it seek into the middle of
// the new one and silently skip everything before that point.
const fingerprintBytes = 4096

// fingerprintPrefix versions the fingerprint format. A checkpoint written
// by an older build carries a bare hex string over the legacy window;
// recognising it avoids re-reading every followed file on upgrade.
const fingerprintPrefix = "v2:"

// legacyFingerprintBytes is the original window, kept only to validate a
// checkpoint written before fingerprintPrefix existed.
const legacyFingerprintBytes = 256

// fingerprintWidth is how much of a file to fingerprint given how much of
// it has already been consumed.
//
// The window covers bytes that were *already read*, not a fixed prefix.
// That is what makes it both stable and meaningful: appending cannot
// change bytes behind the read offset, so the hash does not move on a
// growing file, while a truncate, a copytruncate or a rewrite in place
// changes it immediately -- at any file size. A fixed prefix could only
// ever be compared once the file was longer than the window, which left
// every short file with no rotation detection but a size comparison, and a
// copytruncate that restores the size defeats that.
func fingerprintWidth(consumed int64) int {
	switch {
	case consumed <= 0:
		return 0
	case consumed > fingerprintBytes:
		return fingerprintBytes
	default:
		return int(consumed)
	}
}

// fingerprintAt hashes a file's first width bytes, reporting how many it
// could actually read. A short read means the file no longer contains the
// bytes we already consumed from it.
func fingerprintAt(f *os.File, width int) (string, int) {
	if width <= 0 {
		return "", 0
	}
	head := make([]byte, width)
	n, _ := f.ReadAt(head, 0)
	return fingerprintOf(head[:n]), n
}

// fingerprintOf hashes a file's first bytes. Inode numbers get reused and
// are invisible over a remote transport; content is neither.
func fingerprintOf(head []byte) string {
	sum := sha256.Sum256(head)
	return fingerprintPrefix + hex.EncodeToString(sum[:16])
}

// legacyFingerprintOf reproduces the pre-v2 hash so a checkpoint written
// by an older build is still recognised.
func legacyFingerprintOf(head []byte) string {
	sum := sha256.Sum256(head)
	return hex.EncodeToString(sum[:8])
}

// fingerprintMatches reports whether a stored checkpoint fingerprint still
// describes this file, accepting the pre-v2 form.
func fingerprintMatches(f *os.File, cp *Checkpoint) bool {
	if cp == nil || cp.Fingerprint == "" {
		return false
	}
	if strings.HasPrefix(cp.Fingerprint, fingerprintPrefix) {
		width := cp.FingerprintBytes
		if width <= 0 {
			return false
		}
		got, n := fingerprintAt(f, width)
		return n == width && got == cp.Fingerprint
	}
	head := make([]byte, legacyFingerprintBytes)
	n, _ := f.ReadAt(head, 0)
	if n < legacyFingerprintBytes {
		return false
	}
	return cp.Fingerprint == legacyFingerprintOf(head[:n])
}
