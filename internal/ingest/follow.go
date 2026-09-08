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
		opts.IdleFlush = defaultIdleFlush
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

// defaultIdleFlush bounds how long a partial multiline record or a
// half-filled aggregation window waits on any followed stream. It is
// shared by the local and the remote follower so one --idle-flush means
// one thing.
const defaultIdleFlush = 30 * time.Second

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
	//
	// It is cleared by the first flush that succeeds afterwards, which
	// sets rewind so the poll goroutine re-reads from the frozen offset.
	// The freeze itself is the right policy; leaving a process restart as
	// its only exit was not, because a few seconds of store unavailability
	// then stopped checkpointing every followed file until someone noticed.
	holed bool
	// rewind asks the poll goroutine to seek back to acked and re-read.
	// It is set on the flush goroutine and consumed on the poll one,
	// which is the goroutine that owns offset and file.
	rewind bool
	// lag is how many bytes this file held past the read head the last
	// time the sweep reached its end. It lives here rather than being
	// published directly so the progress document can report the backlog
	// across every followed file rather than the last one visited.
	lag int64
}

func (t *tailer) setLag(n int64) {
	t.mu.Lock()
	t.lag = n
	t.mu.Unlock()
}

func (t *tailer) lagBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lag
}

// setPending publishes how far the reader has handed bytes to the sink.
//
// It does nothing while a rewind is owed. The read loop only tests
// rewindOwed at the top of an iteration, so a hole that cleared mid-record
// left it free to finish that record and publish a position past the
// frozen offset -- and a flush landing in that window snapshotted the
// advanced value and committed it, writing a checkpoint past the very
// bytes the rewind exists to re-read. Those bytes are about to be read
// again, so there is nothing here worth publishing.
func (t *tailer) setPending(n int64) {
	t.mu.Lock()
	if !t.rewind {
		t.pending = n
	}
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
//
// A tailer that owes a rewind snapshots nothing, for the reason setPending
// publishes nothing: clearHole has already pulled inflight back to acked,
// and leaving it there is what makes the commit below a no-op until the
// re-read has actually happened.
func (t *tailer) markInflight() {
	t.mu.Lock()
	if !t.rewind {
		t.inflight = t.pending
	}
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

// clearHole releases a frozen tailer once delivery is demonstrably working
// again, and reports the offset to re-read from.
//
// A successful flush is the signal, rather than a timer: it proves the
// store is accepting writes, and it cannot fire while the sink is still
// failing, so this can never spin. Nothing past the hole is acknowledged
// on the way out -- pending and inflight are pulled back to acked -- so
// the re-read starts exactly where the lost batch did. This is what a
// restart does, without the restart.
func (t *tailer) clearHole() (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.holed {
		return 0, false
	}
	t.holed = false
	t.pending, t.inflight = t.acked, t.acked
	t.rewind = true
	return t.acked, true
}

// rewindOwed reports whether a seek back to the acked offset is pending.
func (t *tailer) rewindOwed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rewind
}

// takeRewind reports whether the poll goroutine owes this tailer a seek
// back to the acked offset, and clears the request.
func (t *tailer) takeRewind() (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.rewind {
		return 0, false
	}
	t.rewind = false
	return t.acked, true
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
	// A rotation replaces the bytes the hole was in, so there is nothing
	// left to protect by staying frozen, and no rewind left to owe.
	t.holed, t.rewind = false, false
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
		f.applyRewind(t)
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
	f.retireUnmatched(ctx, paths)
	// After the retirement pass, so a file that has left the glob stops
	// contributing to the backlog rather than pinning it at whatever it
	// held when it went away.
	f.publishLag()
	return firstErr
}

// retireUnmatched drains and closes the tailers of paths the glob no
// longer returns.
//
// checkRotation has an unlink branch, but it is only reached for a path
// that is still in the sweep -- and a deleted file is not, because Glob
// lists a directory. So that branch only ever fired on the race between
// the glob and the stat, and an ordinary deletion left the tailer in the
// map forever: its descriptor stayed open, which keeps the unlinked
// inode's blocks allocated for the life of the process, and its extractor
// and buffers leaked with it. On a glob whose members come and go -- a
// file per day, a file per instance -- that is a slow march to a full
// disk and an exhausted descriptor table.
//
// The open handle is drained first, exactly as the unlink branch does: an
// unlinked file still holds whatever was written before it went away.
func (f *follower) retireUnmatched(ctx context.Context, matched map[string]struct{}) {
	f.mu.Lock()
	var stale []*tailer
	for path, t := range f.tailers {
		if _, ok := matched[path]; ok || t == nil {
			continue
		}
		stale = append(stale, t)
	}
	// The no-profile backoff is pruned with the tailers. Its entries are
	// only ever removed when the path is looked at again, and a path that
	// has left the glob never is, so on a pattern whose members come and
	// go -- a file per day, a file per instance -- the map grew by one
	// path string per file for the life of the process.
	for path := range f.noProfile {
		if _, ok := matched[path]; !ok {
			delete(f.noProfile, path)
		}
	}
	f.mu.Unlock()
	// Retired in a stable order so a sweep that retires several is
	// readable in the log.
	sort.Slice(stale, func(i, j int) bool { return stale[i].path < stale[j].path })
	for _, t := range stale {
		f.ing.cfg.Log.Printf("INFO %s no longer matches any followed pattern; draining and closing it", t.path)
		if err := f.read(ctx, t); err != nil {
			f.ing.cfg.Log.Printf("WARNING draining %s before retiring it: %v", t.path, err)
		}
		f.retireKeepingCheckpoint(ctx, t)
	}
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
	case f.opts.StartAt == "end" && !had:
		// "end" means "only what arrives from now on", and it is honoured
		// once: on the first sight of a stream, when there is no
		// checkpoint at all. It used to be honoured whenever the
		// fingerprint failed to match, which is exactly what a rotation
		// produces -- retire() rewinds the record to offset zero and
		// clears the fingerprint -- so every rename-and-create skipped
		// whatever the replacement already held between the rotation and
		// the next poll, silently. 02-ingest.md section 6.1 says a
		// rotation starts the new file at offset 0, and the remote
		// follower already gates the same flag on `!hadCheckpoint`.
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
	//
	// It is *cleared* when there is nothing consumed to cover, rather than
	// left as the checkpoint loaded it. A start at offset zero is what a
	// failed fingerprint comparison produces, so the hash still on the
	// record describes a file this tailer has just decided it is not
	// reading -- and every commit until the first widening persisted that
	// hash next to the new file's offsets. A checkpoint whose two halves
	// describe different files is the one shape the resume test cannot
	// read correctly.
	fp, width := "", fingerprintWidth(start)
	if width > 0 {
		if got, n := fingerprintAt(fh, width); n == width {
			fp = got
		} else {
			width = 0
		}
	}
	t.setFingerprint(fp, width)
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
		// A hole that cleared while this loop was running invalidates the
		// position it is reading at. Carrying on would advance pending
		// past the very bytes the rewind exists to re-read, and the next
		// commit would acknowledge them. The next poll re-seeks.
		if t.rewindOwed() {
			return nil
		}
		rec, err := readRecord(r, max)
		oversizeUnterminated := false
		if !rec.Terminated {
			if rec.Consumed <= max {
				// Incomplete: leave the offset where it was and stop.
				//
				// "Incomplete" is the shape of a partial line *and* the
				// shape of a read that failed, and the failure used to be
				// dropped here along with the record: an unreadable file
				// -- a disk fault, a revoked permission, a network mount
				// that went away -- was polled forever, five times a
				// second, reporting nothing to anyone. EOF is the one
				// that really does mean "nothing more yet".
				if err != nil && !errors.Is(err, io.EOF) {
					return err
				}
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
		if rec.Oversize || oversizeUnterminated {
			f.ing.cfg.Progress.OversizeRecord()
		}
		f.ing.cfg.Progress.AddRecord(int64(rec.Consumed))
		// The extractor is told where this record began, so a line that
		// only opens a multiline block or feeds an aggregation window can
		// hold the checkpoint back to its own offset.
		t.ex.Mark(recStart)
		results, perr := t.ex.Process(string(rec.Line))
		f.ing.recordOutcome(perr)
		f.ing.cfg.Progress.AddSamples(int64(len(results)))
		// Every sample of this record is queued before the failure is
		// reported, and the first failure is what the sweep hears about.
		//
		// Sink.Add buffers the sample and only then flushes, so its error
		// is the flush's verdict and never a refusal to take the sample:
		// returning on it abandoned the rest of the record while keeping
		// what came before, and left the offset before a record the
		// extractor had already consumed. The next poll then handed that
		// record to the extractor a *second* time -- an `increment`
		// window counted the line twice, a multiline join appended its
		// capture twice -- because extract.Stream is stateful and reading
		// a record into it is not idempotent. Finishing the record leaves
		// the extractor consistent with the offset, and the samples sit
		// in the sink's buffer, which is exactly where an undelivered
		// batch belongs: nothing is acknowledged, because acked only ever
		// moves on a committed flush.
		var addErr error
		for n, res := range results {
			if aerr := f.ing.cfg.Sink.Add(ctx, res, t.labels, keyHint(t.stream, offsetPos(recStart), n)); aerr != nil && addErr == nil {
				addErr = aerr
			}
		}
		t.offset = recStart + int64(rec.Consumed)
		// pending is only advanced once every sample from these bytes
		// has been handed to the sink, so a commit can never
		// acknowledge a byte whose samples are still unqueued.
		f.syncPending(t)
		if addErr != nil {
			return addErr
		}
		if oversizeUnterminated {
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return f.atEOF(t)
			}
			return err
		}
	}
}

// syncPending publishes the offset a commit may acknowledge: the read
// head, pulled back to the start of the oldest record the extractor is
// still holding.
//
// A record that produces no samples is not the same as a record that has
// been dealt with. One that merely opened a multiline block, or that was
// folded into an aggregation window which has not closed yet, exists only
// inside extract.Stream -- so acknowledging its bytes claims a durability
// the store has never been offered. It used to be acknowledged anyway,
// because the read loop advanced pending per record rather than per
// delivered sample: a crash inside the idle-flush window lost those
// records silently, and applyRewind's discard of the extractor threw away
// the part of an open window that came from bytes already acked.
func (f *follower) syncPending(t *tailer) {
	if at, held := t.ex.HeldFrom(); held && at < t.offset {
		t.setPending(at)
		return
	}
	t.setPending(t.offset)
}

// applyRewind seeks a thawed tailer back to its frozen offset.
//
// The extractor's buffered state -- an open multiline record, a
// half-filled aggregation window -- is discarded rather than flushed: it
// was built from the bytes that are about to be read again, so delivering
// it would emit each of those records twice. This is what a restart does
// with the same state.
//
// Discarding it is only lossless because syncPending holds the acked
// offset at or before the oldest record the extractor is holding, so the
// re-read starts early enough to rebuild the whole window. Advancing
// pending past a buffered record, which is what the read loop used to do,
// made this an unconditional loss of everything the window had already
// absorbed.
func (f *follower) applyRewind(t *tailer) {
	at, ok := t.takeRewind()
	if !ok {
		return
	}
	_ = t.ex.Flush()
	// reset, not a bare offset assignment: the read loop may have pushed
	// pending forward between the hole clearing and this seek, and a
	// pending that sits past the rewind point is exactly what would let a
	// later commit acknowledge the re-read bytes without reading them.
	t.reset(at)
}

// atEOF records how far this tailer is behind its file. The number is kept
// on the tailer rather than published straight to the progress document:
// SetLag overwrites, so with a glob matching several files the reported
// lag was whichever tailer happened to run last in the sweep, which on a
// busy directory is a number that describes nothing. poll publishes the
// sum once the sweep is done.
func (f *follower) atEOF(t *tailer) error {
	// The fingerprint is widened here, where the read head stops, and not
	// only at the top of the next poll.
	//
	// It used to be widened in checkRotation alone, which runs *before*
	// the read -- so between the moment a file's bytes were consumed and
	// the moment a hash covering them existed sat a whole poll interval,
	// and a fresh tailer spent that interval with no fingerprint at all
	// (fingerprintWidth(0) is 0). A rewrite in place landing in that
	// window was undetectable twice over: the size test does not fire
	// when the replacement is at least as long as the old read offset,
	// and there was no stored hash to compare against -- and then the
	// widening step ran on the *new* content and adopted it as the
	// fingerprint of bytes this tailer had never read. The head of the
	// replacement was skipped, permanently and silently. Covering the
	// bytes the moment they are consumed leaves no interval to lose them
	// in.
	f.widenFingerprint(t)
	if info, serr := t.file.Stat(); serr == nil {
		if lag := info.Size() - t.offset; lag > 0 {
			t.setLag(lag)
		} else {
			t.setLag(0)
		}
	}
	return nil
}

// widenFingerprint records a content hash covering everything this tailer
// has consumed, up to the cap. It never narrows one: the window only
// grows with the read offset, and a short read means the file no longer
// holds the bytes we already took from it, which is checkRotation's
// question rather than this function's.
func (f *follower) widenFingerprint(t *tailer) {
	if t.file == nil {
		return
	}
	width := fingerprintWidth(t.offset)
	if width <= t.fingerprintAt {
		return
	}
	if fp, n := fingerprintAt(t.file, width); n == width {
		t.setFingerprint(fp, width)
	}
}

// publishLag reports the whole backlog across every followed file.
func (f *follower) publishLag() {
	total := int64(0)
	for _, t := range f.snapshotTailers() {
		total += t.lagBytes()
	}
	f.ing.cfg.Progress.SetLag(total)
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
	// Only ever after the comparison above: widening first would hash
	// whatever the file holds now and call it what we read.
	f.widenFingerprint(t)
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
		// Still worth republishing: the flush emptied the extractor, so
		// the bytes it was holding the checkpoint back for are now the
		// store's problem rather than this process's memory.
		f.syncPending(t)
		return
	}
	t.flushSeq++
	pos := flushPos(t.flushSeq)
	f.ing.cfg.Progress.AddSamples(int64(len(results)))
	for n, r := range results {
		_ = f.ing.cfg.Sink.Add(ctx, r, t.labels, keyHint(t.stream, pos, n))
	}
	// Published only once every flushed sample is queued, so the offset
	// this releases is one the sink already holds.
	f.syncPending(t)
}

// retire closes a tailer whose path now holds -- or will hold -- a
// different file, and rewinds its checkpoint so the replacement is read
// from its beginning.
func (f *follower) retire(ctx context.Context, t *tailer) { f.closeTailer(ctx, t, true) }

// retireKeepingCheckpoint closes a tailer whose path is simply no longer
// in the followed set, leaving its resume record on disk untouched.
//
// The distinction matters because filepath.Glob reports an unreadable
// directory as "no matches" rather than as an error: an NFS blip, a
// permission change or a mount flap makes one sweep see nothing at all.
// Rewinding on that took every followed file back to offset zero, so the
// next sweep re-read every one of them in full -- a duplicate row for
// every record under `key: offset`, and a re-import of the whole backlog
// under content keying. A path that comes back is answered by ensure(),
// which compares the stored fingerprint against the file it finds and
// starts at zero when they disagree, so keeping the record is safe in
// both directions: the same file resumes, a different one does not.
func (f *follower) retireKeepingCheckpoint(ctx context.Context, t *tailer) {
	f.closeTailer(ctx, t, false)
}

func (f *follower) closeTailer(ctx context.Context, t *tailer, rewind bool) {
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
	if !rewind {
		return
	}
	// The replacement file starts from its beginning, and the record on
	// disk is rewound to say so now rather than on the next flush: a
	// crash in between would otherwise resume the *new* file at the old
	// one's offset.
	//
	// The fingerprint is cleared with the offsets. It described the file
	// that has just been rotated away, and leaving it behind left a
	// checkpoint whose content hash names bytes that are gone -- so
	// ensure() would compare the new file against the old one's hash,
	// which is a question with no useful answer either way.
	t.reset(0)
	t.setFingerprint("", 0)
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
				f.ing.cfg.Log.Printf("ERROR a batch was lost, so the checkpoint for %s is frozen at offset %d; it will be re-read from there once delivery recovers",
					t.path, t.ackedOffset())
			}
		}
		return
	}
	now := time.Now().Unix()
	for _, t := range tailers {
		// Delivery is working again, so a tailer frozen by an earlier
		// hole is released and asked to re-read from where it froze.
		if at, thawed := t.clearHole(); thawed {
			f.ing.cfg.Log.Printf("INFO delivery recovered; re-reading %s from offset %d to fill the hole left by the lost batch", t.path, at)
			continue
		}
		cp, ok := t.commitInflight(now)
		if !ok {
			continue
		}
		if err := f.cps.Save(&cp); err != nil {
			f.ing.cfg.Log.Printf("ERROR saving checkpoint for %s: %v", t.path, err)
		}
	}
}

// pendingOffsetForTest exposes the in-flight high-water mark, which is
// the value a commit is allowed to acknowledge.
func (t *tailer) pendingOffsetForTest() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pending
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
		f.ing.cfg.Progress.AddSamples(int64(len(results)))
		for n, r := range results {
			_ = f.ing.cfg.Sink.Add(ctx, r, t.labels, keyHint(t.stream, pos, n))
		}
		// The idle flush is what releases a checkpoint held back by an
		// open window on a quiet file. Without this the release waited
		// for the next record, which on a stream that has gone silent is
		// exactly the case the idle flush exists for.
		f.syncPending(t)
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
