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
	i.cfg.Sink.OnFlush(func(int) { f.commit() })

	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()
	idle := time.NewTicker(opts.IdleFlush)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
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
	info        os.FileInfo
	fingerprint string
	// fingerprintFull records whether fingerprint covers a full window;
	// a short-file fingerprint is provisional and may not be compared.
	fingerprintFull bool
	offset          int64
	labels          map[string]string
	ex              *extract.Stream
	cp              *Checkpoint

	mu      sync.Mutex
	pending int64
	acked   int64
}

func (t *tailer) setPending(n int64) {
	t.mu.Lock()
	t.pending = n
	t.mu.Unlock()
}

// commitPending promotes pending to acked and returns the checkpoint to
// write. The record is built and copied under the lock: two flushes can
// land at once, and mutating the shared Checkpoint while another goroutine
// marshals it is a race.
func (t *tailer) commitPending(now int64) Checkpoint {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.acked = t.pending
	t.cp.Offset = t.acked
	t.cp.AckedOffset = t.acked
	t.cp.UpdatedUnix = now
	return *t.cp
}

// setFingerprint records a new content fingerprint on the checkpoint.
func (t *tailer) setFingerprint(fp string, full bool) {
	t.mu.Lock()
	t.fingerprint, t.fingerprintFull = fp, full
	t.cp.Fingerprint = fp
	t.mu.Unlock()
}

func (t *tailer) reset(to int64) {
	t.mu.Lock()
	t.pending, t.acked = to, to
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
	fingerprint, fpFull := fingerprintFile(fh)

	stream := StreamID(path)
	cp, had := f.cps.Load(stream)
	if !had {
		cp = &Checkpoint{Stream: stream, Path: path}
	}
	var start int64
	switch {
	case had && fpFull && cp.Fingerprint == fingerprint && f.opts.StartAt != "beginning":
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
		path: path, stream: stream, file: fh, info: info,
		fingerprint: fingerprint, fingerprintFull: fpFull,
		labels: f.ing.streamLabels(path, full[:hn]), ex: ex, cp: cp,
	}
	t.reset(start)
	cp.Fingerprint = fingerprint
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
	r := bufio.NewReaderSize(t.file, 64<<10)
	for {
		chunk, err := r.ReadBytes('\n')
		if len(chunk) > 0 {
			if chunk[len(chunk)-1] != '\n' {
				// Incomplete: leave the offset where it was and stop.
				return f.atEOF(t)
			}
			t.offset += int64(len(chunk))
			line := chunk[:len(chunk)-1]
			f.ing.cfg.Progress.AddBytes(int64(len(chunk)))
			results, perr := t.ex.Process(string(line))
			f.ing.recordOutcome(perr)
			for _, res := range results {
				if err := f.ing.cfg.Sink.Add(ctx, res, t.labels); err != nil {
					return err
				}
			}
			// pending is only advanced once every sample from these bytes
			// has been handed to the sink, so a commit can never
			// acknowledge a byte whose samples are still unqueued.
			t.setPending(t.offset)
		}
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
	current, full := fingerprintFile(t.file)
	// A fingerprint over fewer than fingerprintBytes bytes changes on
	// every append, so below that threshold only a shrinking file counts
	// as a rewrite. Treating a growing young file as "rewritten" would
	// reset the offset and re-read it on every poll.
	rewritten := openInfo.Size() < t.offset
	if !rewritten && full && t.fingerprintFull && current != t.fingerprint {
		rewritten = true
	}
	if rewritten {
		// A truncate, a copytruncate, or a rewrite in place: the bytes we
		// were reading are gone. Content is what decides, because a
		// truncate followed by fresh appends restores the size.
		f.ing.cfg.Log.Printf("INFO %s was truncated or rewritten; re-reading from the start", t.path)
		t.reset(0)
		t.setFingerprint(current, full)
		return nil
	}
	// Adopt the fingerprint once the file is finally long enough to have a
	// stable one, so a later rewrite is still detected.
	if full && !t.fingerprintFull {
		t.setFingerprint(current, full)
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

func (f *follower) retire(ctx context.Context, t *tailer) {
	for _, r := range t.ex.Flush() {
		_ = f.ing.cfg.Sink.Add(ctx, r, t.labels)
	}
	_ = t.file.Close()
	f.mu.Lock()
	delete(f.tailers, t.path)
	f.mu.Unlock()
	// The replacement file starts from its beginning.
	t.reset(0)
	cp := t.commitPending(time.Now().Unix())
	if err := f.cps.Save(&cp); err != nil {
		f.ing.cfg.Log.Printf("ERROR saving checkpoint for %s: %v", t.path, err)
	}
}

// commit advances every tailer's acknowledged offset. It runs after a
// successful flush, which is the only event that proves the store has the
// bytes.
//
// It runs on the sink's flush goroutine, so it must not read a tailer's
// live read offset; only the guarded pending/acked pair is safe here.
func (f *follower) commit() {
	f.mu.Lock()
	tailers := make([]*tailer, 0, len(f.tailers))
	for _, t := range f.tailers {
		if t != nil {
			tailers = append(tailers, t)
		}
	}
	f.mu.Unlock()
	now := time.Now().Unix()
	for _, t := range tailers {
		cp := t.commitPending(now)
		if err := f.cps.Save(&cp); err != nil {
			f.ing.cfg.Log.Printf("ERROR saving checkpoint for %s: %v", t.path, err)
		}
	}
}

func (f *follower) flushIdle(ctx context.Context) {
	f.mu.Lock()
	tailers := make([]*tailer, 0, len(f.tailers))
	for _, t := range f.tailers {
		if t != nil {
			tailers = append(tailers, t)
		}
	}
	f.mu.Unlock()
	now := time.Now()
	for _, t := range tailers {
		for _, r := range t.ex.FlushIdle(now) {
			_ = f.ing.cfg.Sink.Add(ctx, r, t.labels)
		}
	}
}

func (f *follower) closeAll(ctx context.Context) {
	f.mu.Lock()
	tailers := make([]*tailer, 0, len(f.tailers))
	for _, t := range f.tailers {
		if t != nil {
			tailers = append(tailers, t)
		}
	}
	f.mu.Unlock()
	for _, t := range tailers {
		for _, r := range t.ex.Flush() {
			_ = f.ing.cfg.Sink.Add(ctx, r, t.labels)
		}
		_ = t.file.Close()
	}
	f.commit()
}

// fingerprintBytes is the width of the rotation fingerprint window. It is
// fixed rather than "however much the file has": a hash over a growing
// prefix changes on every append, which reads as a rewrite.
const fingerprintBytes = 256

// fingerprintFile hashes a file's first fingerprintBytes bytes and reports
// whether the file was long enough to fill the window. A fingerprint taken
// over a short file is not yet stable and must not be compared.
func fingerprintFile(f *os.File) (string, bool) {
	head := make([]byte, fingerprintBytes)
	n, _ := f.ReadAt(head, 0)
	return fingerprintOf(head[:n]), n >= fingerprintBytes
}

// fingerprintOf hashes a file's first bytes. Inode numbers get reused and
// are invisible over a remote transport; content is neither.
func fingerprintOf(head []byte) string {
	sum := sha256.Sum256(head)
	return hex.EncodeToString(sum[:8])
}
