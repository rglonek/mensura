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
	// RotatedGlobs are extra patterns to sweep for content that rotated
	// away before it could be read.
	RotatedGlobs []string
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
	f := &follower{ing: i, opts: opts, cps: cps, tailers: map[string]*tailer{}}
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

type follower struct {
	ing  *Ingest
	opts FollowOptions
	cps  *CheckpointStore

	mu      sync.Mutex
	tailers map[string]*tailer
}

// tailer follows one file.
type tailer struct {
	path        string
	stream      string
	file        *os.File
	info        os.FileInfo
	fingerprint string
	offset      int64
	pending     int64
	acked       int64
	partial     []byte
	labels      map[string]string
	ex          *extract.Stream
	cp          *Checkpoint
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
	for p := range paths {
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
			return err
		}
		if err := f.read(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (f *follower) ensure(path string) (*tailer, error) {
	f.mu.Lock()
	t, ok := f.tailers[path]
	f.mu.Unlock()
	if ok {
		return t, nil
	}
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := fh.Stat()
	if err != nil {
		_ = fh.Close()
		return nil, err
	}
	head := make([]byte, 256)
	n, _ := fh.ReadAt(head, 0)
	fingerprint := fingerprintOf(head[:n])

	stream := StreamID(path)
	cp, had := f.cps.Load(stream)
	if !had {
		cp = &Checkpoint{Stream: stream, Path: path}
	}
	var start int64
	switch {
	case had && cp.Fingerprint == fingerprint && f.opts.StartAt != "beginning":
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
		f.ing.cfg.Log.Printf("WARNING no profile matched %s; not following it", path)
		f.mu.Lock()
		f.tailers[path] = nil
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
		path: path, stream: stream, file: fh, info: info, fingerprint: fingerprint,
		offset: start, acked: start, pending: start,
		labels: f.ing.streamLabels(path, full[:hn]), ex: ex, cp: cp,
	}
	cp.Fingerprint = fingerprint
	f.mu.Lock()
	f.tailers[path] = t
	f.mu.Unlock()
	return t, nil
}

// read consumes whatever new bytes exist, holding back a trailing partial
// line until its newline arrives.
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
			t.offset += int64(len(chunk))
			if chunk[len(chunk)-1] != '\n' {
				// Partial line: hold it back and rewind the offset so the
				// next pass reads it whole.
				t.partial = append(t.partial, chunk...)
				t.offset -= int64(len(chunk))
				return nil
			}
			line := chunk[:len(chunk)-1]
			if len(t.partial) > 0 {
				line = append(t.partial, line...)
				t.partial = nil
			}
			f.ing.cfg.Progress.AddBytes(int64(len(line) + 1))
			results, perr := t.ex.Process(string(line))
			f.ing.recordOutcome(perr)
			for _, res := range results {
				if err := f.ing.cfg.Sink.Add(ctx, res, t.labels); err != nil {
					return err
				}
			}
			t.pending = t.offset
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if info, serr := t.file.Stat(); serr == nil {
					f.ing.cfg.Progress.SetLag(info.Size() - t.offset)
				}
				return nil
			}
			return err
		}
	}
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
	head := make([]byte, 256)
	hn, _ := t.file.ReadAt(head, 0)
	current := fingerprintOf(head[:hn])
	if openInfo.Size() < t.offset || current != t.fingerprint {
		// A truncate, a copytruncate, or a rewrite in place: the bytes we
		// were reading are gone. Content is what decides, because a
		// truncate followed by fresh appends restores the size.
		f.ing.cfg.Log.Printf("INFO %s was truncated or rewritten; re-reading from the start", t.path)
		t.offset, t.pending, t.acked = 0, 0, 0
		t.partial = nil
		t.fingerprint = current
		t.cp.Fingerprint = current
		return nil
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
	t.cp.AckedOffset = 0
	t.cp.Offset = 0
	_ = f.cps.Save(t.cp)
}

// commit advances every tailer's acknowledged offset. It runs after a
// successful flush, which is the only event that proves the store has the
// bytes.
func (f *follower) commit() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range f.tailers {
		if t == nil {
			continue
		}
		t.acked = t.pending
		t.cp.Offset = t.offset
		t.cp.AckedOffset = t.acked
		t.cp.UpdatedUnix = time.Now().Unix()
		if err := f.cps.Save(t.cp); err != nil {
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

// fingerprintOf hashes a file's first bytes. Inode numbers get reused and
// are invisible over a remote transport; content is neither.
func fingerprintOf(head []byte) string {
	sum := sha256.Sum256(head)
	return hex.EncodeToString(sum[:8])
}
