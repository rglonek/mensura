package ingest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
)

// RemoteOptions configure an SSH-attached follow: nothing is installed on
// the target, at the cost of weaker rotation detection than a local follow.
// Where an agent can be installed, install the agent.
type RemoteOptions struct {
	Host string
	User string
	Port int
	// CredentialPath is passed to the ssh client as its identity file.
	CredentialPath string
	// InsecureHostKey turns off host-key verification. The zero value
	// keeps it on, so forgetting to set anything is the safe choice.
	InsecureHostKey bool
	Paths           []string
	// StartAt is "checkpoint" (default), "beginning" or "end", the same
	// three choices the local follower offers.
	StartAt string
	// ProbeInterval is how often the remote file's size is re-checked.
	// Rotation cannot be observed directly over a tail, so it is inferred
	// from the file becoming shorter than the bytes already read.
	ProbeInterval time.Duration
	// MaxRecordBytes bounds one record, so a newline-free remote file
	// cannot be read into memory in one piece.
	MaxRecordBytes int
	// ReconnectBackoff bounds the wait between reconnect attempts.
	ReconnectBackoff time.Duration
	// SSHBinary overrides the client binary, for tests.
	SSHBinary string
}

// FollowRemote tails remote files over SSH. Checkpoints are held locally
// and expressed as byte offsets, replayed with a skip on reconnect, so a
// dropped connection is lossless.
func (i *Ingest) FollowRemote(ctx context.Context, opts RemoteOptions) error {
	if opts.ProbeInterval <= 0 {
		opts.ProbeInterval = 15 * time.Second
	}
	if opts.ReconnectBackoff <= 0 {
		opts.ReconnectBackoff = 5 * time.Second
	}
	if opts.SSHBinary == "" {
		opts.SSHBinary = "ssh"
	}
	if opts.MaxRecordBytes <= 0 {
		opts.MaxRecordBytes = defaultMaxRecordBytes
	}
	cps, err := NewCheckpointStore(i.cfg.StateDir)
	if err != nil {
		return err
	}
	cps.Log = i.cfg.Log
	// Every path is waited for. Returning on the first error would leave
	// the other tails running against a sink the caller is about to close.
	tailCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, len(opts.Paths))
	for _, path := range opts.Paths {
		go func(p string) { errCh <- i.followRemotePath(tailCtx, opts, cps, p) }(path)
	}
	var first error
	for range opts.Paths {
		if err := <-errCh; err != nil && ctx.Err() == nil && first == nil {
			first = err
			cancel()
		}
	}
	return first
}

func (i *Ingest) followRemotePath(ctx context.Context, opts RemoteOptions, cps *CheckpointStore, path string) error {
	target := opts.Host + ":" + path
	stream := StreamID(target)
	cp, ok := cps.Load(stream)
	if !ok {
		cp = &Checkpoint{Stream: stream, Path: target}
	}
	labels := map[string]string{"host": opts.Host, "source": path}
	for k, v := range i.cfg.Labels {
		labels[k] = v
	}
	profile := i.cfg.Spec.SelectProfile(path, nil, labels, "ssh")
	if profile == nil {
		return fmt.Errorf("ingest: no profile matched remote path %s", path)
	}
	i.cfg.Sink.DeclareFields(profile)

	// The checkpoint advances only for bytes the store has accepted, the
	// same rule the local follower uses. runRemoteTail publishes the byte
	// offset each record ends at; the delivery observer promotes it.
	//
	// This registers one observer per followed path. It used to install a
	// single callback, which the sink stored in one slot -- so with more
	// than one --path, every goroutine but the last overwrote the
	// previous registration and those paths were never checkpointed at
	// all. They replayed from offset zero on every restart, for the life
	// of the deployment.
	// The checkpoint is shared between two goroutines: the tail loop
	// below, which rewinds it on a rotation, and the sink's flush
	// goroutine, which advances it through the observer. Guarding it and
	// handing Save a copy is what the local follower does for the same
	// reason -- CheckpointStore's own lock protects the file, not the
	// struct, so marshalling one goroutine's write while the other is
	// mid-update is a race that can persist a torn record.
	box := &checkpointBox{cp: cp}
	save := func(off int64) {
		snap := box.advance(off)
		if err := cps.Save(&snap); err != nil {
			i.cfg.Log.Printf("ERROR saving checkpoint for %s: %v", target, err)
		}
	}
	obs := &remoteObserver{
		progress: &remoteProgress{},
		save:     save,
		onHole: func(at int64) {
			i.cfg.Log.Printf("ERROR a batch was lost, so the checkpoint for %s is frozen at offset %d; restart to re-read from there", target, at)
		},
	}
	obs.progress.set(box.ackedOffset())
	progress := obs.progress
	i.cfg.Sink.Observe(obs)

	resetTo := func(off int64) {
		progress.set(off)
		save(off)
	}
	// StartAt used to be accepted on the command line and then ignored on
	// this path, so `--start-at end` against a remote host replayed the
	// whole history instead of starting at the tail.
	first := true
	flushSeq := 0
	for ctx.Err() == nil {
		// The size is the only rotation signal this path has. A file
		// shorter than what has already been read was truncated,
		// copytruncated or replaced, and resuming at the stored byte
		// offset would seek past the whole of the new one.
		if size, serr := i.remoteSize(ctx, opts, path); serr != nil {
			i.cfg.Log.Printf("WARNING cannot size %s: %v", target, serr)
		} else {
			switch {
			case first && opts.StartAt == "beginning":
				resetTo(0)
			case first && opts.StartAt == "end":
				resetTo(size)
			case size < progress.ackedOffset():
				i.cfg.Log.Printf("INFO %s was truncated or rotated; re-reading from the start", target)
				resetTo(0)
			}
		}
		first = false
		ex, err := i.cfg.Spec.NewStream(profile, extract.StreamOptions{
			RefTime: time.Now(), From: i.cfg.From, To: i.cfg.To,
		})
		if err != nil {
			return err
		}
		err = i.runRemoteTail(ctx, opts, path, target, ex, labels, progress)
		if flushed := ex.Flush(); len(flushed) > 0 {
			flushSeq++
			pos := flushPos(flushSeq)
			for n, r := range flushed {
				_ = i.cfg.Sink.Add(ctx, r, labels, keyHint(target, pos, n))
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			i.cfg.Log.Printf("WARNING remote tail %s: %v; reconnecting in %s", target, err, opts.ReconnectBackoff)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opts.ReconnectBackoff):
		}
	}
	return nil
}

// checkpointBox owns one remote path's Checkpoint. Every read hands back
// a copy, so the record being marshalled can never be the record being
// written.
type checkpointBox struct {
	mu sync.Mutex
	cp *Checkpoint
}

// advance moves the resume point and returns the record to persist.
func (b *checkpointBox) advance(off int64) Checkpoint {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cp.Offset, b.cp.AckedOffset = off, off
	b.cp.UpdatedUnix = time.Now().Unix()
	return *b.cp
}

func (b *checkpointBox) ackedOffset() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cp.AckedOffset
}

// remoteProgress carries a remote tail's byte position between the reader
// goroutine and the delivery observer. pending is how far the reader has
// handed bytes to the sink, inflight is how far it had got when the sink
// took the batch it is currently writing, and acked is how far the store
// has confirmed -- the only value a checkpoint may hold.
type remoteProgress struct {
	mu       sync.Mutex
	pending  int64
	inflight int64
	acked    int64
	// holed freezes the resume offset after a dropped batch. A checkpoint
	// is a single offset, so once records are missing from the middle of
	// the stream it can never move past them without losing them.
	holed bool
}

func (p *remoteProgress) set(n int64) {
	p.mu.Lock()
	p.pending, p.inflight, p.acked = n, n, n
	p.mu.Unlock()
}

func (p *remoteProgress) advance(n int64) {
	p.mu.Lock()
	p.pending = n
	p.mu.Unlock()
}

func (p *remoteProgress) markInflight() {
	p.mu.Lock()
	p.inflight = p.pending
	p.mu.Unlock()
}

func (p *remoteProgress) commitInflight() (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.holed || p.inflight <= p.acked {
		return p.acked, false
	}
	p.acked = p.inflight
	return p.acked, true
}

func (p *remoteProgress) markHoled() (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// A path with nothing in flight cannot have contributed to the lost
	// batch; freezing it too stopped checkpointing every followed path
	// for the life of the process because one of them produced one bad
	// record.
	if p.holed || p.inflight <= p.acked {
		return p.acked, false
	}
	p.holed = true
	return p.acked, true
}

func (p *remoteProgress) pendingOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending
}

func (p *remoteProgress) ackedOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acked
}

// remoteObserver turns one remote tail's delivery outcomes into
// checkpoints. One is registered per followed path.
type remoteObserver struct {
	progress *remoteProgress
	save     func(acked int64)
	onHole   func(at int64)
}

func (o *remoteObserver) BeginFlush() { o.progress.markInflight() }

func (o *remoteObserver) EndFlush(_ int, dropped bool) {
	if dropped {
		if at, first := o.progress.markHoled(); first && o.onHole != nil {
			o.onHole(at)
		}
		return
	}
	if acked, moved := o.progress.commitInflight(); moved {
		o.save(acked)
	}
}

// sshArgs builds the client arguments shared by every remote invocation.
func sshArgs(opts RemoteOptions) []string {
	args := []string{"-o", "BatchMode=yes"}
	if !opts.InsecureHostKey {
		args = append(args, "-o", "StrictHostKeyChecking=yes")
	}
	if opts.Port > 0 {
		args = append(args, "-p", strconv.Itoa(opts.Port))
	}
	if opts.CredentialPath != "" {
		args = append(args, "-i", opts.CredentialPath)
	}
	return args
}

func sshDest(opts RemoteOptions) string {
	if opts.User != "" {
		return opts.User + "@" + opts.Host
	}
	return opts.Host
}

// remoteSize reports the current length of a remote file.
//
// It is what stands in for the local follower's fingerprint: a tail cannot
// report that the file underneath it was replaced, and the byte offsets
// this path checkpoints are meaningless once it has been. A file shorter
// than what has already been read is the observable signature of a
// truncate, a copytruncate or a rotation, and `wc -c` needs no GNU
// coreutils on the far end.
func (i *Ingest) remoteSize(ctx context.Context, opts RemoteOptions, path string) (int64, error) {
	args := append(sshArgs(opts), sshDest(opts), "wc -c < "+shellQuote(path))
	cmd := exec.CommandContext(ctx, opts.SSHBinary, args...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return 0, fmt.Errorf("%w: %s", err, msg)
		}
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(out.String()), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unreadable size %q", strings.TrimSpace(out.String()))
	}
	return n, nil
}

// runRemoteTail streams one connection's worth of bytes, publishing the
// byte offset each complete record ends at so the flush callback can turn
// it into a checkpoint.
//
// Bytes are counted from the raw framing rather than from len(line)+1: a
// CRLF stream or a file whose last line has no newline would otherwise
// drift the offset permanently, and the drift compounds on every
// reconnect.
func (i *Ingest) runRemoteTail(ctx context.Context, opts RemoteOptions, path, target string, ex *extract.Stream, labels map[string]string, progress *remoteProgress) error {
	args := sshArgs(opts)
	dest := sshDest(opts)
	start := progress.ackedOffset()
	// One tail invocation, not a shell "|| fallback": a fallback that
	// fires after the first tail has already streamed bytes would replay
	// them from the original offset. Support for -F is remembered
	// per-target instead, so the retry starts from the same place.
	flag := "-F"
	if !i.remoteFollowsName(opts.Host, path) {
		flag = "-f"
	}
	remote := fmt.Sprintf("tail -c +%d %s %s", start+1, flag, shellQuote(path))
	args = append(args, dest, remote)

	cmd := exec.CommandContext(ctx, opts.SSHBinary, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	// A rotation cannot be seen from inside the stream: `tail -F` follows
	// the new file while this counter keeps climbing on the old one, so
	// the checkpoint ends up pointing far into a file that never held
	// those bytes, and the next reconnect skips its whole head. Watching
	// the size and dropping the connection makes the reconnect re-probe
	// and restart from the beginning.
	watchStop := make(chan struct{})
	var watchDone sync.WaitGroup
	watchDone.Add(1)
	go func() {
		defer watchDone.Done()
		t := time.NewTicker(opts.ProbeInterval)
		defer t.Stop()
		for {
			select {
			case <-watchStop:
				return
			case <-t.C:
				sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				size, serr := i.remoteSize(sctx, opts, path)
				cancel()
				if serr != nil || size >= progress.pendingOffset() {
					continue
				}
				i.cfg.Log.Printf("INFO %s is shorter than the bytes already read; reconnecting from the start", target)
				_ = cmd.Process.Kill()
				return
			}
		}
	}()
	defer func() { close(watchStop); watchDone.Wait() }()

	consumed := start
	r := bufio.NewReaderSize(stdout, 64<<10)
	var readErr error
	for {
		// readRecord rather than ReadBytes: an unbounded read pulls a
		// newline-free remote file into memory whole, which is the
		// failure the local acquisition paths were converted away from.
		rec, err := readRecord(r, opts.MaxRecordBytes)
		if rec.Terminated {
			recStart := consumed
			consumed += int64(rec.Consumed)
			if rec.Oversize {
				i.cfg.Progress.OversizeRecord()
			}
			i.cfg.Progress.AddBytes(int64(rec.Consumed))
			results, perr := ex.Process(string(rec.Line))
			i.recordOutcome(perr)
			for n, res := range results {
				if aerr := i.cfg.Sink.Add(ctx, res, labels, keyHint(target, offsetPos(recStart), n)); aerr != nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					return aerr
				}
			}
			// Only complete records advance the offset, so a connection
			// that dies mid-line resumes at the start of that line.
			progress.advance(consumed)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}
	waitErr := cmd.Wait()
	if readErr != nil {
		return readErr
	}
	if waitErr != nil {
		if flag == "-F" && consumed == start {
			// The remote tail produced nothing and failed; it may not
			// support -F. Remember that and use -f from now on.
			i.markRemoteNoFollowName(opts.Host, path)
		}
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return fmt.Errorf("%w: %s", waitErr, msg)
		}
	}
	return waitErr
}

// remoteFollowName tracks which targets accepted "tail -F".
var (
	remoteFollowNameMu sync.Mutex
	remoteNoFollowName = map[string]struct{}{}
)

func (i *Ingest) remoteFollowsName(host, path string) bool {
	remoteFollowNameMu.Lock()
	defer remoteFollowNameMu.Unlock()
	_, no := remoteNoFollowName[host+":"+path]
	return !no
}

func (i *Ingest) markRemoteNoFollowName(host, path string) {
	remoteFollowNameMu.Lock()
	remoteNoFollowName[host+":"+path] = struct{}{}
	remoteFollowNameMu.Unlock()
}

// shellQuote wraps a remote path so a space or a glob character cannot be
// reinterpreted by the remote shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
