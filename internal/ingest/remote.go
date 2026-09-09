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
	// ProbeInterval is how often the remote file is re-checked. Rotation
	// cannot be observed directly over a tail, so it is inferred from the
	// file becoming shorter than the bytes already read, or from its
	// inode changing under the same name.
	ProbeInterval time.Duration
	// MaxRecordBytes bounds one record, so a newline-free remote file
	// cannot be read into memory in one piece.
	MaxRecordBytes int
	// ReconnectBackoff bounds the wait between reconnect attempts.
	ReconnectBackoff time.Duration
	// IdleFlush bounds how long a partial multiline record or a
	// half-filled aggregation window may wait, exactly as it does on a
	// local follow. A remote tail has no end of file to flush at and a
	// connection can stay up for days, so without a clock of its own a
	// quiet stream held its last record -- and, because the checkpoint
	// is pulled back to the oldest byte the extractor is still holding,
	// its resume offset with it -- until the connection dropped.
	IdleFlush time.Duration
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
	if opts.IdleFlush <= 0 {
		opts.IdleFlush = defaultIdleFlush
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
	cp, hadCheckpoint := cps.Load(stream)
	if !hadCheckpoint {
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
		progress: newRemoteProgress(),
		save:     save,
		onHole: func(at int64) {
			i.cfg.Log.Printf("ERROR a batch was lost, so the checkpoint for %s is frozen at offset %d; it will be re-read from there once delivery recovers", target, at)
		},
		onThaw: func(at int64) {
			i.cfg.Log.Printf("INFO delivery recovered; reconnecting %s from offset %d to fill the hole left by the lost batch", target, at)
		},
	}
	obs.progress.set(box.ackedOffset())
	progress := obs.progress
	i.cfg.Sink.Observe(obs)

	resetTo := func(off int64) {
		progress.set(off)
		save(off)
	}
	saveIdentity := func(ident string) {
		snap := box.setIdentity(ident)
		if err := cps.Save(&snap); err != nil {
			i.cfg.Log.Printf("ERROR saving checkpoint for %s: %v", target, err)
		}
	}
	// startAtPending records that --start-at has not been honoured yet.
	// The flag used to be accepted on the command line and ignored on
	// this path entirely, so `--start-at end` against a remote host
	// replayed the whole history; then it was honoured only inside the
	// else arm of the size probe below, so a single transient SSH
	// failure on the first pass -- which this loop logs and carries on
	// from -- silently dropped it again, for the life of the process.
	// It is cleared where the flag is actually applied.
	startAtPending := opts.StartAt == "beginning" || (opts.StartAt == "end" && !hadCheckpoint)
	rs := &remoteStream{}
	for ctx.Err() == nil {
		// "beginning" needs no size, so it is honoured even when the
		// remote host cannot be reached to measure the file.
		if startAtPending && opts.StartAt == "beginning" {
			resetTo(0)
			startAtPending = false
		}
		// A file shorter than what has already been read was truncated
		// or copytruncated, and resuming at the stored byte offset would
		// seek past the whole of the new one. A file whose identity has
		// changed was renamed away and re-created, and its length says
		// nothing at all -- a replacement that has already grown past
		// the acknowledged offset looks perfectly healthy by size.
		size, ident, serr := i.remoteStat(ctx, opts, path)
		if serr != nil {
			// The probe is retried on the next pass rather than treated
			// as an answer; startAtPending stays set until it succeeds.
			i.cfg.Log.Printf("WARNING cannot stat %s: %v", target, serr)
		} else {
			known := box.identity()
			switch {
			case startAtPending:
				// Only when there is no checkpoint, which is what the
				// local follower does with the same flag. Honouring it
				// unconditionally meant a restart of a remote follow
				// skipped everything written while it was down, while
				// the identical local command resumed.
				resetTo(size)
				startAtPending = false
			case ident != "" && known != "" && ident != known:
				i.cfg.Log.Printf("INFO %s has been replaced by a new file; re-reading from the start", target)
				resetTo(0)
			case size < progress.ackedOffset():
				i.cfg.Log.Printf("INFO %s was truncated; re-reading from the start", target)
				resetTo(0)
			}
			// Recorded on the checkpoint, so a rotation that happens
			// while this process is down is seen on the next start
			// rather than resumed into at an offset that means nothing.
			if ident != "" && ident != known {
				saveIdentity(ident)
			}
		}
		ex, err := i.cfg.Spec.NewStream(profile, extract.StreamOptions{
			RefTime: time.Now(), From: i.cfg.From, To: i.cfg.To,
		})
		if err != nil {
			return err
		}
		_, err = i.runRemoteTail(ctx, opts, path, target, rs, ex, labels, progress, box.identity())
		if flushed, pos := rs.flushAll(); len(flushed) > 0 {
			i.cfg.Progress.AddSamples(int64(len(flushed)))
			for n, r := range flushed {
				_ = i.cfg.Sink.Add(ctx, r, labels, keyHint(target, pos, n))
			}
		}
		// The tail held the offset back to the oldest record its
		// extractor was buffering; that buffer has just been flushed into
		// the sink, so those bytes are now the store's to hold and the
		// checkpoint may cover them.
		//
		// It is published on every exit, not only the clean one, and it
		// is read off the stream rather than from the tail's own byte
		// count. The old shape did neither, and a dropped connection is
		// the ordinary exit here: the flush was delivered while the
		// resume offset stayed behind the window it came from, so the
		// reconnect re-read those records and the extractor emitted the
		// same window a second time -- collapsed by a content key,
		// landing as a duplicate row under `key: offset`, whose flush
		// hint is a per-connection sequence number rather than a byte
		// offset. The same happened on every clean shutdown. Reading it
		// off the stream is what keeps the delivery-failure path honest:
		// there the tail's counter has already moved past a record whose
		// samples were not all queued, while remoteStream.consumed --
		// which only advances once delivery has succeeded -- has not.
		progress.advance(rs.held())
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

// remoteStream is one remote path's extraction state.
//
// extract.Stream is single-threaded and the idle flusher runs on its own
// goroutine while the reader is blocked on a quiet tail, so both reach it
// through this lock. consumed travels with it because the idle flush is
// what releases a checkpoint the extractor was holding back, and the
// offset it releases is the reader's, not the flusher's.
type remoteStream struct {
	mu       sync.Mutex
	ex       *extract.Stream
	consumed int64
	// flushSeq numbers the flushes of buffered state over the whole life
	// of the path rather than of one connection: it is half of the key
	// hint, so restarting it per connection would hand two different
	// flushes the same one and collapse their rows under `key: offset`.
	flushSeq int
}

// open binds a fresh connection's extractor and its starting read
// position.
func (rs *remoteStream) open(ex *extract.Stream, at int64) {
	rs.mu.Lock()
	rs.ex, rs.consumed = ex, at
	rs.mu.Unlock()
}

// nextFlushPosLocked reserves the hint position for one flush of buffered
// state. It must be called with the lock held.
func (rs *remoteStream) nextFlushPosLocked() string {
	rs.flushSeq++
	return flushPos(rs.flushSeq)
}

// process runs one framed record through the extractor and hands what it
// produced to deliver. The whole record -- the mark, the extraction and
// the delivery -- happens under one lock, so an idle flush can never land
// in the middle of one; and the read position only moves once every
// sample from these bytes has been queued, which is what stops a
// checkpoint acknowledging a byte whose sample is still unsent.
//
// extractErr is why the record produced nothing, which is a counter
// rather than a failure. deliverErr is the sink refusing it, which is.
func (rs *remoteStream) process(recStart int64, line string, deliver func([]extract.Result) error, consumed int64) (extractErr, deliverErr error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.ex == nil {
		return nil, nil
	}
	rs.ex.Mark(recStart)
	results, perr := rs.ex.Process(line)
	if err := deliver(results); err != nil {
		return perr, err
	}
	rs.consumed = consumed
	return perr, nil
}

// held is the offset a checkpoint may cover: the read position, pulled
// back to the start of the oldest record the extractor is still holding.
func (rs *remoteStream) held() int64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.heldLocked()
}

func (rs *remoteStream) heldLocked() int64 {
	if rs.ex == nil {
		return rs.consumed
	}
	return heldOffset(rs.ex, rs.consumed)
}

// flushIdle emits whatever has been waiting longer than the profile's own
// idle timeout, hands it to deliver, and reports the offset that releases.
//
// The delivery happens under the lock, exactly as process's does, and for
// the same reason. Emptying the extractor raises the held offset to the
// read head; returning the results to be queued afterwards left a window
// in which the reader -- which runs on its own goroutine here, unlike the
// local follower's idle flush -- could process the next record and
// publish a position covering these bytes, and a flush landing in that
// window would acknowledge them while their samples were still on their
// way to the sink. Flushing and queueing as one step is what keeps the
// published offset behind everything the sink has been handed.
func (rs *remoteStream) flushIdle(now time.Time, deliver func(results []extract.Result, pos string)) int64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.ex == nil {
		return rs.consumed
	}
	if results := rs.ex.FlushIdle(now); len(results) > 0 {
		deliver(results, rs.nextFlushPosLocked())
	}
	return rs.heldLocked()
}

// flushAll drains the extractor at the end of a connection.
func (rs *remoteStream) flushAll() ([]extract.Result, string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.ex == nil {
		return nil, ""
	}
	results := rs.ex.Flush()
	if len(results) == 0 {
		return nil, ""
	}
	return results, rs.nextFlushPosLocked()
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

// identity is the remote file this checkpoint's offsets belong to. It is
// held in the same field a local follow keeps its content fingerprint in:
// both answer "are these offsets still about this file?".
func (b *checkpointBox) identity() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cp.Fingerprint
}

// setIdentity records the file the offsets now belong to and returns the
// record to persist.
func (b *checkpointBox) setIdentity(ident string) Checkpoint {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cp.Fingerprint = ident
	b.cp.UpdatedUnix = time.Now().Unix()
	return *b.cp
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
	//
	// It is cleared by the first flush that succeeds afterwards, which
	// signals rewind so the tail is reconnected from the frozen offset.
	// The freeze is the right policy; needing a process restart to leave
	// it was not.
	holed bool
	// rewindOwed says the tail is still streaming bytes from past the
	// frozen offset, so nothing it publishes may be acknowledged. The
	// local follower keeps the same flag on its tailer and for the same
	// reason: clearing the hole only asks the reader to restart, and the
	// reader is very likely mid-record when it is asked. Without this,
	// advance() published the read head it was about to abandon, a flush
	// landing in that window snapshotted and committed it, and the
	// checkpoint moved past the very records the freeze exists to
	// re-read -- which the reconnect then skipped, because it starts at
	// the acknowledged offset.
	rewindOwed bool
	// rewind is signalled when a hole clears. It is buffered and never
	// blocks, so the flush goroutine cannot be held up by a tail loop
	// that is between reconnects.
	rewind chan struct{}
}

func newRemoteProgress() *remoteProgress {
	return &remoteProgress{rewind: make(chan struct{}, 1)}
}

// clearHole releases a frozen path once delivery is working again and asks
// the tail loop to reconnect from the frozen offset. Nothing past the hole
// is acknowledged on the way out, so the reconnect re-reads exactly the
// bytes the lost batch carried.
func (p *remoteProgress) clearHole() (int64, bool) {
	p.mu.Lock()
	if !p.holed {
		p.mu.Unlock()
		return 0, false
	}
	p.holed = false
	p.pending, p.inflight = p.acked, p.acked
	p.rewindOwed = true
	at := p.acked
	ch := p.rewind
	p.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return at, true
}

func (p *remoteProgress) set(n int64) {
	p.mu.Lock()
	p.pending, p.inflight, p.acked = n, n, n
	// A rotation replaces the bytes the hole was in, so there is nothing
	// left to protect by staying frozen, and no rewind left to owe.
	p.holed, p.rewindOwed = false, false
	p.mu.Unlock()
}

// startFrom is the offset a fresh tail connection must begin at, and it
// discharges a pending rewind: from here on the reader really is reading
// from the acknowledged offset, so what it publishes is publishable
// again.
//
// The stale rewind signal is drained with it. It is buffered, so a hole
// that cleared while the tail loop was between reconnects left a token
// behind that the *next* connection's watcher read immediately and
// killed a tail that was already starting from the right place.
func (p *remoteProgress) startFrom() int64 {
	p.mu.Lock()
	if p.rewindOwed {
		p.rewindOwed = false
		p.pending, p.inflight = p.acked, p.acked
	}
	at := p.acked
	ch := p.rewind
	p.mu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		default:
		}
	}
	return at
}

// advance publishes how far the reader has handed bytes to the sink. It
// does nothing while a rewind is owed: those bytes are about to be read
// again, so there is nothing there worth publishing.
func (p *remoteProgress) advance(n int64) {
	p.mu.Lock()
	if !p.rewindOwed {
		p.pending = n
	}
	p.mu.Unlock()
}

// markInflight snapshots what the batch the sink has just taken is
// entitled to acknowledge. A tail that owes a rewind snapshots nothing,
// for the reason advance publishes nothing: clearHole has already pulled
// inflight back to acked, and leaving it there is what makes the commit
// a no-op until the re-read has actually happened.
func (p *remoteProgress) markInflight() {
	p.mu.Lock()
	if !p.rewindOwed {
		p.inflight = p.pending
	}
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

// rewindOwedForTest reports whether a rewind is still pending.
func (p *remoteProgress) rewindOwedForTest() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rewindOwed
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
	onThaw   func(at int64)
}

func (o *remoteObserver) BeginFlush() { o.progress.markInflight() }

func (o *remoteObserver) EndFlush(_ int, dropped bool) {
	if dropped {
		if at, first := o.progress.markHoled(); first && o.onHole != nil {
			o.onHole(at)
		}
		return
	}
	if at, thawed := o.progress.clearHole(); thawed {
		if o.onThaw != nil {
			o.onThaw(at)
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
	if opts.InsecureHostKey {
		// Turned off explicitly, not merely left unsaid. Omitting the
		// strict setting fell back to the client's own default, which
		// under the BatchMode=yes above still refuses an unknown host --
		// so the flag named "insecure host key" disabled nothing, and the
		// follow failed exactly where it was asked not to. The known-hosts
		// file goes with it, or a host whose key has changed is still
		// refused by a conflicting entry.
		args = append(args, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null")
	} else {
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

// remoteStat reports the current length of a remote file and an identity
// for the file itself.
//
// Together they stand in for the local follower's fingerprint: a tail
// cannot report that the file underneath it was replaced, and the byte
// offsets this path checkpoints are meaningless once it has been.
//
// A file shorter than what has already been read is the signature of a
// truncate or a copytruncate. It is not the signature of a rename-and-
// create rotation, which is the common one: `tail -F` follows the new
// file while the offsets keep climbing from the old, so by the time the
// next probe runs the replacement has often already grown past the
// acknowledged offset and the size test sees nothing wrong. The inode
// does: `ls -Li` is POSIX, needs no GNU coreutils on the far end, and
// dereferences a symlinked log path the way tail does. A host whose ls
// cannot answer leaves the identity empty, which is exactly the
// size-only behaviour this path had before.
func (i *Ingest) remoteStat(ctx context.Context, opts RemoteOptions, path string) (int64, string, error) {
	q := shellQuote(path)
	args := append(sshArgs(opts), sshDest(opts), "wc -c < "+q+"; ls -Li "+q+" 2>/dev/null || true")
	cmd := exec.CommandContext(ctx, opts.SSHBinary, args...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return 0, "", fmt.Errorf("%w: %s", err, msg)
		}
		return 0, "", err
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	n, err := strconv.ParseInt(strings.TrimSpace(lines[0]), 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("unreadable size %q", strings.TrimSpace(lines[0]))
	}
	ident := ""
	if len(lines) > 1 {
		if f := strings.Fields(lines[1]); len(f) > 0 {
			if _, perr := strconv.ParseInt(f[0], 10, 64); perr == nil {
				ident = "ino:" + f[0]
			}
		}
	}
	return n, ident, nil
}

// runRemoteTail streams one connection's worth of bytes, publishing the
// byte offset each complete record ends at so the flush callback can turn
// it into a checkpoint.
//
// Bytes are counted from the raw framing rather than from len(line)+1: a
// CRLF stream or a file whose last line has no newline would otherwise
// drift the offset permanently, and the drift compounds on every
// reconnect.
func (i *Ingest) runRemoteTail(ctx context.Context, opts RemoteOptions, path, target string, rs *remoteStream, ex *extract.Stream, labels map[string]string, progress *remoteProgress, ident string) (int64, error) {
	args := sshArgs(opts)
	dest := sshDest(opts)
	start := progress.startFrom()
	rs.open(ex, start)
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
		return start, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return start, err
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
		// A clock for the extractor, which a tail does not supply: a
		// connection can stay up for days, so a stream that goes quiet
		// would otherwise hold its last partial record and its
		// half-filled window -- and the checkpoint they pin -- until it
		// dropped. The cadence is the question, not the answer: the
		// profile's own idle_timeout decides what is actually due.
		idle := time.NewTicker(remoteIdleTick(opts.IdleFlush))
		defer idle.Stop()
		for {
			select {
			case <-watchStop:
				return
			case now := <-idle.C:
				i.remoteFlushIdle(ctx, rs, target, labels, progress, now)
			case <-progress.rewind:
				// A hole cleared: the tail is streaming bytes from past
				// the frozen offset, so it has to be restarted from it.
				_ = cmd.Process.Kill()
				return
			case <-t.C:
				sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				size, now, serr := i.remoteStat(sctx, opts, path)
				cancel()
				if serr != nil {
					continue
				}
				switch {
				case now != "" && ident != "" && now != ident:
					// tail -F is already following the replacement while
					// the offsets here keep climbing from the file it
					// left, so the acknowledged offset is drifting into
					// a file that never held those bytes. Only the size
					// used to be watched, which misses this entirely
					// once the new file is longer than the old offset.
					i.cfg.Log.Printf("INFO %s has been replaced by a new file; reconnecting from the start", target)
				case size < progress.pendingOffset():
					i.cfg.Log.Printf("INFO %s is shorter than the bytes already read; reconnecting from the start", target)
				default:
					continue
				}
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
		// A record longer than the cap with no terminator in it is
		// consumed rather than held back, exactly as the local follower
		// does: waiting for a newline that is not coming would re-read
		// the same bytes on every reconnect and never advance.
		oversizeUnterminated := !rec.Terminated && rec.Consumed > opts.MaxRecordBytes
		if rec.Terminated || oversizeUnterminated {
			recStart := consumed
			consumed += int64(rec.Consumed)
			if rec.Oversize || oversizeUnterminated {
				i.cfg.Progress.OversizeRecord()
			}
			i.cfg.Progress.AddRecord(int64(rec.Consumed))
			// Where this record began, so a line that only opens a
			// multiline block or feeds an aggregation window holds the
			// checkpoint back to its own offset rather than letting it
			// run past data that exists only in the extractor. The whole
			// record goes through the stream lock, so the idle flusher
			// cannot land between extraction and delivery.
			perr, aerr := rs.process(recStart, string(rec.Line), func(results []extract.Result) error {
				i.cfg.Progress.AddSamples(int64(len(results)))
				for n, res := range results {
					if err := i.cfg.Sink.Add(ctx, res, labels, keyHint(target, offsetPos(recStart), n)); err != nil {
						return err
					}
				}
				return nil
			}, consumed)
			i.recordOutcome(perr)
			if aerr != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				return consumed, aerr
			}
			// Only complete records advance the offset, so a connection
			// that dies mid-line resumes at the start of that line.
			progress.advance(rs.held())
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
		return consumed, readErr
	}
	if waitErr != nil {
		if flag == "-F" && consumed == start {
			// The remote tail produced nothing and failed; it may not
			// support -F. Remember that and use -f from now on.
			i.markRemoteNoFollowName(opts.Host, path)
		}
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return consumed, fmt.Errorf("%w: %s", waitErr, msg)
		}
	}
	return consumed, waitErr
}

// remoteIdleTick is how often the idle question is asked. It is bounded
// below so a very short --idle-flush cannot turn into a busy loop, and
// asking more than once per timeout is what makes a record wait at most
// its timeout rather than twice it.
func remoteIdleTick(idle time.Duration) time.Duration {
	tick := idle / 2
	if tick < time.Second {
		tick = time.Second
	}
	return tick
}

// remoteFlushIdle emits whatever the extractor has held past the
// profile's idle timeout and republishes the offset that releases.
func (i *Ingest) remoteFlushIdle(ctx context.Context, rs *remoteStream, target string, labels map[string]string, progress *remoteProgress, now time.Time) {
	at := rs.flushIdle(now, func(results []extract.Result, pos string) {
		i.cfg.Progress.AddSamples(int64(len(results)))
		for n, r := range results {
			_ = i.cfg.Sink.Add(ctx, r, labels, keyHint(target, pos, n))
		}
	})
	// Published even when the flush emitted nothing: the extractor may
	// have been holding the offset for a window that has since closed on
	// its own, and this is the only clock a quiet tail has.
	progress.advance(at)
}

// heldOffset pulls a read offset back to the start of the oldest record
// the extractor is still holding, so a checkpoint never claims bytes whose
// sample exists only inside extract.Stream.
func heldOffset(ex *extract.Stream, read int64) int64 {
	if at, held := ex.HeldFrom(); held && at < read {
		return at
	}
	return read
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
