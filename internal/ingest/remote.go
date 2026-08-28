package ingest

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
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
	// StrictHostKey keeps host-key verification on, which is the default.
	StrictHostKey bool
	Paths         []string
	// ProbeInterval is how often a stalled stream is re-checked, since
	// remote rotation cannot be observed directly.
	ProbeInterval time.Duration
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
	cps, err := NewCheckpointStore(i.cfg.StateDir)
	if err != nil {
		return err
	}
	errCh := make(chan error, len(opts.Paths))
	for _, path := range opts.Paths {
		go func(p string) { errCh <- i.followRemotePath(ctx, opts, cps, p) }(path)
	}
	for range opts.Paths {
		if err := <-errCh; err != nil && ctx.Err() == nil {
			return err
		}
	}
	return nil
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

	for ctx.Err() == nil {
		ex, err := i.cfg.Spec.NewStream(profile, extract.StreamOptions{
			RefTime: time.Now(), From: i.cfg.From, To: i.cfg.To,
		})
		if err != nil {
			return err
		}
		n, err := i.runRemoteTail(ctx, opts, path, cp, ex, labels)
		cp.AckedOffset += n
		cp.Offset = cp.AckedOffset
		cp.UpdatedUnix = time.Now().Unix()
		_ = cps.Save(cp)
		for _, r := range ex.Flush() {
			_ = i.cfg.Sink.Add(ctx, r, labels)
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

// runRemoteTail streams one connection's worth of bytes and returns how
// many it consumed, so the checkpoint can resume exactly there.
func (i *Ingest) runRemoteTail(ctx context.Context, opts RemoteOptions, path string, cp *Checkpoint, ex *extract.Stream, labels map[string]string) (int64, error) {
	args := []string{"-o", "BatchMode=yes"}
	if opts.StrictHostKey {
		args = append(args, "-o", "StrictHostKeyChecking=yes")
	}
	if opts.Port > 0 {
		args = append(args, "-p", strconv.Itoa(opts.Port))
	}
	if opts.CredentialPath != "" {
		args = append(args, "-i", opts.CredentialPath)
	}
	dest := opts.Host
	if opts.User != "" {
		dest = opts.User + "@" + opts.Host
	}
	// tail -F follows the name across a rotation where the remote tail
	// supports it; the byte offset is what makes a reconnect lossless.
	remote := fmt.Sprintf("tail -c +%d -F %s 2>/dev/null || tail -c +%d -f %s",
		cp.AckedOffset+1, shellQuote(path), cp.AckedOffset+1, shellQuote(path))
	args = append(args, dest, remote)

	cmd := exec.CommandContext(ctx, opts.SSHBinary, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	var consumed int64
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		consumed += int64(len(line) + 1)
		i.cfg.Progress.AddBytes(int64(len(line) + 1))
		results, perr := ex.Process(line)
		i.recordOutcome(perr)
		for _, r := range results {
			if err := i.cfg.Sink.Add(ctx, r, labels); err != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				return consumed, err
			}
		}
	}
	scanErr := sc.Err()
	waitErr := cmd.Wait()
	if scanErr != nil {
		return consumed, scanErr
	}
	return consumed, waitErr
}

// shellQuote wraps a remote path so a space or a glob character cannot be
// reinterpreted by the remote shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
