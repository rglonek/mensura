package ingest

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/model"
)

// Config is the ingest process configuration shared by every input.
type Config struct {
	Spec *extract.Spec
	Sink *Sink
	// Labels are attached to every sample from every input: the operator's
	// declaration, which outranks anything discovered from content or path.
	Labels map[string]string
	// From and To slice a source by time during extraction.
	From, To time.Time
	// StateDir holds follow checkpoints.
	StateDir string
	// ClientName identifies this ingest in the store's logs and in the
	// ingest-progress set.
	ClientName string
	Log        Logger
	// ReadBufferBytes sizes the line scanner.
	ReadBufferBytes int
	// MaxConcurrentFiles bounds parse parallelism.
	MaxConcurrentFiles int
	// Progress reports pipeline health.
	Progress *Progress
}

// Ingest runs inputs against a spec and a sink.
type Ingest struct {
	cfg Config
}

func New(cfg Config) (*Ingest, error) {
	if cfg.Spec == nil {
		return nil, fmt.Errorf("ingest: a spec is required")
	}
	if cfg.Sink == nil {
		return nil, fmt.Errorf("ingest: a sink is required")
	}
	if cfg.ReadBufferBytes <= 0 {
		cfg.ReadBufferBytes = 1 << 20
	}
	if cfg.MaxConcurrentFiles <= 0 {
		cfg.MaxConcurrentFiles = clamp(numCPU(), 4, 16)
	}
	if cfg.Progress == nil {
		cfg.Progress = NewProgress()
	}
	return &Ingest{cfg: cfg}, nil
}

func (i *Ingest) Progress() *Progress { return i.cfg.Progress }

// Batch imports a set of sources once and returns when they are all
// processed. Sources may be files, directories, globs or archives.
func (i *Ingest) Batch(ctx context.Context, sources []string) error {
	files, err := i.resolve(sources)
	if err != nil {
		return err
	}
	i.cfg.Progress.SetFilesTotal(len(files))
	sem := make(chan struct{}, i.cfg.MaxConcurrentFiles)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for _, f := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := i.processFile(ctx, path); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				i.cfg.Log.Printf("ERROR %s: %v", path, err)
			}
			i.cfg.Progress.FileDone()
		}(f)
	}
	wg.Wait()
	if err := i.cfg.Sink.Flush(ctx); err != nil {
		return err
	}
	return firstErr
}

// resolve expands sources into a list of readable files, unpacking
// archives into a working directory as it goes.
func (i *Ingest) resolve(sources []string) ([]string, error) {
	var out []string
	for _, src := range sources {
		matches, err := filepath.Glob(src)
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			if _, err := os.Stat(src); err != nil {
				return nil, fmt.Errorf("ingest: source %s: %w", src, err)
			}
			matches = []string{src}
		}
		for _, m := range matches {
			info, err := os.Stat(m)
			if err != nil {
				return nil, err
			}
			if info.IsDir() {
				err = filepath.WalkDir(m, func(p string, d os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if !d.IsDir() {
						out = append(out, p)
					}
					return nil
				})
				if err != nil {
					return nil, err
				}
				continue
			}
			out = append(out, m)
		}
	}
	return out, nil
}

// openRecords opens a file, transparently decompressing it, and returns a
// reader over its records.
func openRecords(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.HasSuffix(path, ".gz"), strings.HasSuffix(path, ".tgz"):
		zr, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		return readCloser{zr, func() error { zr.Close(); return f.Close() }}, nil
	case strings.HasSuffix(path, ".bz2"):
		return readCloser{bzip2.NewReader(f), f.Close}, nil
	}
	return f, nil
}

type readCloser struct {
	io.Reader
	closeFn func() error
}

func (r readCloser) Close() error { return r.closeFn() }

// processFile parses one file start to finish.
func (i *Ingest) processFile(ctx context.Context, path string) error {
	head, mtime, err := peek(path, 64<<10)
	if err != nil {
		return err
	}
	if isBinary(head) {
		i.cfg.Progress.SkipBinary()
		return nil
	}
	profile := i.cfg.Spec.SelectProfile(path, head, i.cfg.Labels, "")
	if profile == nil {
		// Silently dropping unmatched files is the failure mode that
		// wastes an afternoon, so it is counted and logged.
		i.cfg.Progress.NoProfile(path)
		i.cfg.Log.Printf("WARNING no profile matched %s; skipping", path)
		return nil
	}
	labels := i.streamLabels(path, head)
	stream, err := i.cfg.Spec.NewStream(profile, extract.StreamOptions{RefTime: mtime, From: i.cfg.From, To: i.cfg.To})
	if err != nil {
		return err
	}
	i.cfg.Sink.DeclareFields(profile)

	rc, err := openRecords(path)
	if err != nil {
		return err
	}
	defer rc.Close()

	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 0, 64<<10), i.cfg.ReadBufferBytes)
	for sc.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := sc.Text()
		i.cfg.Progress.AddBytes(int64(len(line) + 1))
		results, err := stream.Process(line)
		i.recordOutcome(err)
		for _, r := range results {
			if err := i.cfg.Sink.Add(ctx, r, labels); err != nil {
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	for _, r := range stream.Flush() {
		if err := i.cfg.Sink.Add(ctx, r, labels); err != nil {
			return err
		}
	}
	i.cfg.Progress.MergeStream(&stream.Stats)
	return nil
}

func (i *Ingest) recordOutcome(err error) {
	switch err {
	case nil:
	case extract.ErrNoMatch:
		i.cfg.Progress.Unmatched()
	case extract.ErrNoTimestamp:
		i.cfg.Progress.TSError()
	default:
		i.cfg.Progress.ExtractError()
	}
}

// streamLabels resolves identity: explicit declarations first, then
// content and path discovery, then the fallbacks. A file name is the least
// trustworthy source, so it never overrides the others.
func (i *Ingest) streamLabels(path string, head []byte) map[string]string {
	labels := map[string]string{}
	for k, v := range i.cfg.Spec.DiscoverIdentity(path, head) {
		labels[k] = v
	}
	for k, v := range i.cfg.Labels {
		labels[k] = v
	}
	if _, ok := labels["host"]; !ok {
		if h, err := os.Hostname(); err == nil {
			labels["host"] = h
		}
	}
	if _, ok := labels["source"]; !ok {
		labels["source"] = filepath.Base(path)
	}
	for k, v := range labels {
		if model.ValidateLabelKey(k) != nil || model.ValidateLabelValue(v) != nil {
			delete(labels, k)
		}
	}
	return labels
}

func peek(path string, n int) ([]byte, time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") || strings.HasSuffix(path, ".tgz") {
		zr, err := gzip.NewReader(f)
		if err != nil {
			return nil, info.ModTime(), nil
		}
		defer zr.Close()
		r = zr
	}
	buf := make([]byte, n)
	read, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, info.ModTime(), err
	}
	return buf[:read], info.ModTime(), nil
}

// isBinary sniffs content rather than trusting an extension: a NUL byte in
// the first block is the practical test.
func isBinary(head []byte) bool {
	for _, b := range head {
		if b == 0 {
			return true
		}
	}
	return false
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
