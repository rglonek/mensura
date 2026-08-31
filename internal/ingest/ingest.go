package ingest

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"errors"
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
	if cfg.ClientName == "" {
		if h, err := os.Hostname(); err == nil {
			cfg.ClientName = h
		} else {
			cfg.ClientName = "mensura-ingest"
		}
	}
	// The spec's `sets:` declarations travel with the first write, the
	// same way field metadata does.
	cfg.Sink.DeclareSets(cfg.Spec)
	return &Ingest{cfg: cfg}, nil
}

func (i *Ingest) Progress() *Progress { return i.cfg.Progress }

// ClientName is the name this ingest reports to the store, resolved to the
// hostname when the operator did not choose one.
func (i *Ingest) ClientName() string { return i.cfg.ClientName }

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

	cancelled := false
	for _, f := range files {
		select {
		case <-ctx.Done():
			// Stop starting work, but still wait below: returning here
			// would leave running goroutines writing into a sink the
			// caller is about to close.
			cancelled = true
		case sem <- struct{}{}:
		}
		if cancelled {
			break
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
	if cancelled {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if err := i.cfg.Sink.Flush(ctx); err != nil {
		return err
	}
	return firstErr
}

// resolve expands sources into a list of readable files: globs,
// directories walked recursively, and plain paths.
//
// It does not unpack archives. It never did; the comment that said it did
// was describing 02-ingest.md section 4.1 rather than this code.
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

// errArchive names a container format that holds several files. Reading
// one as a record stream is not a degraded result, it is a wrong one.
var errArchive = errors.New("ingest: archive members are not unpacked")

// isArchive reports whether a path names a multi-file container.
//
// A .tgz is a tar inside a gzip, not a compressed log. Decompressing it
// and handing the result to the line scanner fed tar headers -- file
// names, modes, padding -- to the extractor as if they were log records,
// which produces samples rather than an error and quietly poisons the
// match rate. Nested archive walking is listed as unimplemented in
// docs/design/12-implementation.md, so the honest behaviour is to say so.
func isArchive(path string) bool {
	lower := strings.ToLower(path)
	for _, suffix := range []string{".tar", ".tgz", ".tar.gz", ".tar.bz2", ".tar.zst", ".tar.xz", ".zip"} {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	return false
}

// openRecords opens a file, transparently decompressing single-file
// gzip and bzip2 streams, and returns a reader over its records.
func openRecords(path string) (io.ReadCloser, error) {
	if isArchive(path) {
		return nil, fmt.Errorf("%w: unpack %s and point --source at its contents", errArchive, path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		zr, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		return readCloser{zr, func() error { zr.Close(); return f.Close() }}, nil
	}
	if strings.HasSuffix(strings.ToLower(path), ".bz2") {
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
	// Checked before sniffing. A tar stream is compressed binary, so the
	// NUL test below would classify it as binary and skip it with no
	// explanation -- which is safe but leaves the operator staring at an
	// import that read nothing and said nothing about why.
	if isArchive(path) {
		i.cfg.Progress.SkipArchive(path)
		i.cfg.Log.Printf("WARNING %s is an archive; members are not unpacked. Extract it and point --source at its contents", path)
		return nil
	}
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

	// bufio.Scanner would fail the whole file with ErrTooLong on one
	// over-long line and abandon everything after it. A record longer than
	// the cap is a spec or source problem worth reporting, but it is not a
	// reason to stop importing the rest of the file.
	br := bufio.NewReaderSize(rc, 64<<10)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		rec, rerr := readRecord(br, i.cfg.ReadBufferBytes)
		if len(rec.Line) > 0 || rec.Terminated {
			if rec.Oversize {
				i.cfg.Progress.OversizeRecord()
			}
			i.cfg.Progress.AddBytes(int64(rec.Consumed))
			results, err := stream.Process(string(rec.Line))
			i.recordOutcome(err)
			for _, r := range results {
				if err := i.cfg.Sink.Add(ctx, r, labels); err != nil {
					return err
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return rerr
		}
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
	// Sniffing must see decompressed bytes: compressed data is full of
	// NULs, so a .bz2 whose head was read raw would always be classified
	// as binary and skipped, making the bzip2 reader below unreachable.
	var r io.Reader = f
	switch {
	case strings.HasSuffix(strings.ToLower(path), ".gz"):
		zr, err := gzip.NewReader(f)
		if err != nil {
			return nil, info.ModTime(), nil
		}
		defer zr.Close()
		r = zr
	case strings.HasSuffix(strings.ToLower(path), ".bz2"):
		r = bzip2.NewReader(f)
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
