package ingest

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/model"
)

func numCPU() int { return runtime.GOMAXPROCS(0) }

// Snapshot is the pipeline's telemetry at one instant: a plain value with
// no lock, so it can be marshalled and passed around freely.
type Snapshot struct {
	FilesTotal      int       `json:"files_total"`
	FilesDone       int       `json:"files_done"`
	BytesRead       int64     `json:"bytes_read"`
	Records         int64     `json:"records"`
	Samples         int64     `json:"samples"`
	UnmatchedLines  int64     `json:"unmatched_lines"`
	TSParseErrors   int64     `json:"ts_parse_errors"`
	ExtractErrors   int64     `json:"extract_errors"`
	UnjoinedLines   int64     `json:"unjoined_lines"`
	BinarySkipped   int64     `json:"binary_skipped"`
	ArchivesSkipped []string  `json:"archives_skipped,omitempty"`
	OversizeRecords int64     `json:"oversize_records"`
	NoProfileFiles  []string  `json:"no_profile_files,omitempty"`
	FirstUnmatched  []string  `json:"first_unmatched,omitempty"`
	LagBytes        int64     `json:"lag_bytes"`
	UDPDropped      int64     `json:"udp_dropped"`
	Started         time.Time `json:"started"`
}

// Progress accumulates the counters behind a lock. It is published three
// ways: a JSON file, periodic printing, and samples in the ingest set, so
// ingest health can be plotted next to the data it produced.
type Progress struct {
	mu sync.Mutex
	c  Snapshot
}

func NewProgress() *Progress { return &Progress{c: Snapshot{Started: time.Now()}} }

func (p *Progress) SetFilesTotal(n int) { p.mu.Lock(); p.c.FilesTotal = n; p.mu.Unlock() }
func (p *Progress) FileDone()           { p.mu.Lock(); p.c.FilesDone++; p.mu.Unlock() }
func (p *Progress) AddBytes(n int64)    { p.mu.Lock(); p.c.BytesRead += n; p.c.Records++; p.mu.Unlock() }
func (p *Progress) Unmatched()          { p.mu.Lock(); p.c.UnmatchedLines++; p.mu.Unlock() }
func (p *Progress) TSError()            { p.mu.Lock(); p.c.TSParseErrors++; p.mu.Unlock() }
func (p *Progress) ExtractError()       { p.mu.Lock(); p.c.ExtractErrors++; p.mu.Unlock() }

// Unjoined counts a continuation line that matched a multiline
// continue_regex but no join rule, so it was absorbed into nothing. It
// used to be invisible on every counter.
func (p *Progress) Unjoined()   { p.mu.Lock(); p.c.UnjoinedLines++; p.mu.Unlock() }
func (p *Progress) SkipBinary() { p.mu.Lock(); p.c.BinarySkipped++; p.mu.Unlock() }

// SkipArchive records a multi-file container that was not unpacked, so an
// import that read nothing can say which inputs it declined and why.
func (p *Progress) SkipArchive(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.c.ArchivesSkipped) < 20 {
		p.c.ArchivesSkipped = append(p.c.ArchivesSkipped, path)
	}
}

// OversizeRecord counts a record longer than the configured cap, whose
// excess was discarded. Truncation that nothing counts is indistinguishable
// from data that was never there.
func (p *Progress) OversizeRecord() { p.mu.Lock(); p.c.OversizeRecords++; p.mu.Unlock() }
func (p *Progress) SetLag(n int64)  { p.mu.Lock(); p.c.LagBytes = n; p.mu.Unlock() }
func (p *Progress) UDPDrop()        { p.mu.Lock(); p.c.UDPDropped++; p.mu.Unlock() }

func (p *Progress) NoProfile(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.c.NoProfileFiles) < 20 {
		p.c.NoProfileFiles = append(p.c.NoProfileFiles, path)
	}
}

// MergeStream folds a finished stream's counters in, including the sample
// of unmatched lines, which is the single most useful spec-debugging
// output there is.
func (p *Progress) MergeStream(s *extract.Stats) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.c.Samples += s.Samples
	for _, l := range s.FirstUnmatched {
		if len(p.c.FirstUnmatched) >= 10 {
			break
		}
		p.c.FirstUnmatched = append(p.c.FirstUnmatched, l)
	}
}

func (p *Progress) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.c
	c.NoProfileFiles = append([]string(nil), p.c.NoProfileFiles...)
	c.ArchivesSkipped = append([]string(nil), p.c.ArchivesSkipped...)
	c.FirstUnmatched = append([]string(nil), p.c.FirstUnmatched...)
	return c
}

// WriteFile persists the progress document atomically.
func (p *Progress) WriteFile(path string) error {
	if path == "" {
		return nil
	}
	snap := p.Snapshot()
	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Report ships progress into the store as ordinary samples, so a dashboard
// can plot ingest health with the same query language as everything else.
func (p *Progress) Report(ctx context.Context, sink *Sink, client string, streamLabels map[string]string) error {
	snap := p.Snapshot()
	sinkStats := sink.Snapshot()
	labels := map[string]string{"client": client}
	for k, v := range streamLabels {
		labels[k] = v
	}
	return sink.AddSample(ctx, model.IngestSet, model.Sample{
		TSMs:   time.Now().UnixMilli(),
		Labels: labels,
		Fields: map[string]model.Value{
			"bytes_read":      model.Int(snap.BytesRead),
			"records":         model.Int(snap.Records),
			"samples":         model.Int(snap.Samples),
			"unmatched_lines": model.Int(snap.UnmatchedLines),
			"unjoined_lines":  model.Int(snap.UnjoinedLines),
			"ts_parse_errors": model.Int(snap.TSParseErrors),
			"batches_sent":    model.Int(sinkStats.Sent),
			"batches_dropped": model.Int(sinkStats.Dropped),
			"rejected":        model.Int(sinkStats.Rejected),
			"lag_bytes":       model.Int(snap.LagBytes),
			"udp_dropped":     model.Int(snap.UDPDropped),
			"files_total":     model.Int(int64(snap.FilesTotal)),
			"files_done":      model.Int(int64(snap.FilesDone)),
		},
	})
}
