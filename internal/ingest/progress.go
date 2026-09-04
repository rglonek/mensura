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

// AddRecord counts one framed record and the bytes it occupied. The two
// move together on every acquisition path, and the name says so: it was
// called AddBytes while also being the only thing that incremented the
// record counter, so a reader of the call sites could not tell that
// "records" was being counted at all.
func (p *Progress) AddRecord(n int64) {
	p.mu.Lock()
	p.c.BytesRead += n
	p.c.Records++
	p.mu.Unlock()
}

func (p *Progress) Unmatched()    { p.mu.Lock(); p.c.UnmatchedLines++; p.mu.Unlock() }
func (p *Progress) TSError()      { p.mu.Lock(); p.c.TSParseErrors++; p.mu.Unlock() }
func (p *Progress) ExtractError() { p.mu.Lock(); p.c.ExtractErrors++; p.mu.Unlock() }

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

// AddSamples counts extraction results handed to the sink.
//
// Every acquisition path calls it. The counter used to be fed only by
// MergeStream, and MergeStream is called from one place -- the batch
// importer -- so follow, SSH follow and receive reported "0 samples"
// forever: on the console, in the progress document, and in the samples
// field the _mensura_ingest set publishes for dashboards to plot.
func (p *Progress) AddSamples(n int64) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	p.c.Samples += n
	p.mu.Unlock()
}

// MergeStream folds a finished stream's sample of unmatched lines in,
// which is the single most useful spec-debugging output there is.
//
// The stream's own sample count is deliberately not merged: AddSamples
// counts results as they are handed to the sink, on every path, and adding
// both would double every batch-imported sample.
func (p *Progress) MergeStream(s *extract.Stats) {
	p.mu.Lock()
	defer p.mu.Unlock()
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
	// Synced before the rename, and the directory after it, the way
	// CheckpointStore.Save is: this file is what an operator reads after
	// the crash it is meant to describe, and one that survived only in
	// the page cache describes nothing.
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if d, derr := os.Open(filepath.Dir(path)); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
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
