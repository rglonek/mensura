// Package ingest acquires bytes, extracts samples and delivers them to a
// store over the write API. It never opens the database: one write path
// means one set of semantics for ordering, idempotency, backpressure and
// auth. See docs/design/02-ingest.md.
package ingest

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// SinkConfig tunes batching and delivery.
type SinkConfig struct {
	BatchSize  int
	BatchBytes int
	FlushEvery time.Duration
	// MaxFatalDrops bounds how many unretryable batches may be dropped
	// before the process gives up, so a supervisor notices a spec that no
	// longer matches its store.
	MaxFatalDrops int
}

func DefaultSinkConfig() SinkConfig {
	return SinkConfig{BatchSize: 1024, BatchBytes: 4 << 20, FlushEvery: 50 * time.Millisecond, MaxFatalDrops: 100}
}

// Sink batches samples per set and ships them to the store.
type Sink struct {
	client *wire.Client
	cfg    SinkConfig
	log    Logger

	mu      sync.Mutex
	buffers map[string][]model.Sample
	pending int

	metaMu   sync.Mutex
	metaSent map[string]struct{}
	metaQ    []wire.FieldMeta

	stopCh chan struct{}
	wg     sync.WaitGroup

	Stats SinkStats

	// onFlush is called after a successful write with the number of
	// samples committed; the follow driver uses it to advance checkpoints
	// only for bytes the store has actually accepted.
	onFlush func(int)
}

// SinkStats counts delivery outcomes.
type SinkStats struct {
	mu        sync.Mutex
	Sent      int64
	Accepted  int64
	Rejected  int64
	Retried   int64
	Dropped   int64
	FatalDrop int64
}

func (s *SinkStats) snapshot() SinkStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SinkStats{Sent: s.Sent, Accepted: s.Accepted, Rejected: s.Rejected, Retried: s.Retried, Dropped: s.Dropped, FatalDrop: s.FatalDrop}
}

// Logger is the small logging surface ingest needs.
type Logger interface {
	Printf(format string, args ...any)
}

func NewSink(client *wire.Client, cfg SinkConfig, log Logger) *Sink {
	if cfg.BatchSize <= 0 {
		cfg = DefaultSinkConfig()
	}
	s := &Sink{
		client: client, cfg: cfg, log: log,
		buffers:  map[string][]model.Sample{},
		metaSent: map[string]struct{}{},
		stopCh:   make(chan struct{}),
	}
	s.wg.Add(1)
	go s.flushLoop()
	return s
}

// OnFlush registers a callback invoked after each successful write.
func (s *Sink) OnFlush(fn func(int)) { s.onFlush = fn }

// Add queues one extracted result. It blocks when the batch is full and
// the store is not keeping up: back-pressure has to reach the reader, or
// the loss just moves somewhere less visible.
func (s *Sink) Add(ctx context.Context, r extract.Result, streamLabels map[string]string) error {
	labels := make(map[string]string, len(streamLabels)+len(r.Labels))
	for k, v := range streamLabels {
		labels[k] = v
	}
	for k, v := range r.Labels {
		labels[k] = v
	}
	sample := model.Sample{TSMs: r.TSMs, Labels: labels, Fields: r.Fields}

	s.mu.Lock()
	s.buffers[r.Set] = append(s.buffers[r.Set], sample)
	s.pending++
	full := s.pending >= s.cfg.BatchSize
	s.mu.Unlock()
	if full {
		return s.Flush(ctx)
	}
	return nil
}

// AddSample queues an already-formed sample, which is what the receive
// path and the progress reporter use.
func (s *Sink) AddSample(ctx context.Context, set string, sample model.Sample) error {
	s.mu.Lock()
	s.buffers[set] = append(s.buffers[set], sample)
	s.pending++
	full := s.pending >= s.cfg.BatchSize
	s.mu.Unlock()
	if full {
		return s.Flush(ctx)
	}
	return nil
}

// DeclareFields queues the field metadata a profile promises, per
// destination set. It is sent with the next write and then only again when
// it changes, because metadata is per field, not per sample.
func (s *Sink) DeclareFields(p *extract.Profile) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	for _, d := range p.Declarations() {
		for name, fs := range d.Fields {
			key := d.Set + "\x00" + name
			if _, done := s.metaSent[key]; done {
				continue
			}
			s.metaSent[key] = struct{}{}
			m := wire.FieldMeta{
				Set: d.Set, Field: name, Kind: model.Kind(fs.Kind),
				Unit: fs.Unit, UnitHint: fs.UnitHint, Description: fs.Description,
				MaxIntervalS: int(fs.MaxIntervalMs() / 1000),
			}
			if fs.Limits != nil {
				m.LimitMin, m.LimitMax = fs.Limits.Min, fs.Limits.Max
			}
			s.metaQ = append(s.metaQ, m)
		}
		for _, bs := range d.BucketSets {
			edges := bs.Edges2()
			for i, b := range bs.Buckets {
				key := d.Set + "\x00" + b
				if _, done := s.metaSent[key]; done {
					continue
				}
				s.metaSent[key] = struct{}{}
				s.metaQ = append(s.metaQ, wire.FieldMeta{
					Set: d.Set, Field: b, Kind: model.KindGauge, Unit: bs.EdgeUnit,
					BucketSet: bs.Name, BucketIndex: i, BucketEdge: edges[i],
				})
			}
		}
	}
}

// Flush ships everything buffered.
func (s *Sink) Flush(ctx context.Context) error {
	s.mu.Lock()
	if s.pending == 0 {
		s.mu.Unlock()
		return nil
	}
	batches := make([]model.Batch, 0, len(s.buffers))
	count := s.pending
	for set, samples := range s.buffers {
		if len(samples) == 0 {
			continue
		}
		batches = append(batches, model.Batch{Set: set, Samples: samples})
		delete(s.buffers, set)
	}
	s.pending = 0
	s.mu.Unlock()

	s.metaMu.Lock()
	meta := s.metaQ
	s.metaQ = nil
	s.metaMu.Unlock()

	req := &wire.WriteRequest{FieldMeta: meta, Batches: batches}
	resp, err := s.client.Write(ctx, req)
	if err != nil {
		var fatal *wire.ErrFatal
		if errors.As(err, &fatal) {
			// Retrying a malformed batch forever is how a pipeline stalls
			// silently, so it is dropped, counted and logged with enough
			// detail to fix the spec.
			s.Stats.mu.Lock()
			s.Stats.Dropped += int64(count)
			s.Stats.FatalDrop++
			drops := s.Stats.FatalDrop
			s.Stats.mu.Unlock()
			s.log.Printf("ERROR store rejected %d samples: %v (first set %q)", count, fatal, firstSet(batches))
			if s.cfg.MaxFatalDrops > 0 && drops >= int64(s.cfg.MaxFatalDrops) {
				return err
			}
			return nil
		}
		return err
	}
	s.Stats.mu.Lock()
	s.Stats.Sent++
	s.Stats.Accepted += int64(resp.Accepted)
	s.Stats.Rejected += int64(len(resp.Rejected))
	s.Stats.mu.Unlock()
	if len(resp.Rejected) > 0 {
		s.log.Printf("WARNING store rejected %d sample(s): %s", len(resp.Rejected), resp.Rejected[0].Reason)
	}
	if s.onFlush != nil {
		s.onFlush(resp.Accepted)
	}
	return nil
}

func firstSet(batches []model.Batch) string {
	if len(batches) == 0 {
		return ""
	}
	return batches[0].Set
}

func (s *Sink) flushLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.FlushEvery)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			if err := s.Flush(context.Background()); err != nil {
				s.log.Printf("ERROR flushing to store: %v", err)
			}
		}
	}
}

// Close flushes and stops the background flusher.
func (s *Sink) Close(ctx context.Context) error {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	s.wg.Wait()
	return s.Flush(ctx)
}

// Snapshot returns delivery counters for the progress report.
func (s *Sink) Snapshot() SinkStats { return s.Stats.snapshot() }
