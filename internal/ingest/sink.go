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
	setQ     []wire.SetMeta

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	Stats SinkStats

	// onFlush is called after a successful write with the number of
	// samples committed; the follow driver uses it to advance checkpoints
	// only for bytes the store has actually accepted. It is guarded
	// because it is registered after the flush loop is already running.
	flushMu sync.Mutex
	onFlush func(int)

	// sendMu serialises delivery so callbacks fire in commit order: two
	// concurrent flushes would otherwise let a later batch acknowledge
	// before an earlier one.
	sendMu sync.Mutex
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
	// Fill in only what the caller left unset; replacing the whole config
	// would silently discard every other field they did set.
	d := DefaultSinkConfig()
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = d.BatchSize
	}
	if cfg.BatchBytes <= 0 {
		cfg.BatchBytes = d.BatchBytes
	}
	if cfg.FlushEvery <= 0 {
		cfg.FlushEvery = d.FlushEvery
	}
	if cfg.MaxFatalDrops == 0 {
		cfg.MaxFatalDrops = d.MaxFatalDrops
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
func (s *Sink) OnFlush(fn func(int)) {
	s.flushMu.Lock()
	s.onFlush = fn
	s.flushMu.Unlock()
}

func (s *Sink) notifyFlush(n int) {
	s.flushMu.Lock()
	fn := s.onFlush
	s.flushMu.Unlock()
	if fn != nil {
		fn(n)
	}
}

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
			edges := bs.EdgeValues()
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

// DeclareSets queues what the spec says about whole destination sets:
// retention, shard width and key scheme. Like field metadata it travels
// with the next write and is not repeated.
func (s *Sink) DeclareSets(spec *extract.Spec) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	for name, opt := range spec.Sets {
		key := "\x00set\x00" + name
		if _, done := s.metaSent[key]; done {
			continue
		}
		s.metaSent[key] = struct{}{}
		s.setQ = append(s.setQ, wire.SetMeta{
			Set:         name,
			RetentionMs: opt.RetentionMs(),
			ShardMs:     opt.ShardMs(),
			KeyScheme:   model.KeyScheme(opt.Key),
		})
	}
}

// Flush ships everything buffered.
//
// Delivery is serialised: the buffer swap and the write happen under one
// lock, so two concurrent flushes cannot deliver out of order and let a
// later batch acknowledge bytes an earlier one has not written yet.
func (s *Sink) Flush(ctx context.Context) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	if s.pending == 0 {
		s.mu.Unlock()
		// Metadata still has to reach the store even when no samples are
		// waiting, or a spec's declarations would sit in the queue until
		// the first sample happens to arrive.
		return s.flushMetaOnly(ctx)
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
	meta, sets := s.metaQ, s.setQ
	s.metaQ, s.setQ = nil, nil
	s.metaMu.Unlock()

	req := &wire.WriteRequest{FieldMeta: meta, SetMeta: sets, Batches: batches}
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
			s.requeueMeta(meta, sets)
			return nil
		}
		// The batch was taken out of the buffer before the write, so a
		// retryable failure that ran out of retries loses it. Say so and
		// count it: an uncounted drop is indistinguishable from success.
		s.Stats.mu.Lock()
		s.Stats.Dropped += int64(count)
		s.Stats.mu.Unlock()
		s.log.Printf("ERROR gave up delivering %d samples after retries: %v (first set %q)", count, err, firstSet(batches))
		s.requeueMeta(meta, sets)
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
	s.notifyFlush(resp.Accepted)
	return nil
}

// requeueMeta puts undelivered field metadata back at the head of the
// queue. DeclareFields marks a field as sent when it is queued, so
// dropping the queue on a failed write would lose that metadata for the
// life of the process and leave the catalogue without kinds or units.
func (s *Sink) requeueMeta(meta []wire.FieldMeta, sets []wire.SetMeta) {
	if len(meta) == 0 && len(sets) == 0 {
		return
	}
	s.metaMu.Lock()
	s.metaQ = append(meta, s.metaQ...)
	s.setQ = append(sets, s.setQ...)
	s.metaMu.Unlock()
}

// flushMetaOnly delivers queued metadata with no samples attached.
func (s *Sink) flushMetaOnly(ctx context.Context) error {
	s.metaMu.Lock()
	meta, sets := s.metaQ, s.setQ
	s.metaQ, s.setQ = nil, nil
	s.metaMu.Unlock()
	if len(meta) == 0 && len(sets) == 0 {
		return nil
	}
	if _, err := s.client.Write(ctx, &wire.WriteRequest{FieldMeta: meta, SetMeta: sets}); err != nil {
		s.requeueMeta(meta, sets)
		var fatal *wire.ErrFatal
		if errors.As(err, &fatal) {
			s.log.Printf("ERROR store rejected spec metadata: %v", fatal)
			return nil
		}
		return err
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
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
	return s.Flush(ctx)
}

// Snapshot returns delivery counters for the progress report.
func (s *Sink) Snapshot() SinkStats { return s.Stats.snapshot() }
