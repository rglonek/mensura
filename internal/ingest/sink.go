// Package ingest acquires bytes, extracts samples and delivers them to a
// store over the write API. It never opens the database: one write path
// means one set of semantics for ordering, idempotency, backpressure and
// auth. See docs/design/02-ingest.md.
package ingest

import (
	"context"
	"errors"
	"fmt"
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
	// MaxBufferedSamples bounds what may be held while the store is
	// refusing writes. A store that sheds load asks the client to slow
	// down, so running out of retries is back-pressure, not a licence to
	// discard: the batch goes back in the buffer and delivery is held.
	// That trade only holds while there is somewhere to put it, and this
	// is where "somewhere" ends -- past it the oldest batch is dropped
	// and counted, because an unbounded buffer turns a store outage into
	// an out-of-memory kill that loses everything rather than the tail.
	MaxBufferedSamples int
}

func DefaultSinkConfig() SinkConfig {
	return SinkConfig{
		BatchSize: 1024, BatchBytes: 4 << 20, FlushEvery: 50 * time.Millisecond,
		MaxFatalDrops: 100, MaxBufferedSamples: 100_000,
	}
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

	// observers are told when a delivery begins and how it ended. They are
	// guarded because they are registered after the flush loop is already
	// running, and they are a slice rather than a single callback: every
	// followed path registers one, and a single slot silently kept only
	// the last registration, so every other path's checkpoint was never
	// written at all.
	flushMu   sync.Mutex
	observers []DeliveryObserver

	// sendMu serialises delivery so callbacks fire in commit order: two
	// concurrent flushes would otherwise let a later batch acknowledge
	// before an earlier one.
	sendMu sync.Mutex

	// holdMu guards holdUntil, the point in time before which delivery
	// is not worth attempting. It exists for the auth case: the batch is
	// requeued rather than dropped, so without a gate the very next Add
	// would find the buffer still full, flush again, and spin on a
	// credential that will not change for minutes.
	holdMu    sync.Mutex
	holdUntil time.Time
	// now is the clock, overridable in tests.
	now func() time.Time
}

// deliveryHold is how long the sink waits after a rejected credential
// before trying again. Long enough that a restart or a config reload can
// land, short enough that recovery is automatic.
const deliveryHold = 30 * time.Second

// retryHold is how long the sink waits after exhausting the client's own
// retries. The store has just spent several seconds telling this client to
// back off; flushing again immediately would spend the next batch's
// retries on the same overload. It is much shorter than deliveryHold
// because the condition it waits out -- a compaction stall, a rolling
// restart -- clears on its own.
const retryHold = 5 * time.Second

func (s *Sink) holding() bool {
	s.holdMu.Lock()
	defer s.holdMu.Unlock()
	return !s.holdUntil.IsZero() && s.now().Before(s.holdUntil)
}

func (s *Sink) holdDelivery(d time.Duration) {
	s.holdMu.Lock()
	s.holdUntil = s.now().Add(d)
	s.holdMu.Unlock()
}

// SinkStats counts delivery outcomes.
type SinkStats struct {
	mu       sync.Mutex
	Sent     int64
	Accepted int64
	Rejected int64
	// Retried counts batches put back in the buffer after the write
	// client ran out of its own retries, not the individual HTTP
	// attempts: one increment is one batch that was held rather than lost.
	Retried   int64
	Dropped   int64
	FatalDrop int64
	// Unencodable counts samples refused before they entered a buffer
	// because they could not be marshalled. They never reach the store,
	// so the store cannot name them; this counter is the only place they
	// are visible.
	Unencodable int64
}

func (s *SinkStats) snapshot() SinkStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SinkStats{
		Sent: s.Sent, Accepted: s.Accepted, Rejected: s.Rejected, Retried: s.Retried,
		Dropped: s.Dropped, FatalDrop: s.FatalDrop, Unencodable: s.Unencodable,
	}
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
	if cfg.MaxBufferedSamples <= 0 {
		cfg.MaxBufferedSamples = d.MaxBufferedSamples
	}
	s := &Sink{
		client: client, cfg: cfg, log: log,
		buffers:  map[string][]model.Sample{},
		metaSent: map[string]struct{}{},
		stopCh:   make(chan struct{}),
		now:      time.Now,
	}
	s.wg.Add(1)
	go s.flushLoop()
	return s
}

// DeliveryObserver is how a driver that tracks byte offsets learns which
// bytes a write actually committed.
//
// Two calls, not one, because a single "flush succeeded" callback cannot
// express either half of the problem it needs to solve:
//
//   - BeginFlush runs while the sink still holds the buffer it is about to
//     send. Everything handed to the sink before this point is in the
//     batch and nothing handed to it afterwards is, so this is the only
//     moment at which a driver can snapshot a correct high-water mark. A
//     driver that instead read its own live offset after the write
//     acknowledged bytes whose samples were still sitting in the next
//     buffer.
//   - EndFlush reports the outcome, including the case the sink gave up
//     on. A dropped batch is a hole, and a checkpoint that is a single
//     resume offset can never advance past a hole without losing it.
type DeliveryObserver interface {
	// BeginFlush is called with the sink's buffer lock held, immediately
	// after the batch is taken. It must not call back into the sink.
	BeginFlush()
	// EndFlush is called after the write, in commit order. dropped is
	// true when the batch was abandoned rather than committed.
	EndFlush(accepted int, dropped bool)
}

// Observe registers a delivery observer. Registrations accumulate.
func (s *Sink) Observe(o DeliveryObserver) {
	if o == nil {
		return
	}
	s.flushMu.Lock()
	s.observers = append(s.observers, o)
	s.flushMu.Unlock()
}

func (s *Sink) snapshotObservers() []DeliveryObserver {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	return append([]DeliveryObserver(nil), s.observers...)
}

func (s *Sink) beginFlush(obs []DeliveryObserver) {
	for _, o := range obs {
		o.BeginFlush()
	}
}

func (s *Sink) endFlush(obs []DeliveryObserver, accepted int, dropped bool) {
	for _, o := range obs {
		o.EndFlush(accepted, dropped)
	}
}

// Add queues one extracted result. It blocks when the batch is full and
// the store is not keeping up: back-pressure has to reach the reader, or
// the loss just moves somewhere less visible.
//
// keyHint identifies the occurrence: the stream plus the byte offset the
// record was read at. It is what a set keyed by `offset` hashes instead of
// the field values, so two records that share a millisecond and a label
// set stay two rows. Passing "" leaves such a set collapsing them into
// one, which is why every acquisition path supplies one.
func (s *Sink) Add(ctx context.Context, r extract.Result, streamLabels map[string]string, keyHint string) error {
	labels := make(map[string]string, len(streamLabels)+len(r.Labels))
	for k, v := range streamLabels {
		labels[k] = v
	}
	for k, v := range r.Labels {
		labels[k] = v
	}
	sample := model.Sample{TSMs: r.TSMs, Labels: labels, Fields: r.Fields, KeyHint: keyHint}
	if !s.encodable(r.Set, &sample) {
		return nil
	}

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

// encodable refuses a sample that would make the whole request
// unmarshallable, and reports whether it may be buffered.
//
// One sample can poison a batch: a field carrying NaN or an infinity --
// which strconv.ParseFloat produces from the literal text "NaN" or "Inf"
// in a log line -- makes encoding/json fail on the *request*, so the
// batch is undeliverable no matter how many times it is retried. A
// dropped batch is reported to the delivery observers as a hole, and a
// hole freezes every followed file's checkpoint until the process
// restarts, at which point the same line does it again. Screening the
// one sample keeps the other thousand moving.
func (s *Sink) encodable(set string, sample *model.Sample) bool {
	var bad error
	for k, v := range sample.Fields {
		if err := model.ValidateFieldValue(v); err != nil {
			bad = fmt.Errorf("field %q: %w", k, err)
			break
		}
	}
	if bad == nil {
		return true
	}
	s.Stats.mu.Lock()
	s.Stats.Unencodable++
	n := s.Stats.Unencodable
	s.Stats.mu.Unlock()
	// Logged on the powers of ten: a source emitting these emits many,
	// and a line per sample would bury everything else.
	if isLogMilestone(n) {
		s.log.Printf("WARNING dropped %d sample(s) that cannot be encoded; most recent: set %q, %v", n, set, bad)
	}
	return false
}

// isLogMilestone reports whether a running count is worth another log
// line: the first, then each power of ten.
func isLogMilestone(n int64) bool {
	for m := int64(1); m > 0 && m <= n; m *= 10 {
		if m == n {
			return true
		}
	}
	return false
}

// AddSample queues an already-formed sample, which is what the receive
// path and the progress reporter use.
func (s *Sink) AddSample(ctx context.Context, set string, sample model.Sample) error {
	if !s.encodable(set, &sample) {
		return nil
	}
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
				MaxIntervalMs: fs.MaxIntervalMs(),
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
	if s.holding() {
		// Delivery is known to be failing for a reason a retry cannot
		// fix. The buffer keeps what it has; nothing is dropped and no
		// observer is told anything, so no checkpoint moves either way.
		return nil
	}
	obs := s.snapshotObservers()
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
	// Under the buffer lock: the batch is now fixed, and any Add racing
	// this flush is either already in it or blocked until it is not.
	s.beginFlush(obs)
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
			// The samples are gone. Telling the observers so is what stops
			// a later successful flush from advancing a checkpoint over
			// the hole they left.
			s.endFlush(obs, 0, true)
			if s.cfg.MaxFatalDrops > 0 && drops >= int64(s.cfg.MaxFatalDrops) {
				return err
			}
			s.requeueMeta(meta, sets)
			return nil
		}
		// A cancelled context is not a delivery failure: the caller is
		// shutting down and will flush again on a live context, so the
		// batch goes back in the buffer instead of being counted as
		// lost. Dropping it here made an ordinary Ctrl-C lose whatever
		// was in flight, and -- once a dropped batch started freezing
		// checkpoints -- froze them on every clean shutdown too.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.requeueBatches(batches, count)
			s.requeueMeta(meta, sets)
			return err
		}
		// Nor is a rejected credential. The batch is well formed and the
		// store is reachable; an operator has to fix a token. Dropping
		// it would lose good data and, because a dropped batch reads as
		// a hole, freeze every checkpoint permanently -- so a mistyped
		// token cost far more than the outage that caused it.
		var authErr *wire.ErrAuth
		if errors.As(err, &authErr) {
			s.requeueBatches(batches, count)
			s.requeueMeta(meta, sets)
			s.holdDelivery(deliveryHold)
			s.log.Printf("ERROR %v; holding %d buffered sample(s) and retrying in %s", authErr, count, deliveryHold)
			return err
		}
		// Running out of retries is back-pressure, not a verdict on the
		// batch. A store that sheds load answers 503 with Retry-After --
		// its own documented signal to slow down -- and six retries is
		// only a few seconds of that, so discarding here threw good data
		// away during exactly the condition the shedding exists to
		// survive. The batch goes back in the buffer and delivery is
		// held, which is what the auth path already does; the readers
		// feel it through the buffer filling up.
		if s.bufferedRoom(count) {
			hold := s.retryHoldFor(err)
			s.requeueBatches(batches, count)
			s.requeueMeta(meta, sets)
			s.holdDelivery(hold)
			s.Stats.mu.Lock()
			s.Stats.Retried++
			s.Stats.mu.Unlock()
			s.log.Printf("WARNING %v; holding %d buffered sample(s) and retrying in %s", err, count, hold)
			return err
		}
		// The buffer is full: the store has been refusing writes for long
		// enough that holding more would cost the whole process. Only now
		// is anything dropped, and it is said out loud -- an uncounted
		// drop is indistinguishable from success.
		s.Stats.mu.Lock()
		s.Stats.Dropped += int64(count)
		s.Stats.mu.Unlock()
		s.log.Printf("ERROR gave up delivering %d samples: %v; %d already buffered, which is the %d-sample limit (first set %q)",
			count, err, s.buffered(), s.cfg.MaxBufferedSamples, firstSet(batches))
		s.endFlush(obs, 0, true)
		s.requeueMeta(meta, sets)
		s.holdDelivery(s.retryHoldFor(err))
		return err
	}
	s.holdMu.Lock()
	s.holdUntil = time.Time{}
	s.holdMu.Unlock()
	s.Stats.mu.Lock()
	s.Stats.Sent++
	s.Stats.Accepted += int64(resp.Accepted)
	s.Stats.Rejected += int64(len(resp.Rejected))
	s.Stats.mu.Unlock()
	if len(resp.Rejected) > 0 {
		s.log.Printf("WARNING store rejected %d sample(s): %s", len(resp.Rejected), resp.Rejected[0].Reason)
	}
	s.endFlush(obs, resp.Accepted, false)
	return nil
}

// requeueBatches puts an undelivered batch back at the head of its set's
// buffer, ahead of anything queued since, so the retry preserves the order
// the records were read in.
func (s *Sink) requeueBatches(batches []model.Batch, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range batches {
		s.buffers[b.Set] = append(b.Samples, s.buffers[b.Set]...)
	}
	s.pending += count
}

// buffered reports how many samples are waiting.
func (s *Sink) buffered() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending
}

// bufferedRoom reports whether count samples can go back in the buffer
// without exceeding the cap.
func (s *Sink) bufferedRoom(count int) bool {
	return s.buffered()+count <= s.cfg.MaxBufferedSamples
}

// retryHoldFor honours a Retry-After the store sent, which is the interval
// it asked for, and falls back to the fixed hold otherwise.
func (s *Sink) retryHoldFor(err error) time.Duration {
	var retry *wire.ErrRetryable
	if errors.As(err, &retry) && retry.After > 0 {
		return retry.After
	}
	return retryHold
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
		var authErr *wire.ErrAuth
		if errors.As(err, &authErr) {
			s.holdDelivery(deliveryHold)
			s.log.Printf("ERROR %v; retrying in %s", authErr, deliveryHold)
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

// Close flushes and stops the background flusher. A delivery hold is
// cleared first: this is the last flush there will be, so it is worth one
// attempt even against a store that was refusing the credential.
func (s *Sink) Close(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
	s.holdMu.Lock()
	s.holdUntil = time.Time{}
	s.holdMu.Unlock()
	err := s.Flush(ctx)
	// Whatever the last flush could not deliver stays in memory and dies
	// with the process, so it is counted and named here. It is not
	// reported to the observers as a hole: the checkpoints never advanced
	// past these bytes, so the next start re-reads them.
	if n := s.buffered(); n > 0 {
		s.Stats.mu.Lock()
		s.Stats.Dropped += int64(n)
		s.Stats.mu.Unlock()
		s.log.Printf("ERROR %d sample(s) were still buffered at shutdown and were never delivered; they will be re-read from the last checkpoint", n)
	}
	return err
}

// Snapshot returns delivery counters for the progress report.
func (s *Sink) Snapshot() SinkStats { return s.Stats.snapshot() }
