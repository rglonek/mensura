// Package ingest acquires bytes, extracts samples and delivers them to a
// store over the write API. It never opens the database: one write path
// means one set of semantics for ordering, idempotency, backpressure and
// auth. See docs/design/02-ingest.md.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	// is where "somewhere" ends -- past it the excess is dropped and
	// counted, because an unbounded buffer turns a store outage into an
	// out-of-memory kill that loses everything rather than the tail. The
	// cut is taken from the oldest end of every buffered set in
	// proportion to its size, so no one stream goes dark while another
	// is left untouched.
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
	// now is the clock, overridable through setClock.
	//
	// It is guarded by holdMu, which is not ceremony: the only reader is
	// holding(), which the background flush goroutine calls on every
	// tick, so replacing the function while that goroutine is running is
	// a data race on a func value -- and the race detector reports it
	// against whichever test happens to be running rather than the one
	// that swapped the clock.
	now func() time.Time
}

// setClock replaces the sink's clock. It is the seam a test uses to step
// over a delivery hold without sleeping through it; the write goes under
// the same lock the read already takes.
func (s *Sink) setClock(f func() time.Time) {
	s.holdMu.Lock()
	s.now = f
	s.holdMu.Unlock()
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
	// EndFlush is called after the write, in commit order, and always
	// pairs with a BeginFlush. dropped is true when the batch was
	// abandoned rather than committed.
	//
	// A flush that could only take part of the buffer reports neither
	// half on success: the mark BeginFlush would have taken covers
	// samples this batch does not carry, so there is nothing that batch
	// entitles an observer to acknowledge. Such a flush is invisible
	// here, and the checkpoint waits for one that empties the buffer.
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

// maxFlushRounds bounds how many requests one Flush may send while
// draining a backlog. At the default BatchBytes that is a quarter of a
// gigabyte in one call; past it the flush returns and the next one
// continues, so a source producing faster than the store accepts cannot
// keep a single Flush running forever.
const maxFlushRounds = 64

// Flush ships everything buffered.
//
// Delivery is serialised: the buffer swap and the write happen under one
// lock, so two concurrent flushes cannot deliver out of order and let a
// later batch acknowledge bytes an earlier one has not written yet.
//
// A buffer larger than BatchBytes goes out as several requests within one
// call. Only the take that empties the buffer is announced to the delivery
// observers, so the acknowledgement still covers exactly the bytes the
// store now holds -- but the announcement happens as soon as the backlog
// has drained, rather than waiting for a later flush to find the buffer
// small enough to take in one piece. While it did wait, no checkpoint
// moved for the whole length of the backlog, so a crash during recovery
// from a store outage replayed all of it.
func (s *Sink) Flush(ctx context.Context) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	obs := s.snapshotObservers()
	for round := 1; ; round++ {
		partial, err := s.flushRound(ctx, obs)
		if err != nil || !partial {
			return err
		}
		if round >= maxFlushRounds {
			// Said out loud: the buffer is still short of empty, so no
			// checkpoint has advanced, and a cap nobody can see reads
			// like a flush that finished.
			s.log.Printf("WARNING stopped flushing after %d requests with %d sample(s) still buffered; checkpoints resume once the backlog drains",
				round, s.buffered())
			return nil
		}
	}
}

// flushRound sends at most one request, and reports whether it left
// samples behind.
func (s *Sink) flushRound(ctx context.Context, obs []DeliveryObserver) (bool, error) {
	if s.holding() {
		// Delivery is known to be failing for a reason a retry cannot
		// fix. The buffer keeps what it has; nothing is dropped and no
		// observer is told anything, so no checkpoint moves either way --
		// unless the wait has taken the buffer past its cap, which is the
		// one thing holding cannot survive.
		s.enforceBufferCap(obs)
		return false, nil
	}
	s.mu.Lock()
	if s.pending == 0 {
		s.mu.Unlock()
		// Metadata still has to reach the store even when no samples are
		// waiting, or a spec's declarations would sit in the queue until
		// the first sample happens to arrive.
		return false, s.flushMetaOnly(ctx)
	}
	batches, count, partial := s.takeLocked()
	if !partial {
		// Under the buffer lock: the batch is now fixed, and any Add
		// racing this flush is either already in it or blocked until it
		// is not.
		//
		// A partial take says nothing to the observers on purpose. Their
		// high-water mark covers every byte handed to the sink, and some
		// of those samples are still in the buffer, so announcing this
		// batch would let a commit acknowledge bytes it does not carry.
		// The checkpoint waits for the take that empties the buffer,
		// which the caller keeps sending rounds until it reaches.
		s.beginFlush(obs)
	}
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
			s.reportLost(obs, partial)
			s.discardMeta(meta, sets, fatal)
			if s.cfg.MaxFatalDrops > 0 && drops >= int64(s.cfg.MaxFatalDrops) {
				return false, err
			}
			// No further rounds: the loss has been announced, so the
			// observers are frozen until a later flush thaws them, and
			// there is nothing a second request in this call can add.
			return false, nil
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
			return false, err
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
			return false, err
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
			return false, err
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
		s.reportLost(obs, partial)
		s.requeueMeta(meta, sets)
		s.holdDelivery(s.retryHoldFor(err))
		return false, err
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
	// Symmetric with the beginFlush above. A partial take deliberately
	// announces nothing, because some of the buffer's samples are not in
	// this batch and a high-water mark that covered them would let a
	// commit acknowledge bytes the store does not hold. Ending a flush
	// that was never begun leaned on commitInflight happening to no-op
	// when inflight has not moved -- an invariant no observer contract
	// states, so the next implementation of one would have advanced its
	// checkpoint over undelivered data. A hole left frozen here is
	// released by the first flush that empties the buffer, which is also
	// the first moment a rewind would not throw away a backlog.
	if !partial {
		s.endFlush(obs, resp.Accepted, false)
	}
	return partial, nil
}

// takeLocked moves buffered samples into batches, bounded by BatchBytes,
// and reports how many it took and whether it left anything behind. It
// must be called with the buffer lock held.
//
// The size bound is what BatchBytes was always documented to be and never
// was: nothing read it, so a request was bounded only by a sample count.
// A buffer that filled during an outage then went out as one body, and a
// body past the store's max_request_bytes comes back 413 -- a status the
// client classifies as fatal, so the whole buffer was dropped rather than
// delivered in pieces.
func (s *Sink) takeLocked() ([]model.Batch, int, bool) {
	sets := make([]string, 0, len(s.buffers))
	for set := range s.buffers {
		sets = append(sets, set)
	}
	sort.Strings(sets)

	batches := make([]model.Batch, 0, len(sets))
	taken, size := 0, 0
	for _, set := range sets {
		samples := s.buffers[set]
		if len(samples) == 0 {
			delete(s.buffers, set)
			continue
		}
		n := 0
		for n < len(samples) {
			sz := sampleBytes(&samples[n])
			// Always take at least one sample: a single sample bigger
			// than the budget would otherwise never leave the buffer.
			if taken+n > 0 && s.cfg.BatchBytes > 0 && size+sz > s.cfg.BatchBytes {
				break
			}
			size += sz
			n++
		}
		if n == 0 {
			break
		}
		batches = append(batches, model.Batch{Set: set, Samples: samples[:n]})
		taken += n
		if n == len(samples) {
			delete(s.buffers, set)
			continue
		}
		// Copied, not resliced: requeueBatches prepends to the batch it
		// was handed, and a leftover sharing that array would be written
		// over by the prepend.
		s.buffers[set] = append([]model.Sample(nil), samples[n:]...)
		break
	}
	s.pending -= taken
	return batches, taken, s.pending > 0
}

// sampleBytes estimates what one sample costs in the request body. An
// estimate on purpose: marshalling everything twice to find out exactly
// would cost more than the bound saves.
func sampleBytes(s *model.Sample) int {
	n := 48 + len(s.KeyHint)
	for k, v := range s.Labels {
		n += len(k) + len(v) + 8
	}
	for k, v := range s.Fields {
		n += len(k) + 14
		if v.T == model.TypeString {
			n += len(v.S) + 2
		} else {
			n += 20
		}
	}
	return n
}

// reportLost tells the observers a batch is gone.
//
// A partial take was never announced to them, so the announcement is made
// here before the loss is: the freeze it installs is what stops a later
// flush acknowledging the bytes these samples came from.
func (s *Sink) reportLost(obs []DeliveryObserver, partial bool) {
	if partial {
		s.mu.Lock()
		s.beginFlush(obs)
		s.mu.Unlock()
	}
	s.endFlush(obs, 0, true)
}

// enforceBufferCap drops the oldest buffered samples when a held delivery
// has let the buffer pass MaxBufferedSamples.
//
// Holding is only worth doing while there is somewhere to put the batch.
// The cap used to be consulted only where a batch was put back, so while
// delivery was held -- 30 seconds after a rejected credential, or as long
// as a Retry-After the store chose -- Add kept appending with nothing
// bounding it at all, and the outage the cap exists to survive became an
// out-of-memory kill instead.
func (s *Sink) enforceBufferCap(obs []DeliveryObserver) {
	if s.cfg.MaxBufferedSamples <= 0 {
		return
	}
	s.mu.Lock()
	over := s.pending - s.cfg.MaxBufferedSamples
	if over <= 0 {
		s.mu.Unlock()
		return
	}
	sets := make([]string, 0, len(s.buffers))
	for set := range s.buffers {
		sets = append(sets, set)
	}
	sort.Strings(sets)
	// Every set gives up the same share of its own oldest end, rather
	// than the first set alphabetically giving up everything.
	//
	// The buffers are per-set FIFOs with no cross-set ordering to read,
	// so "the globally oldest samples" is not a thing this structure can
	// name. Draining the sorted list in order was the worst available
	// approximation of it: with sets `access` and `zzz` over the cap,
	// the whole of `access` was discarded before `zzz` lost a single
	// sample, so one stream went dark while another was untouched. A
	// proportional cut takes the oldest samples *within* each set and
	// leaves every stream equally thinned, which is what an operator
	// reading "the oldest buffered samples are dropped" would expect to
	// see on a dashboard.
	dropped := 0
	target := over
	for _, set := range sets {
		if over <= 0 {
			break
		}
		b := s.buffers[set]
		if len(b) == 0 {
			delete(s.buffers, set)
			continue
		}
		// Rounded up, so a set holding a handful of samples still
		// contributes and the loop always makes progress.
		n := (len(b)*target + s.pending - 1) / s.pending
		if n > over {
			n = over
		}
		if n > len(b) {
			n = len(b)
		}
		if n <= 0 {
			continue
		}
		if n == len(b) {
			delete(s.buffers, set)
		} else {
			s.buffers[set] = append([]model.Sample(nil), b[n:]...)
		}
		over -= n
		dropped += n
	}
	// A rounding shortfall is finished off in order; by construction it
	// is at most one sample per set.
	for _, set := range sets {
		if over <= 0 {
			break
		}
		b := s.buffers[set]
		n := over
		if n > len(b) {
			n = len(b)
		}
		if n <= 0 {
			continue
		}
		if n == len(b) {
			delete(s.buffers, set)
		} else {
			s.buffers[set] = append([]model.Sample(nil), b[n:]...)
		}
		over -= n
		dropped += n
	}
	s.pending -= dropped
	// The observers hear that the sink took these samples and lost them:
	// a checkpoint that moved past them would bury them for good.
	s.beginFlush(obs)
	s.mu.Unlock()

	s.Stats.mu.Lock()
	s.Stats.Dropped += int64(dropped)
	total := s.Stats.Dropped
	s.Stats.mu.Unlock()
	// On the milestones: a store that is refusing writes makes this fire
	// on every flush tick, and a line each would bury everything else.
	if isLogMilestone(total) {
		s.log.Printf("ERROR delivery is held and the buffer is full: dropped %d sample(s) from the oldest end of every buffered set, %d dropped in total, which is the %d-sample limit",
			dropped, total, s.cfg.MaxBufferedSamples)
	}
	s.endFlush(obs, 0, true)
}

// discardMeta drops declarations the store refused outright.
//
// They are deliberately not requeued. The store applies metadata before
// any batch and answers a bad declaration with 400, which the client
// classifies as fatal -- so putting it back at the head of the queue made
// the next batch fail for the same reason, and the one after that: one
// unusable line in a spec dropped every sample the ingester produced,
// at full rate, until MaxFatalDrops gave up on the process. Losing those
// kinds and units for the life of the process is the far smaller loss,
// and it is said out loud so the spec gets fixed.
func (s *Sink) discardMeta(meta []wire.FieldMeta, sets []wire.SetMeta, cause error) {
	if len(meta) == 0 && len(sets) == 0 {
		return
	}
	names := make([]string, 0, len(meta)+len(sets))
	for _, m := range meta {
		names = append(names, m.Set+"."+m.Field)
		if len(names) == 8 {
			break
		}
	}
	for _, m := range sets {
		if len(names) == 8 {
			break
		}
		names = append(names, m.Set+" (set options)")
	}
	more := ""
	if len(meta)+len(sets) > len(names) {
		more = fmt.Sprintf(" and %d more", len(meta)+len(sets)-len(names))
	}
	s.log.Printf("ERROR the store refused these declarations, so they are discarded rather than resent with every later batch: %s%s (%v)",
		strings.Join(names, ", "), more, cause)
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
		var fatal *wire.ErrFatal
		if errors.As(err, &fatal) {
			// Not requeued: it would be refused identically forever, and
			// every batch it travelled with would be refused with it.
			s.discardMeta(meta, sets, fatal)
			return nil
		}
		s.requeueMeta(meta, sets)
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
	// Flushed until the buffer is empty or a flush stops making progress:
	// one flush carries at most BatchBytes, so a buffer that grew during
	// an outage needs more than one to leave.
	err := s.Flush(ctx)
	for err == nil {
		before := s.buffered()
		if before == 0 {
			break
		}
		if err = s.Flush(ctx); err != nil || s.buffered() >= before {
			break
		}
	}
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
