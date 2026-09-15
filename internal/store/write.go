package store

import (
	"container/list"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/rglonek/mensura/internal/engine"
	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// ErrBadRequest marks a write failure caused by what the client sent
// rather than by the store: a malformed set name, a negative retention, an
// unknown key scheme. The distinction is not cosmetic. handleWrite turns
// every other error into a 500, wire.Client treats >= 500 as retryable, and
// the ingest sink drops a batch that runs out of retries -- so one bad
// metadata field in a spec used to destroy every batch the ingester
// produced, at full rate, for the life of the process. A client-input fault
// has to come back as a 4xx so it is fatal on the first attempt and the
// spec gets fixed.
type ErrBadRequest struct{ Msg string }

func (e *ErrBadRequest) Error() string { return e.Msg }

func badRequestf(format string, args ...any) error {
	return &ErrBadRequest{Msg: fmt.Sprintf(format, args...)}
}

// idempotencyCache remembers recently committed request keys so a retry
// that crossed with its own success does not write twice.
//
// A key is only ever recorded by record(), which the write path calls
// after the batch has committed. Recording at the start of the request
// instead would answer the retry of a *failed* write with "duplicate",
// and the batch would be lost with nothing counting it.
type idempotencyCache struct {
	mu   sync.Mutex
	max  int
	ll   *list.List
	keys map[string]*list.Element

	// inflight serialises requests that carry the same key. Checking
	// `seen` and recording afterwards is only atomic against a retry that
	// waits for its answer; two retries in flight at once both missed the
	// cache and both wrote.
	inflightMu sync.Mutex
	inflight   map[string]*sync.WaitGroup
}

func newIdempotencyCache(max int) *idempotencyCache {
	return &idempotencyCache{max: max, ll: list.New(), keys: map[string]*list.Element{}, inflight: map[string]*sync.WaitGroup{}}
}

// acquire claims a key for the duration of one write, waiting for any
// request already writing under the same key. The returned function must
// be called when the write is done.
func (c *idempotencyCache) acquire(key string) func() {
	if key == "" {
		return func() {}
	}
	for {
		c.inflightMu.Lock()
		wg, busy := c.inflight[key]
		if !busy {
			wg = &sync.WaitGroup{}
			wg.Add(1)
			c.inflight[key] = wg
			c.inflightMu.Unlock()
			return func() {
				c.inflightMu.Lock()
				delete(c.inflight, key)
				c.inflightMu.Unlock()
				wg.Done()
			}
		}
		c.inflightMu.Unlock()
		wg.Wait()
	}
}

// seen reports whether a key has already been committed, without recording
// it.
func (c *idempotencyCache) seen(key string) bool {
	if key == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.keys[key]
	if ok {
		c.ll.MoveToFront(el)
	}
	return ok
}

// record marks a key as committed. It is idempotent.
func (c *idempotencyCache) record(key string) {
	if key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.keys[key]; ok {
		c.ll.MoveToFront(el)
		return
	}
	c.keys[key] = c.ll.PushFront(key)
	if c.ll.Len() > c.max {
		last := c.ll.Back()
		c.ll.Remove(last)
		delete(c.keys, last.Value.(string))
	}
}

// Write commits a write request. Partial rejection is normal: bad samples
// are named and the rest is committed, because dropping a whole batch for
// one malformed row loses far more than it protects.
func (s *Store) Write(req *wire.WriteRequest, idempotencyKey, clientName string) (*wire.WriteResponse, error) {
	// Counted for the whole of the write, so Close waits for it rather
	// than tearing the engine down while PutBatch is still applying. A
	// write is the operation that runs long here: it blocks on the
	// engine's own back-pressure when L0 is stalled, which is exactly
	// when a shutdown is most likely to overtake it.
	if !s.enterEngine() {
		return nil, ErrClosed
	}
	defer s.leaveEngine()
	release := s.idem.acquire(idempotencyKey)
	defer release()
	if s.idem.seen(idempotencyKey) {
		return &wire.WriteResponse{Duplicate: true, CatalogueVersion: s.catVer.Load()}, nil
	}
	declaredAt := s.catVer.Load()
	if len(req.FieldMeta) > 0 {
		if err := s.applyFieldMeta(req.FieldMeta); err != nil {
			return nil, err
		}
	}
	if err := s.applySetMeta(req.SetMeta); err != nil {
		return nil, err
	}
	// A declaration that changed the catalogue is persisted now, not on
	// the next thirty-second tick.
	//
	// Everything else in the catalogue is rediscovered by the next write:
	// observeSet re-learns a set's labels, fields and time range from the
	// samples themselves. A declaration is not. It travels once per ingest
	// process (Sink.DeclareFields and DeclareSets mark it sent and never
	// repeat it), so an unclean stop inside the save window lost it for
	// good -- and for `sets:` that means the retention and shard width
	// SetRetentionFor exists to persist, so the set was routed to the
	// unsharded shard the sweep skips and then kept forever, silently.
	// That is the exact failure its own comment describes preventing.
	//
	// Logged rather than returned: the batch below is still accepted, and
	// a metadata write that failed is not a reason to make the ingester
	// resend data the store is about to hold.
	if s.catVer.Load() != declaredAt {
		if err := s.saveCatalogue(); err != nil {
			s.cfg.Logger.Printf("ERROR saving catalogue after a declaration changed it: %v", err)
		}
	}

	resp := &wire.WriteResponse{}
	index := 0
	for _, batch := range req.Batches {
		if err := model.ValidateSetName(batch.Set); err != nil {
			for range batch.Samples {
				resp.Reject(index, err.Error())
				index++
			}
			continue
		}
		// The ingest-progress set is the one reserved name a client may
		// write, and only for its own client label.
		if model.IsReserved(batch.Set) && batch.Set != model.IngestSet {
			for range batch.Samples {
				resp.Reject(index, fmt.Sprintf("set %q uses the reserved prefix", batch.Set))
				index++
			}
			continue
		}

		byShard := map[string][]engine.Record{}
		// Only samples that were actually accepted may shape the
		// catalogue: a rejected row must not widen the label set or move
		// the set's first/last timestamp. They are tracked per shard,
		// because a batch spanning a shard boundary is not one atomic
		// write: if the second shard fails, the catalogue must still
		// describe what the first one committed.
		acceptedByShard := map[string][]model.Sample{}
		scheme := s.keyScheme(batch.Set)
		for i := range batch.Samples {
			sm := &batch.Samples[i]
			idx := index
			index++
			if err := sm.Validate(); err != nil {
				resp.Reject(idx, err.Error())
				continue
			}
			// A set keyed by `offset` derives its row key from the hint
			// instead of from the field values, so a sample that carries
			// no hint is keyed by set, timestamp and labels alone: two
			// records in the same millisecond would silently overwrite
			// each other, and the response would still count both as
			// accepted. Naming the omission is the only honest answer.
			if scheme == model.KeyOffset && sm.KeyHint == "" {
				resp.Reject(idx, fmt.Sprintf("set %q is keyed by offset, so every sample needs a key_hint; without one two records sharing a timestamp and labels would collapse into one row", batch.Set))
				continue
			}
			if batch.Set == model.IngestSet && clientName != "" {
				if sm.Labels == nil {
					sm.Labels = map[string]string{}
				}
				sm.Labels["client"] = clientName
			}
			row, err := s.rowFor(batch.Set, sm)
			if err != nil {
				// A fault the store owns is not a rejection of this
				// sample. rowFor reports both through one error, and
				// naming a dictionary write that failed as "this sample
				// was refused" told the ingester its batch had landed
				// minus a few rows -- so the checkpoint advanced over
				// records the store never held. The whole request fails
				// instead, which the write client classifies as
				// retryable and the sink holds rather than drops.
				if errors.Is(err, ErrStoreFault) {
					return nil, err
				}
				resp.Reject(idx, err.Error())
				continue
			}
			pk := model.PrimaryKey(batch.Set, sm, scheme)
			shard := s.shardName(batch.Set, sm.TSMs)
			byShard[shard] = append(byShard[shard], engine.Record{Key: pk, Row: row})
			acceptedByShard[shard] = append(acceptedByShard[shard], *sm)
			resp.Accepted++
		}
		for shard, recs := range byShard {
			if err := s.db.PutBatch(shard, recs); err != nil {
				return nil, err
			}
			s.observeSet(batch.Set, acceptedByShard[shard])
		}
	}
	// The key is recorded only now, once every batch has committed: a
	// retry of a request that failed part-way must write, not be answered
	// "duplicate".
	s.idem.record(idempotencyKey)
	resp.CatalogueVersion = s.catVer.Load()
	return resp, nil
}

// applySetMeta records what a spec declares about whole sets: retention,
// shard width and key scheme. Without this the spec's `sets:` block would
// be parsed and then quietly ignored, and `key: offset` in particular
// would never take effect.
func (s *Store) applySetMeta(metas []wire.SetMeta) error {
	// Validated in full before anything is applied, which is the shape
	// applyFieldMeta already has. Validating and applying in one pass let
	// a declaration that the *next* entry made the request fail land
	// anyway: the caller sees a 400 and the store keeps half of what it
	// refused, so two ingesters sending the same spec could leave the set
	// with a retention nobody's spec asked for.
	for _, m := range metas {
		if err := checkSetMeta(m); err != nil {
			return err
		}
	}
	for _, m := range metas {
		if m.Set == "" {
			continue
		}
		retention, shard := time.Duration(-1), time.Duration(0)
		if m.RetentionMs != nil {
			retention = time.Duration(*m.RetentionMs) * time.Millisecond
		}
		if m.ShardMs != nil {
			shard = time.Duration(*m.ShardMs) * time.Millisecond
		}
		if m.RetentionMs != nil || m.ShardMs != nil {
			s.SetRetentionFor(m.Set, retention, shard)
		}
		if m.KeyScheme != "" {
			s.SetKeyScheme(m.Set, m.KeyScheme)
		}
	}
	return nil
}

// checkSetMeta is the validating half of applySetMeta: everything a
// client can get wrong about one set declaration, decided before any of
// them is applied.
func checkSetMeta(m wire.SetMeta) error {
	if m.Set == "" {
		return nil
	}
	if err := model.ValidateSetName(m.Set); err != nil {
		return &ErrBadRequest{Msg: err.Error()}
	}
	// The same exemption applyFieldMeta makes. The ingest-progress set
	// is the one reserved name a client may write, so refusing to let
	// it carry a retention or a shard width meant the one set every
	// ingester produces was also the one set no spec could age out.
	if model.IsReserved(m.Set) && m.Set != model.IngestSet {
		return badRequestf("set %q uses the reserved prefix", m.Set)
	}
	if m.RetentionMs != nil && *m.RetentionMs < 0 {
		return badRequestf("set %q: retention must not be negative", m.Set)
	}
	if m.ShardMs != nil && *m.ShardMs < 0 {
		return badRequestf("set %q: shard width must not be negative", m.Set)
	}
	switch m.KeyScheme {
	case "", model.KeyContent, model.KeyOffset:
	default:
		return badRequestf("set %q: unknown key scheme %q (content or offset)", m.Set, m.KeyScheme)
	}
	return nil
}

func (s *Store) keyScheme(set string) model.KeyScheme {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.catalogue[set]; ok && e.KeyScheme != "" {
		return e.KeyScheme
	}
	return model.KeyContent
}

// SetKeyScheme records how a set's rows are keyed. Offset keying keeps
// every occurrence distinct; content keying makes replays idempotent.
func (s *Store) SetKeyScheme(set string, scheme model.KeyScheme) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entryLocked(set)
	e.KeyScheme = scheme
	s.catVer.Add(1)
}

// rowFor turns a sample into an engine row: the timestamp, one integer
// column per label (its dictionary index) and the fields as they came.
//
// Every reason to reject the sample is evaluated before the first value is
// interned. Interning as we went meant a sample rejected by a later check
// had already burned dictionary entries and written them to disk, so a
// stream of malformed samples grew the dictionary permanently and could
// exhaust a label key's cardinality budget without storing a single row.
func (s *Store) rowFor(set string, sm *model.Sample) (engine.Row, error) {
	for k, v := range sm.Labels {
		// A label named "timestamp" would overwrite the indexed column
		// with a dictionary index, putting the row at a fabricated time
		// where no range scan can find it. Reject it rather than lose it.
		if k == model.TimestampField {
			return nil, fmt.Errorf("label key %q is reserved for the indexed timestamp column", k)
		}
		if err := model.ValidateLabelValue(v); err != nil {
			return nil, fmt.Errorf("label %q: %w", k, err)
		}
	}
	for k := range sm.Fields {
		if _, clash := sm.Labels[k]; clash {
			return nil, fmt.Errorf("%q is both a label and a field", k)
		}
	}

	row := make(engine.Row, len(sm.Labels)+len(sm.Fields)+1)
	row[model.TimestampField] = model.Int(sm.TSMs)
	for k, v := range sm.Labels {
		idx, err := s.intern(k, v)
		if err != nil {
			return nil, err
		}
		row[k] = model.Int(int64(idx))
	}
	for k, v := range sm.Fields {
		row[k] = v
	}
	return row, nil
}

// observeSet keeps the catalogue current: which labels and fields a set
// carries, and the time range it covers.
func (s *Store) observeSet(set string, samples []model.Sample) {
	if len(samples) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entryLocked(set)
	// Only a schema change bumps the version. Advancing the observed time
	// range does not: it happens on every batch, and versioning it made
	// the catalogue ETag useless and woke every client that watches
	// catalogue_version for metadata changes.
	schemaChanged := false
	now := time.Now().UnixMilli()
	for i := range samples {
		sm := &samples[i]
		if e.FirstTSMs == 0 || sm.TSMs < e.FirstTSMs {
			e.FirstTSMs = sm.TSMs
		}
		if sm.TSMs > e.LastTSMs {
			e.LastTSMs = sm.TSMs
		}
		for k := range sm.Labels {
			if _, ok := e.Labels[k]; !ok {
				e.Labels[k] = struct{}{}
				schemaChanged = true
			}
		}
		for k := range sm.Fields {
			f, ok := e.Fields[k]
			if !ok {
				f = &fieldEntry{Kind: model.KindGauge}
				e.Fields[k] = f
				schemaChanged = true
			}
			f.LastSeenMs = now
		}
	}
	if schemaChanged {
		s.catVer.Add(1)
	}
}

func (s *Store) entryLocked(set string) *setEntry {
	e, ok := s.catalogue[set]
	if !ok {
		e = &setEntry{Name: set, Fields: map[string]*fieldEntry{}, Labels: map[string]struct{}{}}
		s.catalogue[set] = e
	}
	return e
}

// equal compares two catalogue field records by value.
//
// Struct comparison was wrong here: LimitMin and LimitMax are pointers,
// and a decoded request allocates fresh ones on every call, so
// re-declaring identical limits always read as a change and bumped
// CatalogueVersion -- the ETag churn that version's own contract rules
// out.
func (a fieldEntry) equal(b fieldEntry) bool {
	return a.Kind == b.Kind && a.Unit == b.Unit && a.UnitHint == b.UnitHint &&
		a.Description == b.Description && a.MaxInterval == b.MaxInterval &&
		sameLimit(a.LimitMin, b.LimitMin) && sameLimit(a.LimitMax, b.LimitMax) &&
		a.BucketSet == b.BucketSet && a.BucketIndex == b.BucketIndex &&
		a.BucketEdge == b.BucketEdge && a.LastSeenMs == b.LastSeenMs
}

// bucketSetEqual compares two bucket-set records by value.
func bucketSetEqual(a, b wire.BucketSetInfo) bool {
	if a.Unit != b.Unit || len(a.Buckets) != len(b.Buckets) || len(a.Edges) != len(b.Edges) {
		return false
	}
	for i := range a.Buckets {
		if a.Buckets[i] != b.Buckets[i] {
			return false
		}
	}
	for i := range a.Edges {
		if a.Edges[i] != b.Edges[i] {
			return false
		}
	}
	return true
}

func sameLimit(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// maxBucketIndex bounds a declared histogram bucket position. A bucket set
// wider than this is a mistake or an attack, not a histogram; the index is
// used as an allocation size, so it may not be taken on trust.
const maxBucketIndex = 4096

// applyFieldMeta merges declared metadata. Last writer wins, but a
// disagreement between two ingesters is recorded and surfaced rather than
// resolved silently.
//
// Names are validated exactly as a batch's are. They used to be taken on
// trust, and entryLocked creates whatever it is given, so any client with
// the write scope could put a reserved name -- or one carrying the '@'
// that separates a set from its shard suffix -- into the catalogue. Such
// an entry was persisted, served from /v1/catalogue, accepted by the MQL
// validator as a real set, and could not be removed through
// DELETE /v1/admin/sets/, which does validate.
func (s *Store) applyFieldMeta(metas []wire.FieldMeta) error {
	for _, m := range metas {
		if m.Set == "" || m.Field == "" {
			continue
		}
		if err := model.ValidateSetName(m.Set); err != nil {
			return &ErrBadRequest{Msg: err.Error()}
		}
		if model.IsReserved(m.Set) && m.Set != model.IngestSet {
			return badRequestf("set %q uses the reserved prefix", m.Set)
		}
		if err := model.ValidateFieldName(m.Field); err != nil {
			return &ErrBadRequest{Msg: err.Error()}
		}
		// The kind is checked exactly as the names are, and for the same
		// reason: it is stored, served from /v1/catalogue and read back by
		// the MQL validator, which recognises four values and quietly
		// ignores anything else. An unrecognised kind therefore reached a
		// dashboard as metadata that looks declared and behaves as if it
		// were absent.
		if m.Kind != "" {
			if err := model.ValidateKind(m.Kind); err != nil {
				return badRequestf("%s.%s: %s", m.Set, m.Field, err.Error())
			}
		}
		// Named here rather than skipped below. The index is used as an
		// allocation size so it cannot be taken on trust, but silently
		// ignoring the declaration left the field in the catalogue with
		// no bucket set and nothing anywhere saying why the heatmap it
		// was declared for draws nothing.
		if m.BucketSet != "" && (m.BucketIndex < 0 || m.BucketIndex >= maxBucketIndex) {
			return badRequestf("%s.%s: bucket index %d is outside 0..%d", m.Set, m.Field, m.BucketIndex, maxBucketIndex-1)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Only a real change bumps the version. Bumping unconditionally made
	// the /v1/catalogue ETag miss on every write that carried metadata,
	// which is the churn CatalogueVersion's own contract rules out.
	changed := false
	for _, m := range metas {
		if m.Set == "" || m.Field == "" {
			continue
		}
		_, hadSet := s.catalogue[m.Set]
		e := s.entryLocked(m.Set)
		if !hadSet {
			changed = true
		}
		f, ok := e.Fields[m.Field]
		if !ok {
			f = &fieldEntry{}
			e.Fields[m.Field] = f
			changed = true
		}
		before := *f
		if ok && f.Kind != "" && m.Kind != "" && f.Kind != m.Kind {
			s.conflicts = append(s.conflicts, wire.CatalogueConflict{
				Set: m.Set, Field: m.Field, Was: string(f.Kind), Now: string(m.Kind),
			})
			if len(s.conflicts) > 64 {
				s.conflicts = s.conflicts[len(s.conflicts)-64:]
			}
		}
		if m.Kind != "" {
			f.Kind = m.Kind
		}
		if m.Unit != "" {
			f.Unit = m.Unit
		}
		if m.UnitHint != "" {
			f.UnitHint = m.UnitHint
		}
		if m.Description != "" {
			f.Description = m.Description
		}
		// Milliseconds win; the whole-second field is only read so an
		// older ingester's metadata still lands.
		switch {
		case m.MaxIntervalMs > 0:
			f.MaxInterval = m.MaxIntervalMs
		case m.MaxIntervalS > 0:
			f.MaxInterval = int64(m.MaxIntervalS) * 1000
		}
		// The value is copied, not the pointer: the request's own
		// pointers must not stay reachable from the catalogue.
		if m.LimitMin != nil {
			v := *m.LimitMin
			f.LimitMin = &v
		}
		if m.LimitMax != nil {
			v := *m.LimitMax
			f.LimitMax = &v
		}
		// BucketIndex is unauthenticated client input and is used below as
		// an allocation size. It is bounded in the validating pass above,
		// which refuses the request rather than skipping the rest of this
		// iteration: the `continue` that used to sit here jumped over the
		// change detection at the bottom of the loop, so a kind or unit
		// change carried in the same declaration was applied to the
		// catalogue with CatalogueVersion standing still -- and every
		// client holding the old ETag was answered 304 for a catalogue
		// that had moved.
		if m.BucketSet != "" {
			f.BucketSet = m.BucketSet
			f.BucketIndex = m.BucketIndex
			f.BucketEdge = m.BucketEdge
			if e.BucketSets == nil {
				e.BucketSets = map[string]wire.BucketSetInfo{}
			}
			// Built on a copy, not in place: the stored slices are what a
			// rendered catalogue and a running heatmap query were built
			// from, and writing through them changes what those already
			// hold. The copy is also what lets the comparison below see
			// whether anything actually moved.
			stored := e.BucketSets[m.BucketSet]
			bs := wire.BucketSetInfo{
				Buckets: append([]string(nil), stored.Buckets...),
				Edges:   append([]float64(nil), stored.Edges...),
				Unit:    stored.Unit,
			}
			for len(bs.Buckets) <= m.BucketIndex {
				bs.Buckets = append(bs.Buckets, "")
				bs.Edges = append(bs.Edges, 0)
			}
			bs.Buckets[m.BucketIndex] = m.Field
			bs.Edges[m.BucketIndex] = m.BucketEdge
			if m.Unit != "" {
				bs.Unit = m.Unit
			}
			// Only a real change bumps the version. Marking every
			// declaration as one meant each ingest process that declared
			// a bucket set moved CatalogueVersion on startup and woke
			// every client watching it -- the same churn the limits
			// comparison above exists to prevent.
			if !bucketSetEqual(stored, bs) {
				e.BucketSets[m.BucketSet] = bs
				changed = true
			}
		}
		if !before.equal(*f) {
			changed = true
		}
	}
	if changed {
		s.catVer.Add(1)
	}
	return nil
}

// SetRetentionFor records a per-set retention and shard width supplied by
// a spec, so a set declared "keep 7 days at hourly shards" behaves that way
// without editing the store's own config.
//
// The overrides live in their own map behind retentionMu rather than in
// cfg: cfg is read lock-free from the write path, and mutating it here
// would be a concurrent map write.
func (s *Store) SetRetentionFor(set string, retention, shard time.Duration) {
	s.retentionMu.Lock()
	if s.setRetention == nil {
		s.setRetention = map[string]time.Duration{}
	}
	if s.setShard == nil {
		s.setShard = map[string]time.Duration{}
	}
	if retention >= 0 {
		s.setRetention[set] = retention
	}
	shardChanged := false
	if shard > 0 {
		shardChanged = s.setShard[set] != shard
		s.setShard[set] = shard
	}
	s.retentionMu.Unlock()

	// Said out loud, the way a width from the config file is at startup.
	// The suffix encoding can only express whole days and the hour counts
	// that divide one, so anything else is rounded down -- and a spec is
	// the only place that rounding happened in silence, because
	// warnInexactShardWidths reads cfg and a spec-supplied width never
	// goes there. An operator who wrote `shard: 30m` got hourly shards
	// with nothing anywhere saying so. Only on a change, because the
	// declaration arrives again on every ingest process start.
	if shardChanged {
		if hours, exact := normaliseShardWidth(shard); !exact {
			s.cfg.Logger.Printf("WARNING set %s declares a shard width of %s, which is not a whole number of days or an hour count dividing a day; using %dh",
				set, shard, hours)
		}
	}

	// Persisted alongside the catalogue. The declaration travels once per
	// ingest process, so an ingester that is already running never
	// repeats it: a store that only held it in memory forgot it on
	// restart and then kept the set forever, in the unsharded shard that
	// retention skips.
	s.mu.Lock()
	_, known := s.catalogue[set]
	e := s.entryLocked(set)
	// entryLocked creates the entry, so a spec whose sets: block names a
	// set with no field metadata added it to the catalogue with the
	// version standing still -- and handleCatalogue answers 304 to every
	// client still holding the old ETag, so a datasource that caches it
	// never saw the set at all. Only a real change moves the version,
	// which is the rule the field-metadata path already follows: a
	// declaration repeated on every ingest start must not wake every
	// client watching catalogue_version.
	changed := !known
	if retention >= 0 {
		ms := retention.Milliseconds()
		if e.RetentionMs == nil || *e.RetentionMs != ms {
			changed = true
		}
		e.RetentionMs = &ms
	}
	if shard > 0 {
		ms := shard.Milliseconds()
		if e.ShardMs == nil || *e.ShardMs != ms {
			changed = true
		}
		e.ShardMs = &ms
	}
	s.mu.Unlock()
	if changed {
		s.catVer.Add(1)
	}
}

// retentionFor resolves the effective retention for a logical set: a
// spec-supplied override first, then the configured per-set value, then
// the global default.
func (s *Store) retentionFor(set string) time.Duration {
	s.retentionMu.RLock()
	d, ok := s.setRetention[set]
	s.retentionMu.RUnlock()
	if ok {
		return d
	}
	if d, ok := s.cfg.SetRetention[set]; ok {
		return d
	}
	return s.cfg.Retention
}

// SaveCatalogue persists the catalogue; the HTTP layer calls it on a timer
// so a crash loses at most the last interval of metadata, which is
// rediscovered on the next write anyway.
//
// Gated, unlike the unexported form Close itself uses: this one is called
// from a background ticker and from the admin endpoints, and it writes
// through the engine.
func (s *Store) SaveCatalogue() error {
	if !s.enterEngine() {
		return ErrClosed
	}
	defer s.leaveEngine()
	return s.saveCatalogue()
}

// LabelKeys lists the label keys present on a set.
func (s *Store) LabelKeys(set string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.catalogue[set]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(e.Labels))
	for k := range e.Labels {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
