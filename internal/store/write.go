package store

import (
	"container/list"
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
	release := s.idem.acquire(idempotencyKey)
	defer release()
	if s.idem.seen(idempotencyKey) {
		return &wire.WriteResponse{Duplicate: true, CatalogueVersion: s.catVer.Load()}, nil
	}
	if len(req.FieldMeta) > 0 {
		if err := s.applyFieldMeta(req.FieldMeta); err != nil {
			return nil, err
		}
	}
	if err := s.applySetMeta(req.SetMeta); err != nil {
		return nil, err
	}

	resp := &wire.WriteResponse{}
	index := 0
	for _, batch := range req.Batches {
		if err := model.ValidateSetName(batch.Set); err != nil {
			for range batch.Samples {
				resp.Rejected = append(resp.Rejected, wire.Rejection{Index: index, Reason: err.Error()})
				index++
			}
			continue
		}
		// The ingest-progress set is the one reserved name a client may
		// write, and only for its own client label.
		if model.IsReserved(batch.Set) && batch.Set != model.IngestSet {
			for range batch.Samples {
				resp.Rejected = append(resp.Rejected, wire.Rejection{Index: index, Reason: fmt.Sprintf("set %q uses the reserved prefix", batch.Set)})
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
				resp.Rejected = append(resp.Rejected, wire.Rejection{Index: idx, Reason: err.Error()})
				continue
			}
			// A set keyed by `offset` derives its row key from the hint
			// instead of from the field values, so a sample that carries
			// no hint is keyed by set, timestamp and labels alone: two
			// records in the same millisecond would silently overwrite
			// each other, and the response would still count both as
			// accepted. Naming the omission is the only honest answer.
			if scheme == model.KeyOffset && sm.KeyHint == "" {
				resp.Rejected = append(resp.Rejected, wire.Rejection{
					Index:  idx,
					Reason: fmt.Sprintf("set %q is keyed by offset, so every sample needs a key_hint; without one two records sharing a timestamp and labels would collapse into one row", batch.Set),
				})
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
				resp.Rejected = append(resp.Rejected, wire.Rejection{Index: idx, Reason: err.Error()})
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
	for _, m := range metas {
		if m.Set == "" {
			continue
		}
		if err := model.ValidateSetName(m.Set); err != nil {
			return &ErrBadRequest{Msg: err.Error()}
		}
		if model.IsReserved(m.Set) {
			return badRequestf("set %q uses the reserved prefix", m.Set)
		}
		retention, shard := time.Duration(-1), time.Duration(0)
		if m.RetentionMs != nil {
			if *m.RetentionMs < 0 {
				return badRequestf("set %q: retention must not be negative", m.Set)
			}
			retention = time.Duration(*m.RetentionMs) * time.Millisecond
		}
		if m.ShardMs != nil {
			if *m.ShardMs < 0 {
				return badRequestf("set %q: shard width must not be negative", m.Set)
			}
			shard = time.Duration(*m.ShardMs) * time.Millisecond
		}
		if m.RetentionMs != nil || m.ShardMs != nil {
			s.SetRetentionFor(m.Set, retention, shard)
		}
		switch m.KeyScheme {
		case "":
		case model.KeyContent, model.KeyOffset:
			s.SetKeyScheme(m.Set, m.KeyScheme)
		default:
			return badRequestf("set %q: unknown key scheme %q (content or offset)", m.Set, m.KeyScheme)
		}
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
		if m.BucketSet != "" {
			// BucketIndex is unauthenticated client input and is used
			// below as an allocation size, so it is bounded here rather
			// than trusted.
			if m.BucketIndex < 0 || m.BucketIndex >= maxBucketIndex {
				continue
			}
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
	if shard > 0 {
		s.setShard[set] = shard
	}
	s.retentionMu.Unlock()

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
func (s *Store) SaveCatalogue() error { return s.saveCatalogue() }

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
