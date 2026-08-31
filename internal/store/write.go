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
}

func newIdempotencyCache(max int) *idempotencyCache {
	return &idempotencyCache{max: max, ll: list.New(), keys: map[string]*list.Element{}}
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
	if s.idem.seen(idempotencyKey) {
		return &wire.WriteResponse{Duplicate: true, CatalogueVersion: s.catVer.Load()}, nil
	}
	if len(req.FieldMeta) > 0 {
		s.applyFieldMeta(req.FieldMeta)
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
		scheme := s.keyScheme(batch.Set)
		// Only samples that were actually accepted may shape the
		// catalogue: a rejected row must not widen the label set or move
		// the set's first/last timestamp.
		accepted := make([]model.Sample, 0, len(batch.Samples))
		for i := range batch.Samples {
			sm := &batch.Samples[i]
			idx := index
			index++
			if err := sm.Validate(); err != nil {
				resp.Rejected = append(resp.Rejected, wire.Rejection{Index: idx, Reason: err.Error()})
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
			accepted = append(accepted, *sm)
			resp.Accepted++
		}
		for shard, recs := range byShard {
			if err := s.db.PutBatch(shard, recs); err != nil {
				return nil, err
			}
		}
		s.observeSet(batch.Set, accepted)
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
			return err
		}
		if model.IsReserved(m.Set) {
			return fmt.Errorf("set %q uses the reserved prefix", m.Set)
		}
		retention, shard := time.Duration(-1), time.Duration(0)
		if m.RetentionMs != nil {
			if *m.RetentionMs < 0 {
				return fmt.Errorf("set %q: retention must not be negative", m.Set)
			}
			retention = time.Duration(*m.RetentionMs) * time.Millisecond
		}
		if m.ShardMs != nil {
			if *m.ShardMs < 0 {
				return fmt.Errorf("set %q: shard width must not be negative", m.Set)
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
			return fmt.Errorf("set %q: unknown key scheme %q (content or offset)", m.Set, m.KeyScheme)
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

// maxBucketIndex bounds a declared histogram bucket position. A bucket set
// wider than this is a mistake or an attack, not a histogram; the index is
// used as an allocation size, so it may not be taken on trust.
const maxBucketIndex = 4096

// applyFieldMeta merges declared metadata. Last writer wins, but a
// disagreement between two ingesters is recorded and surfaced rather than
// resolved silently.
func (s *Store) applyFieldMeta(metas []wire.FieldMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range metas {
		if m.Set == "" || m.Field == "" {
			continue
		}
		e := s.entryLocked(m.Set)
		f, ok := e.Fields[m.Field]
		if !ok {
			f = &fieldEntry{}
			e.Fields[m.Field] = f
		}
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
		if m.MaxIntervalS > 0 {
			f.MaxInterval = int64(m.MaxIntervalS) * 1000
		}
		if m.LimitMin != nil {
			f.LimitMin = m.LimitMin
		}
		if m.LimitMax != nil {
			f.LimitMax = m.LimitMax
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
			bs := e.BucketSets[m.BucketSet]
			for len(bs.Buckets) <= m.BucketIndex {
				bs.Buckets = append(bs.Buckets, "")
				bs.Edges = append(bs.Edges, 0)
			}
			bs.Buckets[m.BucketIndex] = m.Field
			bs.Edges[m.BucketIndex] = m.BucketEdge
			if m.Unit != "" {
				bs.Unit = m.Unit
			}
			e.BucketSets[m.BucketSet] = bs
		}
	}
	s.catVer.Add(1)
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
	defer s.retentionMu.Unlock()
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

// hasAnyRetention reports whether anything at all is subject to retention,
// which is what decides whether the sweep loop is worth running.
func (s *Store) hasAnyRetention() bool {
	if s.cfg.Retention > 0 {
		return true
	}
	for _, d := range s.cfg.SetRetention {
		if d > 0 {
			return true
		}
	}
	s.retentionMu.RLock()
	defer s.retentionMu.RUnlock()
	for _, d := range s.setRetention {
		if d > 0 {
			return true
		}
	}
	return false
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
