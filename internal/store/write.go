package store

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rglonek/mensura/internal/engine"
	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// idempotencyCache remembers recently committed request keys so a retry
// that crossed with its own success does not write twice.
type idempotencyCache struct {
	mu   sync.Mutex
	max  int
	ll   *list.List
	keys map[string]*list.Element
}

func newIdempotencyCache(max int) *idempotencyCache {
	return &idempotencyCache{max: max, ll: list.New(), keys: map[string]*list.Element{}}
}

func (c *idempotencyCache) seen(key string) bool {
	if key == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.keys[key]; ok {
		c.ll.MoveToFront(el)
		return true
	}
	el := c.ll.PushFront(key)
	c.keys[key] = el
	if c.ll.Len() > c.max {
		last := c.ll.Back()
		c.ll.Remove(last)
		delete(c.keys, last.Value.(string))
	}
	return false
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
			resp.Accepted++
		}
		for shard, recs := range byShard {
			if err := s.db.PutBatch(shard, recs); err != nil {
				return nil, err
			}
		}
		s.observeSet(batch.Set, batch.Samples)
	}
	resp.CatalogueVersion = s.catVer.Load()
	return resp, nil
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
func (s *Store) rowFor(set string, sm *model.Sample) (engine.Row, error) {
	row := make(engine.Row, len(sm.Labels)+len(sm.Fields)+1)
	row[model.TimestampField] = model.Int(sm.TSMs)
	for k, v := range sm.Labels {
		if err := model.ValidateLabelValue(v); err != nil {
			return nil, fmt.Errorf("label %q: %w", k, err)
		}
		idx, err := s.intern(k, v)
		if err != nil {
			return nil, err
		}
		row[k] = model.Int(int64(idx))
	}
	for k, v := range sm.Fields {
		if _, clash := sm.Labels[k]; clash {
			return nil, fmt.Errorf("%q is both a label and a field", k)
		}
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
	changed := false
	now := time.Now().UnixMilli()
	for i := range samples {
		sm := &samples[i]
		if e.FirstTSMs == 0 || sm.TSMs < e.FirstTSMs {
			e.FirstTSMs, changed = sm.TSMs, true
		}
		if sm.TSMs > e.LastTSMs {
			e.LastTSMs, changed = sm.TSMs, true
		}
		for k := range sm.Labels {
			if _, ok := e.Labels[k]; !ok {
				e.Labels[k] = struct{}{}
				changed = true
			}
		}
		for k := range sm.Fields {
			f, ok := e.Fields[k]
			if !ok {
				f = &fieldEntry{Kind: model.KindGauge}
				e.Fields[k] = f
				changed = true
			}
			f.LastSeenMs = now
		}
	}
	if changed {
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
func (s *Store) SetRetentionFor(set string, retention, shard time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.SetRetention == nil {
		s.cfg.SetRetention = map[string]time.Duration{}
	}
	if s.cfg.SetShard == nil {
		s.cfg.SetShard = map[string]time.Duration{}
	}
	if retention >= 0 {
		s.cfg.SetRetention[set] = retention
	}
	if shard > 0 {
		s.cfg.SetShard[set] = shard
	}
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
	sortStrings(out)
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && strings.Compare(s[j-1], s[j]) > 0; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
