// Package store is the Mensura store: the engine plus the write path, the
// catalogue, the label dictionary, time-shard routing, retention and the
// query engine. See docs/design/05-storage.md and docs/design/06-query.md.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rglonek/mensura/internal/engine"
	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// Config is the store's runtime configuration.
type Config struct {
	DataDir string

	// Durability selects the WAL/fsync posture: "batch" (neither, because
	// the source files are the truth), "stream" (WAL, group commit) or
	// "paranoid" (WAL plus fsync per batch).
	Durability string

	// StorageProfile is "local" or "network-fs"; the latter trades more
	// memory and larger files for far fewer metadata round trips.
	StorageProfile string

	// Retention defaults, overridable per set.
	Retention      time.Duration
	Shard          time.Duration
	SetRetention   map[string]time.Duration
	SetShard       map[string]time.Duration
	RetentionSweep time.Duration

	MaxSeriesPerGraph     int
	MaxDataPointsReceived int
	MaxLabelCardinality   int
	MaxConcurrentJobs     int

	Logger *log.Logger
}

// DefaultConfig returns the documented defaults.
func DefaultConfig() Config {
	return Config{
		Durability:            "stream",
		StorageProfile:        "local",
		Retention:             30 * 24 * time.Hour,
		Shard:                 24 * time.Hour,
		RetentionSweep:        time.Hour,
		MaxSeriesPerGraph:     1000,
		MaxDataPointsReceived: 34_560_000,
		MaxLabelCardinality:   100_000,
		MaxConcurrentJobs:     8,
		Logger:                log.New(os.Stderr, "mensura-store ", log.LstdFlags),
	}
}

// Store owns the engine and everything layered on it.
type Store struct {
	cfg Config
	db  *engine.DB

	mu        sync.RWMutex
	catalogue map[string]*setEntry
	catVer    atomic.Int64
	conflicts []wire.CatalogueConflict

	dictMu sync.RWMutex
	dict   map[string]*dictionary

	idem *idempotencyCache

	started time.Time
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// setEntry is the catalogue record for one logical set.
type setEntry struct {
	Name       string                        `json:"name"`
	Fields     map[string]*fieldEntry        `json:"fields"`
	Labels     map[string]struct{}           `json:"labels"`
	BucketSets map[string]wire.BucketSetInfo `json:"bucket_sets,omitempty"`
	KeyScheme  model.KeyScheme               `json:"key_scheme,omitempty"`
	FirstTSMs  int64                         `json:"first_ts_ms,omitempty"`
	LastTSMs   int64                         `json:"last_ts_ms,omitempty"`
}

type fieldEntry struct {
	Kind        model.Kind `json:"kind,omitempty"`
	Unit        string     `json:"unit,omitempty"`
	UnitHint    string     `json:"unit_hint,omitempty"`
	Description string     `json:"description,omitempty"`
	MaxInterval int64      `json:"max_interval_ms,omitempty"`
	LimitMin    *float64   `json:"limit_min,omitempty"`
	LimitMax    *float64   `json:"limit_max,omitempty"`
	BucketSet   string     `json:"bucket_set,omitempty"`
	BucketIndex int        `json:"bucket_index,omitempty"`
	BucketEdge  float64    `json:"bucket_edge,omitempty"`
	LastSeenMs  int64      `json:"last_seen_ms,omitempty"`
}

// dictionary is the string-to-index map for one label key. The store owns
// it because with many independent ingesters a single writer is the only
// way to keep indices consistent without a coordination protocol.
type dictionary struct {
	Entries []string `json:"entries"`
	index   map[string]int32
}

// Open starts a store on a data directory. Exactly one process may hold it.
func Open(cfg Config) (*Store, error) {
	if cfg.Logger == nil {
		cfg.Logger = DefaultConfig().Logger
	}
	if cfg.Shard <= 0 {
		cfg.Shard = 24 * time.Hour
	}
	if cfg.MaxConcurrentJobs <= 0 {
		cfg.MaxConcurrentJobs = 8
	}
	opts := engine.DefaultOptions()
	opts.Path = cfg.DataDir
	opts.Logger = cfg.Logger
	switch cfg.Durability {
	case "", "stream":
		opts.EnableWAL, opts.SyncWrites = true, false
	case "batch":
		opts.EnableWAL, opts.SyncWrites = false, false
	case "paranoid":
		opts.EnableWAL, opts.SyncWrites = true, true
	default:
		return nil, fmt.Errorf("store: unknown durability %q (batch, stream or paranoid)", cfg.Durability)
	}
	if cfg.StorageProfile == "network-fs" {
		// Every one of these exists because a network filesystem charges a
		// metadata round trip where a local disk charges nothing.
		opts.TargetFileSizeL0 = 64 << 20
		opts.BytesPerSync = engine.BytesPerSyncDisabled
		opts.LBaseMaxBytes = 512 << 20
		opts.L0StopWritesThreshold = 40
		opts.EnableBloomFilter = true
		opts.MaxConcurrentCompactions = 2
	}
	db, err := engine.Open(opts)
	if err != nil {
		return nil, err
	}
	s := &Store{
		cfg:       cfg,
		db:        db,
		catalogue: map[string]*setEntry{},
		dict:      map[string]*dictionary{},
		idem:      newIdempotencyCache(4096),
		started:   time.Now(),
		stopCh:    make(chan struct{}),
	}
	if err := s.loadCatalogue(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.loadDictionaries(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if cfg.RetentionSweep > 0 && cfg.Retention > 0 {
		s.wg.Add(1)
		go s.retentionLoop()
	}
	return s, nil
}

func (s *Store) Close() error {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	s.wg.Wait()
	if err := s.saveCatalogue(); err != nil {
		s.cfg.Logger.Printf("ERROR saving catalogue on close: %v", err)
	}
	return s.db.Close()
}

func (s *Store) DB() *engine.DB          { return s.db }
func (s *Store) Config() Config          { return s.cfg }
func (s *Store) Uptime() time.Duration   { return time.Since(s.started) }
func (s *Store) CatalogueVersion() int64 { return s.catVer.Load() }

// ---------- catalogue persistence ----------

const catalogueDictKey = "catalogue"

func (s *Store) loadCatalogue() error {
	b, ok, err := s.db.GetDict(catalogueDictKey)
	if err != nil || !ok {
		return err
	}
	var stored struct {
		Version   int64                    `json:"version"`
		Sets      map[string]*setEntry     `json:"sets"`
		Conflicts []wire.CatalogueConflict `json:"conflicts"`
	}
	if err := json.Unmarshal(b, &stored); err != nil {
		return fmt.Errorf("store: catalogue is unreadable: %w", err)
	}
	for name, e := range stored.Sets {
		if e.Fields == nil {
			e.Fields = map[string]*fieldEntry{}
		}
		if e.Labels == nil {
			e.Labels = map[string]struct{}{}
		}
		s.catalogue[name] = e
	}
	s.conflicts = stored.Conflicts
	s.catVer.Store(stored.Version)
	return nil
}

func (s *Store) saveCatalogue() error {
	s.mu.RLock()
	payload := struct {
		Version   int64                    `json:"version"`
		Sets      map[string]*setEntry     `json:"sets"`
		Conflicts []wire.CatalogueConflict `json:"conflicts"`
	}{Version: s.catVer.Load(), Sets: s.catalogue, Conflicts: s.conflicts}
	b, err := json.Marshal(payload)
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	return s.db.PutDict(catalogueDictKey, b)
}

// ---------- label dictionary ----------

func (s *Store) loadDictionaries() error {
	keys, err := s.db.DictKeys()
	if err != nil {
		return err
	}
	for _, k := range keys {
		if !strings.HasPrefix(k, "label:") {
			continue
		}
		b, ok, err := s.db.GetDict(k)
		if err != nil || !ok {
			continue
		}
		var d dictionary
		if err := json.Unmarshal(b, &d); err != nil {
			return fmt.Errorf("store: label dictionary %s is unreadable: %w", k, err)
		}
		d.index = make(map[string]int32, len(d.Entries))
		for i, e := range d.Entries {
			d.index[e] = int32(i)
		}
		s.dict[strings.TrimPrefix(k, "label:")] = &d
	}
	return nil
}

// intern maps a label value to its dictionary index, adding it if new.
// Returns an error when the key would exceed the cardinality limit:
// unbounded labels are the classic way to destroy a metrics system, so the
// failure is loud and early rather than silent and terminal.
func (s *Store) intern(key, value string) (int32, error) {
	s.dictMu.RLock()
	d, ok := s.dict[key]
	if ok {
		if idx, hit := d.index[value]; hit {
			s.dictMu.RUnlock()
			return idx, nil
		}
	}
	s.dictMu.RUnlock()

	s.dictMu.Lock()
	defer s.dictMu.Unlock()
	d, ok = s.dict[key]
	if !ok {
		d = &dictionary{index: map[string]int32{}}
		s.dict[key] = d
	}
	if idx, hit := d.index[value]; hit {
		return idx, nil
	}
	if s.cfg.MaxLabelCardinality > 0 && len(d.Entries) >= s.cfg.MaxLabelCardinality {
		return 0, fmt.Errorf("label %q exceeds the cardinality limit of %d distinct values", key, s.cfg.MaxLabelCardinality)
	}
	idx := int32(len(d.Entries))
	d.Entries = append(d.Entries, value)
	d.index[value] = idx
	b, err := json.Marshal(d)
	if err != nil {
		return 0, err
	}
	if err := s.db.PutDict("label:"+key, b); err != nil {
		return 0, err
	}
	return idx, nil
}

// lookup resolves a label value to its index without adding it. A miss is
// reported so the query layer can turn it into an empty result plus a
// warning, rather than dropping the clause and widening the query.
func (s *Store) lookup(key, value string) (int32, bool) {
	s.dictMu.RLock()
	defer s.dictMu.RUnlock()
	d, ok := s.dict[key]
	if !ok {
		return 0, false
	}
	idx, hit := d.index[value]
	return idx, hit
}

func (s *Store) labelValue(key string, idx int32) (string, bool) {
	s.dictMu.RLock()
	defer s.dictMu.RUnlock()
	d, ok := s.dict[key]
	if !ok || idx < 0 || int(idx) >= len(d.Entries) {
		return "", false
	}
	return d.Entries[idx], true
}

// LabelValues lists the known values of a label key.
func (s *Store) LabelValues(key string) []string {
	s.dictMu.RLock()
	defer s.dictMu.RUnlock()
	d, ok := s.dict[key]
	if !ok {
		return nil
	}
	out := append([]string(nil), d.Entries...)
	sort.Strings(out)
	return out
}

// ---------- time shards ----------

const shardAll = "all"

func (s *Store) shardWidth(set string) time.Duration {
	if w, ok := s.cfg.SetShard[set]; ok && w > 0 {
		return w
	}
	if s.cfg.Retention == 0 && len(s.cfg.SetRetention) == 0 {
		return 0
	}
	if r, ok := s.cfg.SetRetention[set]; ok && r == 0 {
		return 0
	}
	return s.cfg.Shard
}

// shardName routes a timestamp to its physical set. '@' is excluded from
// the set-name charset precisely so this suffix cannot collide with a
// caller-chosen name.
func (s *Store) shardName(set string, tsMs int64) string {
	w := s.shardWidth(set)
	if w <= 0 {
		return set + "@" + shardAll
	}
	t := time.UnixMilli(tsMs).UTC()
	if w >= 24*time.Hour {
		return set + "@" + t.Format("20060102")
	}
	return set + "@" + t.Format("2006010215")
}

// shardsFor lists the physical shards of a logical set that overlap a time
// range, oldest first.
func (s *Store) shardsFor(set string, fromMs, toMs int64) []string {
	prefix := set + "@"
	var out []string
	for _, name := range s.db.Sets() {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := name[len(prefix):]
		if suffix == shardAll {
			out = append(out, name)
			continue
		}
		start, width, err := parseShardSuffix(suffix)
		if err != nil {
			continue
		}
		end := start.Add(width)
		if end.UnixMilli() <= fromMs || start.UnixMilli() > toMs {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func parseShardSuffix(suffix string) (time.Time, time.Duration, error) {
	switch len(suffix) {
	case 8:
		t, err := time.ParseInLocation("20060102", suffix, time.UTC)
		return t, 24 * time.Hour, err
	case 10:
		t, err := time.ParseInLocation("2006010215", suffix, time.UTC)
		return t, time.Hour, err
	}
	return time.Time{}, 0, fmt.Errorf("store: unrecognised shard suffix %q", suffix)
}

// logicalSets lists the logical set names behind the physical shards.
func (s *Store) logicalSets() []string {
	seen := map[string]struct{}{}
	for _, name := range s.db.Sets() {
		if i := strings.LastIndex(name, "@"); i > 0 {
			seen[name[:i]] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// ---------- retention ----------

func (s *Store) retentionLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.RetentionSweep)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			if n, err := s.RunRetention(time.Now()); err != nil {
				s.cfg.Logger.Printf("ERROR retention sweep: %v", err)
			} else if n > 0 {
				s.cfg.Logger.Printf("retention dropped %d shard(s)", n)
			}
		}
	}
}

// RunRetention drops every shard that ended before its set's retention
// horizon. Dropping a whole shard is one range delete, not millions of
// point tombstones.
func (s *Store) RunRetention(now time.Time) (int, error) {
	dropped := 0
	for _, name := range s.db.Sets() {
		i := strings.LastIndex(name, "@")
		if i <= 0 {
			continue
		}
		logical, suffix := name[:i], name[i+1:]
		if suffix == shardAll {
			continue
		}
		retention := s.cfg.Retention
		if r, ok := s.cfg.SetRetention[logical]; ok {
			retention = r
		}
		if retention <= 0 {
			continue
		}
		start, width, err := parseShardSuffix(suffix)
		if err != nil {
			continue
		}
		if start.Add(width).Before(now.Add(-retention)) {
			if err := s.db.DropSet(name); err != nil {
				return dropped, err
			}
			dropped++
		}
	}
	return dropped, nil
}

// Compact runs a full-keyspace compaction; batch ingest calls this once at
// the end of an import.
func (s *Store) Compact() error { return s.db.Compact() }

// Stats returns engine statistics for the admin endpoint.
func (s *Store) Stats() engine.StatsSnapshot { return s.db.Snapshot() }

// ---------- catalogue reads (mql.Schema) ----------

// Schema adapts the catalogue to the MQL validator.
type Schema struct{ s *Store }

func (s *Store) Schema() mql.Schema { return Schema{s} }

func (sc Schema) HasSet(set string) bool {
	sc.s.mu.RLock()
	defer sc.s.mu.RUnlock()
	_, ok := sc.s.catalogue[set]
	return ok
}

func (sc Schema) Sets() []string { return sc.s.Sets() }

func (sc Schema) Field(set, field string) (mql.FieldInfo, bool) {
	sc.s.mu.RLock()
	defer sc.s.mu.RUnlock()
	e, ok := sc.s.catalogue[set]
	if !ok {
		return mql.FieldInfo{}, false
	}
	f, ok := e.Fields[field]
	if !ok {
		return mql.FieldInfo{}, false
	}
	return mql.FieldInfo{
		Kind: f.Kind, Unit: f.Unit, UnitHint: f.UnitHint, Description: f.Description,
		MaxInterval: f.MaxInterval, LimitMin: f.LimitMin, LimitMax: f.LimitMax,
		BucketSet: f.BucketSet, BucketIndex: f.BucketIndex, BucketEdge: f.BucketEdge,
		Stale: f.LastSeenMs > 0 && time.Since(time.UnixMilli(f.LastSeenMs)) > 7*24*time.Hour,
	}, true
}

func (sc Schema) HasLabel(set, key string) bool {
	sc.s.mu.RLock()
	defer sc.s.mu.RUnlock()
	e, ok := sc.s.catalogue[set]
	if !ok {
		return false
	}
	_, ok = e.Labels[key]
	return ok
}

func (sc Schema) BucketSet(set, name string) ([]string, bool) {
	sc.s.mu.RLock()
	defer sc.s.mu.RUnlock()
	e, ok := sc.s.catalogue[set]
	if !ok {
		return nil, false
	}
	bs, ok := e.BucketSets[name]
	return bs.Buckets, ok
}

// Sets lists the logical sets in the catalogue.
func (s *Store) Sets() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.catalogue))
	for n := range s.catalogue {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Catalogue renders the catalogue for the API and the query builder.
func (s *Store) Catalogue() wire.Catalogue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := wire.Catalogue{Version: s.catVer.Load(), Conflicts: s.conflicts}
	for name, e := range s.catalogue {
		info := wire.SetInfo{
			Name:      name,
			Fields:    map[string]mql.FieldInfo{},
			FirstTSMs: e.FirstTSMs,
			LastTSMs:  e.LastTSMs,
			Shards:    s.shardsFor(name, 0, 1<<62),
		}
		for f, fe := range e.Fields {
			info.Fields[f] = mql.FieldInfo{
				Kind: fe.Kind, Unit: fe.Unit, UnitHint: fe.UnitHint, Description: fe.Description,
				MaxInterval: fe.MaxInterval, LimitMin: fe.LimitMin, LimitMax: fe.LimitMax,
				BucketSet: fe.BucketSet, BucketIndex: fe.BucketIndex, BucketEdge: fe.BucketEdge,
			}
		}
		for l := range e.Labels {
			info.Labels = append(info.Labels, l)
		}
		sort.Strings(info.Labels)
		if len(e.BucketSets) > 0 {
			info.BucketSets = e.BucketSets
		}
		out.Sets = append(out.Sets, info)
	}
	sort.Slice(out.Sets, func(i, j int) bool { return out.Sets[i].Name < out.Sets[j].Name })
	return out
}

// Ping keeps a context-aware liveness check in one place.
func (s *Store) Ping(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
