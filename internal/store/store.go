// Package store is the Mensura store: the engine plus the write path, the
// catalogue, the label dictionary, time-shard routing, retention and the
// query engine. See docs/design/05-storage.md and docs/design/06-query.md.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
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

	// MaxConcurrentJobs bounds how many queries may execute at once. A
	// query buffers its series in memory, so an unbounded number of them
	// is an unbounded memory footprint.
	MaxConcurrentJobs int

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

	// Spec-supplied retention and shard overrides. They are separate from
	// cfg because cfg is read without a lock on the write path.
	retentionMu  sync.RWMutex
	setRetention map[string]time.Duration
	setShard     map[string]time.Duration

	idem *idempotencyCache

	// jobs bounds concurrent query execution.
	jobs chan struct{}

	started  time.Time
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
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

	// RetentionMs and ShardMs are what a spec declared for this set over
	// the write API. They are persisted because the declaration travels
	// once per ingest process, not per write: an ingester that is already
	// running never repeats it, so a store that forgot the declaration on
	// restart kept the set forever and routed it to the unsharded shard
	// that retention can never drop. A nil pointer means "never declared".
	RetentionMs *int64 `json:"retention_ms,omitempty"`
	ShardMs     *int64 `json:"shard_ms,omitempty"`
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
//
// The map is global per label key, not per set: 05-storage.md section 8
// specifies one dictionary per key for the whole store. So a label key's
// cardinality budget is shared across sets, and LabelValues reports every
// value of that key seen anywhere. `LABELS <key> WHERE …` is the
// set-scoped form, and it scans rather than reading the dictionary.
// 12-implementation.md section 6.0 records the consequences; changing it
// would reinterpret every index already on disk.
type dictionary struct {
	Entries []string `json:"entries"`
	index   map[string]int32
	// holes lists free positions left by lost records, newest last.
	//
	// It is a list rather than a scan because interning used to walk the
	// whole entry slice looking for a gap on every new value, under the
	// exclusive dictionary lock and on the write path: quadratic in
	// cardinality, measured at 4x per doubling, which at the default
	// 100k-value limit is seconds of lock-held CPU per label key with
	// every write serialised behind it.
	holes []int32
}

// rebuildHoles records the free positions in a freshly loaded dictionary.
func (d *dictionary) rebuildHoles() {
	d.holes = nil
	for i, e := range d.Entries {
		if e == "" {
			d.holes = append(d.holes, int32(i))
		}
	}
}

// takeHole pops a free position, skipping any that has been filled since
// the list was built.
func (d *dictionary) takeHole() (int32, bool) {
	for len(d.holes) > 0 {
		h := d.holes[len(d.holes)-1]
		d.holes = d.holes[:len(d.holes)-1]
		if int(h) < len(d.Entries) && d.Entries[h] == "" {
			return h, true
		}
	}
	return 0, false
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
		cfg:          cfg,
		db:           db,
		catalogue:    map[string]*setEntry{},
		dict:         map[string]*dictionary{},
		setRetention: map[string]time.Duration{},
		setShard:     map[string]time.Duration{},
		idem:         newIdempotencyCache(4096),
		jobs:         make(chan struct{}, cfg.MaxConcurrentJobs),
		started:      time.Now(),
		stopCh:       make(chan struct{}),
	}
	if err := s.loadCatalogue(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.loadDictionaries(); err != nil {
		_ = db.Close()
		return nil, err
	}
	s.warnInexactShardWidths()
	// The sweep is started whenever it is configured, not only when
	// something is retained right now. Retention also arrives later, from
	// a spec's `sets:` block over the write API (applySetMeta), and that
	// is the documented way to run "--retention 0" globally with per-set
	// retention declared by the ingester. Deciding once at startup meant
	// such a set was sharded as if it would be swept and then kept
	// forever, with no error and no log line. A sweep over nothing costs
	// one pass across the set names per interval.
	if cfg.RetentionSweep > 0 {
		s.wg.Add(1)
		go s.retentionLoop()
	}
	return s, nil
}

// warnInexactShardWidths says so when a configured shard width is not one
// the suffix encoding can express, rather than rounding in silence.
func (s *Store) warnInexactShardWidths() {
	report := func(what string, w time.Duration) {
		if w <= 0 {
			return
		}
		if hours, exact := normaliseShardWidth(w); !exact {
			s.cfg.Logger.Printf("WARNING %s shard width %s is not a whole number of days or an hour count dividing a day; using %dh",
				what, w, hours)
		}
	}
	report("default", s.cfg.Shard)
	for name, w := range s.cfg.SetShard {
		report("set "+name, w)
	}
}

func (s *Store) Close() error {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
	if err := s.saveCatalogue(); err != nil {
		s.cfg.Logger.Printf("ERROR saving catalogue on close: %v", err)
	}
	return s.db.Close()
}

func (s *Store) DB() *engine.DB        { return s.db }
func (s *Store) Config() Config        { return s.cfg }
func (s *Store) Uptime() time.Duration { return time.Since(s.started) }

// CatalogueVersion is the version of the catalogue *schema*: which sets
// exist and what fields, labels and bucket sets they carry.
//
// It deliberately does not move when a set's first/last timestamp moves.
// It used to, which meant it changed on every single write batch, so the
// ETag on /v1/catalogue never matched and every client watching this
// number for metadata changes was woken continuously. The first/last
// timestamps are still reported live in the catalogue body; they are
// advisory hints for the query builder, not schema.
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
		// Spec-supplied retention and shard width are restored into the
		// live maps, so a restart keeps enforcing what the ingester
		// declared rather than silently reverting to the defaults.
		if e.RetentionMs != nil {
			s.setRetention[name] = time.Duration(*e.RetentionMs) * time.Millisecond
		}
		if e.ShardMs != nil && *e.ShardMs > 0 {
			s.setShard[name] = time.Duration(*e.ShardMs) * time.Millisecond
		}
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

// dictEntryPrefix marks a single interned value. One record per value, so
// adding the n-th value costs one small write rather than a rewrite of the
// whole array; DictKeys returns them sorted, and the zero-padded index in
// the key makes that sort the insertion order.
const dictEntryPrefix = "labelv:"

// dictPackedPrefix is the original whole-array form. It is still read so a
// data directory written by an earlier build opens, but never written.
const dictPackedPrefix = "label:"

func dictEntryKey(label string, idx int32) string {
	return fmt.Sprintf("%s%s\x00%010d", dictEntryPrefix, label, idx)
}

// splitDictEntryKey recovers the label key and the index from a per-value
// record name.
//
// The index is parsed rather than re-derived from load order: the index is
// what every stored row carries, so a missing or duplicated record must
// leave a hole, not shift every value after it onto someone else's index
// and silently mislabel the data.
func splitDictEntryKey(k string) (string, int32, bool) {
	rest, ok := strings.CutPrefix(k, dictEntryPrefix)
	if !ok {
		return "", 0, false
	}
	label, idxText, ok := strings.Cut(rest, "\x00")
	if !ok {
		return "", 0, false
	}
	idx, err := strconv.ParseInt(idxText, 10, 32)
	if err != nil || idx < 0 {
		return "", 0, false
	}
	return label, int32(idx), true
}

func (s *Store) loadDictionaries() error {
	keys, err := s.db.DictKeys()
	if err != nil {
		return err
	}
	get := func(label string) *dictionary {
		d, ok := s.dict[label]
		if !ok {
			d = &dictionary{index: map[string]int32{}}
			s.dict[label] = d
		}
		return d
	}
	// Legacy packed arrays first, so per-value records written afterwards
	// append to them rather than shadowing them.
	for _, k := range keys {
		if !strings.HasPrefix(k, dictPackedPrefix) {
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
		s.dict[strings.TrimPrefix(k, dictPackedPrefix)] = &d
	}
	for _, k := range keys {
		label, idx, ok := splitDictEntryKey(k)
		if !ok {
			continue
		}
		b, found, err := s.db.GetDict(k)
		if err != nil || !found {
			continue
		}
		d := get(label)
		v := string(b)
		if idx > maxDictIndex {
			return fmt.Errorf("store: label dictionary %s has an implausible index %d", k, idx)
		}
		// Grow to the recorded position, leaving holes empty rather than
		// closing them up.
		for int(idx) >= len(d.Entries) {
			d.Entries = append(d.Entries, "")
		}
		d.Entries[idx] = v
		if _, dup := d.index[v]; !dup {
			d.index[v] = idx
		}
	}
	for _, d := range s.dict {
		d.rebuildHoles()
	}
	return nil
}

// maxDictIndex bounds an index recovered from a record name, because it is
// used to size a slice.
const maxDictIndex = 1 << 24

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
	// A hole left by a lost record is reused rather than skipped, so the
	// dictionary does not grow past its budget on nothing. The free list
	// is built once at load and maintained as values are added, so this
	// costs the same whether the dictionary holds ten values or a hundred
	// thousand.
	idx, reused := d.takeHole()
	if !reused {
		idx = int32(len(d.Entries))
	}
	// One record for the new value only. Rewriting the whole array here
	// would make interning the n-th value cost O(n) bytes, so filling a
	// 100k-value dictionary would write ~5 GB under the dictionary lock.
	if err := s.db.PutDict(dictEntryKey(key, idx), []byte(value)); err != nil {
		return 0, err
	}
	if int(idx) < len(d.Entries) {
		d.Entries[idx] = value
	} else {
		d.Entries = append(d.Entries, value)
	}
	d.index[value] = idx
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
	out := make([]string, 0, len(d.Entries))
	for _, e := range d.Entries {
		if e != "" {
			out = append(out, e)
		}
	}
	sort.Strings(out)
	return out
}

// ---------- time shards ----------

const shardAll = "all"

// subDayShardHours are the sub-day widths the suffix encoding can express
// exactly: whole hours that divide a day, so a shard never straddles a day
// boundary.
var subDayShardHours = []int{1, 2, 3, 4, 6, 8, 12}

// normaliseShardWidth rounds a configured width down onto the nearest
// width the encoding can represent, and reports whether that was exact.
// Returning the width in whole hours keeps the boundary arithmetic in
// integers.
func normaliseShardWidth(w time.Duration) (hours int, exact bool) {
	if w >= 24*time.Hour {
		days := int(w / (24 * time.Hour))
		return days * 24, w == time.Duration(days)*24*time.Hour
	}
	want := int(w / time.Hour)
	best := 1
	for _, h := range subDayShardHours {
		if h <= want {
			best = h
		}
	}
	return best, time.Duration(best)*time.Hour == w
}

func (s *Store) shardWidth(set string) time.Duration {
	s.retentionMu.RLock()
	w, ok := s.setShard[set]
	s.retentionMu.RUnlock()
	if !ok {
		w, ok = s.cfg.SetShard[set]
	}
	if ok && w > 0 {
		return w
	}
	// Sharding exists so retention can be a range delete. A set that is
	// never dropped gains nothing from being split.
	if s.retentionFor(set) <= 0 {
		return 0
	}
	return s.cfg.Shard
}

// shardName routes a timestamp to its physical set. '@' is excluded from
// the set-name charset precisely so this suffix cannot collide with a
// caller-chosen name.
//
// The suffix carries its own width for anything but the two original
// granularities, because the width is what decides whether a shard
// overlaps a query range and when retention may drop it: deriving it from
// the suffix length would silently mis-size every other configured width.
func (s *Store) shardName(set string, tsMs int64) string {
	w := s.shardWidth(set)
	if w <= 0 {
		return set + "@" + shardAll
	}
	hours, _ := normaliseShardWidth(w)
	t := time.UnixMilli(tsMs).UTC()
	if hours%24 == 0 {
		days := hours / 24
		day := t.Unix() / 86400
		start := time.Unix((day-mod(day, int64(days)))*86400, 0).UTC()
		if days == 1 {
			return set + "@" + start.Format("20060102")
		}
		return set + "@" + start.Format("20060102") + "-" + strconv.Itoa(days) + "d"
	}
	start := time.Date(t.Year(), t.Month(), t.Day(), t.Hour()-t.Hour()%hours, 0, 0, 0, time.UTC)
	if hours == 1 {
		return set + "@" + start.Format("2006010215")
	}
	return set + "@" + start.Format("2006010215") + "-" + strconv.Itoa(hours) + "h"
}

// mod is a non-negative modulo, so a pre-epoch timestamp still floors onto
// a bucket boundary rather than jumping forward.
func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// shardsFor lists the physical shards of a logical set that overlap a time
// range, oldest first.
func (s *Store) shardsFor(set string, fromMs, toMs int64) []string {
	prefix := set + "@"
	type shard struct {
		name  string
		start int64
	}
	var found []shard
	for _, name := range s.db.Sets() {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := name[len(prefix):]
		if suffix == shardAll {
			// The unsharded shard covers everything, so it sorts first.
			found = append(found, shard{name, math.MinInt64})
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
		found = append(found, shard{name, start.UnixMilli()})
	}
	// Chronological, not lexicographic: "all" sorts after digits, so a set
	// that has both forms would otherwise be scanned out of time order.
	sort.Slice(found, func(i, j int) bool {
		if found[i].start != found[j].start {
			return found[i].start < found[j].start
		}
		return found[i].name < found[j].name
	})
	out := make([]string, 0, len(found))
	for _, f := range found {
		out = append(out, f.name)
	}
	return out
}

// parseShardSuffix recovers a shard's start and width. The bare forms are
// the original 1-hour and 1-day granularities; anything else carries an
// explicit "-<n>h" or "-<n>d" width.
func parseShardSuffix(suffix string) (time.Time, time.Duration, error) {
	stamp, unit, hasUnit := strings.Cut(suffix, "-")
	var (
		t     time.Time
		err   error
		width time.Duration
	)
	switch len(stamp) {
	case 8:
		t, err = time.ParseInLocation("20060102", stamp, time.UTC)
		width = 24 * time.Hour
	case 10:
		t, err = time.ParseInLocation("2006010215", stamp, time.UTC)
		width = time.Hour
	default:
		return time.Time{}, 0, fmt.Errorf("store: unrecognised shard suffix %q", suffix)
	}
	if err != nil {
		return time.Time{}, 0, err
	}
	if !hasUnit {
		return t, width, nil
	}
	if len(unit) < 2 {
		return time.Time{}, 0, fmt.Errorf("store: unrecognised shard width %q", suffix)
	}
	n, cerr := strconv.Atoi(unit[:len(unit)-1])
	if cerr != nil || n <= 0 {
		return time.Time{}, 0, fmt.Errorf("store: unrecognised shard width %q", suffix)
	}
	switch unit[len(unit)-1] {
	case 'h':
		return t, time.Duration(n) * time.Hour, nil
	case 'd':
		return t, time.Duration(n) * 24 * time.Hour, nil
	}
	return time.Time{}, 0, fmt.Errorf("store: unrecognised shard width %q", suffix)
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
	emptied := map[string]struct{}{}
	for _, name := range s.db.Sets() {
		i := strings.LastIndex(name, "@")
		if i <= 0 {
			continue
		}
		logical, suffix := name[:i], name[i+1:]
		if suffix == shardAll {
			continue
		}
		retention := s.retentionFor(logical)
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
			emptied[logical] = struct{}{}
		}
	}
	// A set whose every shard has aged out is gone; leaving its catalogue
	// entry behind would keep advertising fields and a time range that no
	// longer have any data under them.
	//
	// The label dictionary is deliberately not reclaimed with it. There is
	// one dictionary per label key for the whole store (05-storage.md
	// section 8, ADR-004), and every stored row holds an index into it, so
	// a value dropped because one set aged out would silently relabel the
	// rows of every other set that still carries it. The cost is that a
	// key's cardinality budget only ever grows; recovering it needs the
	// per-set dictionaries that ADR-004 rejected, not a sweep here.
	for logical := range emptied {
		if len(s.shardsFor(logical, math.MinInt64, math.MaxInt64)) > 0 {
			continue
		}
		s.mu.Lock()
		delete(s.catalogue, logical)
		s.mu.Unlock()
		s.catVer.Add(1)
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

// shardsByLogical groups every physical shard under its logical set in one
// pass. Catalogue used to call shardsFor per set, and shardsFor walks the
// whole set list, so rendering the catalogue was quadratic in the number
// of shards -- which retention at an hourly width makes large.
func (s *Store) shardsByLogical() map[string][]string {
	out := map[string][]string{}
	for _, name := range s.db.Sets() {
		i := strings.LastIndex(name, "@")
		if i <= 0 {
			continue
		}
		out[name[:i]] = append(out[name[:i]], name)
	}
	return out
}

// Catalogue renders the catalogue for the API and the query builder.
func (s *Store) Catalogue() wire.Catalogue {
	shards := s.shardsByLogical()
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := wire.Catalogue{Version: s.catVer.Load(), Conflicts: s.conflicts}
	for name, e := range s.catalogue {
		info := wire.SetInfo{
			Name:      name,
			Fields:    map[string]mql.FieldInfo{},
			FirstTSMs: e.FirstTSMs,
			LastTSMs:  e.LastTSMs,
			Shards:    shards[name],
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
