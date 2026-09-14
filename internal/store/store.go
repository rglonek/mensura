// Package store is the Mensura store: the engine plus the write path, the
// catalogue, the label dictionary, time-shard routing, retention and the
// query engine. See docs/design/05-storage.md and docs/design/06-query.md.
package store

import (
	"context"
	"encoding/json"
	"errors"
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

	// Engine tuning, the `db:` block of the config file
	// (docs/design/09-operations.md section 1.1). Zero means "leave the
	// engine default"; CacheBytes accepts engine.NoBlockCache to disable
	// the block cache outright. These used to be parsed and dropped, so
	// an operator who sized the store for a small host got the 1 GiB
	// default anyway, with nothing saying so.
	CacheBytes               int64
	MemTableSizeBytes        uint64
	MaxConcurrentCompactions int
	Compression              string

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

	started   time.Time
	stopCh    chan struct{}
	stopOnce  sync.Once
	closeOnce sync.Once
	closed    atomic.Bool
	wg        sync.WaitGroup
}

// ErrClosed is what a query that arrives while the store is shutting down
// is answered with. It is not an internal fault: the caller should come
// back to whatever takes this store's place.
var ErrClosed = errors.New("store: the store is shutting down")

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

// staleAfter is how long a field may go unseen before a query that reads
// it is worth a warning.
const staleAfter = 7 * 24 * time.Hour

// stale reports whether the field has not been written recently. A field
// that was never observed has no age, so it is never stale.
func (f fieldEntry) stale() bool {
	return f.LastSeenMs > 0 && time.Since(time.UnixMilli(f.LastSeenMs)) > staleAfter
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
	// live is how many positions actually hold a value. It is not
	// len(Entries): a lost record leaves a hole, and counting holes
	// against MaxLabelCardinality refused writes on a key that was
	// under its budget -- while takeHole below exists precisely so
	// those positions are reused rather than paid for twice.
	live int
	// holes lists free positions left by lost records, newest last.
	//
	// It is a list rather than a scan because interning used to walk the
	// whole entry slice looking for a gap on every new value, under the
	// exclusive dictionary lock and on the write path: quadratic in
	// cardinality, measured at 4x per doubling, which at the default
	// 100k-value limit is seconds of lock-held CPU per label key with
	// every write serialised behind it.
	holes []int32
	// alias lists the *extra* positions a value occupies, beyond the one
	// the index map names. It is normally nil: intern never stores a
	// value twice, so a duplicate can only come off disk, where an older
	// build repaired a hole by re-interning a value it had already
	// placed. It exists because index is a one-to-one map and cannot say
	// so, and a lowering that resolved `host = "a"` to a single position
	// then silently skipped every row stored under the other one -- while
	// `host =~ /^a$/`, which walks the entries rather than the map, found
	// both. Two spellings of one predicate must not return different
	// rows.
	alias map[string][]int32
}

// rebuildIndex records the free positions in a freshly loaded dictionary,
// the count of positions that are not holes, and any value that occupies
// more than one position.
func (d *dictionary) rebuildIndex() {
	d.holes = nil
	d.alias = nil
	d.live = 0
	for i, e := range d.Entries {
		if e == "" {
			d.holes = append(d.holes, int32(i))
			continue
		}
		d.live++
		if at, ok := d.index[e]; ok && at != int32(i) {
			if d.alias == nil {
				d.alias = map[string][]int32{}
			}
			d.alias[e] = append(d.alias[e], int32(i))
		}
	}
}

// positions lists every index a value is stored under, most-recently
// interned first. A miss reports false so the query layer can turn it
// into an empty result plus a warning rather than dropping the clause.
func (d *dictionary) positions(value string) ([]int32, bool) {
	idx, hit := d.index[value]
	if !hit {
		return nil, false
	}
	extra := d.alias[value]
	if len(extra) == 0 {
		return []int32{idx}, true
	}
	out := make([]int32, 0, len(extra)+1)
	return append(append(out, idx), extra...), true
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
	switch cfg.StorageProfile {
	case "", "local", "network-fs":
	default:
		return nil, fmt.Errorf("store: unknown storage profile %q (local or network-fs)", cfg.StorageProfile)
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
	// Explicit tuning is applied last, so it wins over the profile's
	// choices rather than being quietly overridden by them.
	if cfg.CacheBytes != 0 {
		opts.CacheBytes = cfg.CacheBytes
	}
	if cfg.MemTableSizeBytes != 0 {
		opts.MemTableSizeBytes = cfg.MemTableSizeBytes
	}
	if cfg.MaxConcurrentCompactions != 0 {
		opts.MaxConcurrentCompactions = cfg.MaxConcurrentCompactions
	}
	if cfg.Compression != "" {
		// Validated rather than passed through: the engine falls back to
		// snappy for a name it does not know, so a typo would select a
		// codec the operator did not ask for and say nothing.
		switch strings.ToLower(cfg.Compression) {
		case "none", "snappy", "zstd", "balanced", "default":
			opts.Compression = cfg.Compression
		default:
			return nil, fmt.Errorf("store: unknown compression %q (none, snappy, zstd or balanced)", cfg.Compression)
		}
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

// Close stops the background sweep, persists the catalogue and closes the
// engine. It is safe to call twice: runWithEngine defers it while an
// error path may already have taken it, and a second saveCatalogue plus a
// second db.Close is at best wasted work and at worst a write against a
// closed engine.
func (s *Store) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.stopOnce.Do(func() { close(s.stopCh) })
		s.wg.Wait()
		s.drainJobs()
		if serr := s.saveCatalogue(); serr != nil {
			s.cfg.Logger.Printf("ERROR saving catalogue on close: %v", serr)
		}
		err = s.db.Close()
	})
	return err
}

// closeJobDrain bounds how long Close waits for the queries that are still
// running. A query that will not finish must not stop a store from
// shutting down.
const closeJobDrain = 30 * time.Second

// drainJobs waits for every executing query before the engine is torn
// down.
//
// Closing underneath one is not untidy, it is unsound: a query holds a
// pebble iterator over a snapshot for the whole of its scan, and Close
// releases the sstable readers and the cached blocks that iterator is
// reading out of. Pebble's own contract is that every iterator is closed
// first.
//
// Nothing else guaranteed it. In server mode the HTTP listeners are shut
// down first, but http.Server.Shutdown gives up after its own timeout and
// returns while a long scan is still running. In plugin mode Grafana's
// queries do not go through an http.Server at all: ServeLocal keeps
// answering until the process exits, so a SIGTERM during a panel refresh
// closed the engine under a live iterator every time.
//
// Every query path takes a job slot for the whole of its scan, so taking
// all of them is what "no query is running" means here. The slots are
// never given back: Close is one-way, and Query refuses outright once the
// closed flag is set, so nothing is waiting on them.
func (s *Store) drainJobs() {
	deadline := time.After(closeJobDrain)
	for i := 0; i < cap(s.jobs); i++ {
		select {
		case s.jobs <- struct{}{}:
		case <-deadline:
			s.cfg.Logger.Printf("WARNING closing the engine with %d quer(ies) still running after %s", cap(s.jobs)-i, closeJobDrain)
			return
		}
	}
}

func (s *Store) DB() *engine.DB        { return s.db }
func (s *Store) Config() Config        { return s.cfg }
func (s *Store) Uptime() time.Duration { return time.Since(s.started) }

// CatalogueETag is the cache validator for GET /v1/catalogue, and it is
// deliberately not CatalogueVersion alone.
//
// The rendered catalogue carries a Stale flag per field, and staleness is
// a function of wall-clock time rather than of anything a write moves. So
// the body changed while the version stood still, and a client caching on
// the ETag was answered 304 for the rest of the process's life: it never
// saw a field go quiet, and the W203 warning that says so never reached
// it. Folding the count of stale fields into the validator makes a field
// crossing the threshold change the tag, which is the one event that
// changes the body without changing the version.
func (s *Store) CatalogueETag() string {
	return catalogueETag(s.catVer.Load(), s.staleFieldCount())
}

// staleFieldCount counts the fields the catalogue would report as stale
// right now. It is one pass over the catalogue's own maps, which is a
// small fraction of what rendering the catalogue costs.
func (s *Store) staleFieldCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, e := range s.catalogue {
		for _, f := range e.Fields {
			if f.stale() {
				n++
			}
		}
	}
	return n
}

func catalogueETag(version int64, stale int) string {
	return fmt.Sprintf(`"v%d-s%d"`, version, stale)
}

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
			// An empty entry is a hole left by a lost record, not a
			// value. Indexing it made lookup(key, "") answer with the
			// hole's position, so a query lowering label = "" produced
			// Eq(label, <hole>) instead of the constant-false plus W201
			// that an unknown value gets. Nothing may intern one --
			// ValidateLabelValue refuses it on the write side -- so the
			// entry can only ever be a hole.
			if e == "" {
				continue
			}
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
		if v == "" {
			continue
		}
		if _, dup := d.index[v]; !dup {
			d.index[v] = idx
		}
	}
	for _, d := range s.dict {
		d.rebuildIndex()
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
	if s.cfg.MaxLabelCardinality > 0 && d.live >= s.cfg.MaxLabelCardinality {
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
		// The hole goes back on the free list. Dropping it on this path
		// lost the position for the life of the process: the entry stays
		// empty, so nothing can use it, and it is no longer listed, so
		// nothing will look at it again -- a slow leak of a budget this
		// function exists to defend.
		if reused {
			d.holes = append(d.holes, idx)
		}
		return 0, err
	}
	if int(idx) < len(d.Entries) {
		d.Entries[idx] = value
	} else {
		d.Entries = append(d.Entries, value)
	}
	d.live++
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

// lookupPositions resolves a label value to every index it is stored
// under. Normally that is exactly one; a value an older build placed at a
// second position has two, and a comparison that only ever named the
// first silently excluded every row written under the other.
func (s *Store) lookupPositions(key, value string) ([]model.Value, bool) {
	s.dictMu.RLock()
	defer s.dictMu.RUnlock()
	d, ok := s.dict[key]
	if !ok {
		return nil, false
	}
	idxs, hit := d.positions(value)
	if !hit {
		return nil, false
	}
	out := make([]model.Value, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, model.Int(int64(i)))
	}
	return out, true
}

// labelValue resolves a stored dictionary index back to its string. A hole
// is not a value: it is a position whose record was lost, and every other
// reader of the dictionary already skips one. Reporting it as the empty
// string instead meant `LABELS host WHERE …` -- the filtered form, which
// reads indices off rows -- listed an empty value among the hosts, so a
// dashboard variable grew a blank option that the unfiltered `LABELS host`
// never showed. Nothing may intern "" (ValidateLabelValue refuses it), so
// an empty entry can only ever be a hole.
func (s *Store) labelValue(key string, idx int32) (string, bool) {
	s.dictMu.RLock()
	defer s.dictMu.RUnlock()
	d, ok := s.dict[key]
	if !ok || idx < 0 || int(idx) >= len(d.Entries) {
		return "", false
	}
	if d.Entries[idx] == "" {
		return "", false
	}
	return d.Entries[idx], true
}

// labelIndicesMatching resolves every value of a label key that satisfies
// a predicate to its dictionary index, and reports how many values the key
// holds in all -- which is what tells a regex clause it matched everything
// and can be folded to an existence test.
//
// One pass under one lock. The regex path used to copy and sort the whole
// value list and then take the dictionary lock again for each match, so a
// key at the default 100k-value budget cost a hundred thousand lock
// acquisitions to lower one clause. Reading the index off the entry
// position also keeps a value that appears at two positions -- a hole
// repaired by an older build -- matching at both, which the reverse map
// cannot express.
func (s *Store) labelIndicesMatching(key string, keep func(string) bool) ([]model.Value, int) {
	s.dictMu.RLock()
	defer s.dictMu.RUnlock()
	d, ok := s.dict[key]
	if !ok {
		return nil, 0
	}
	var vals []model.Value
	total := 0
	for i, e := range d.Entries {
		if e == "" {
			continue // a hole left by a lost record, not a value
		}
		total++
		if keep(e) {
			vals = append(vals, model.Int(int64(i)))
		}
	}
	return vals, total
}

// LabelValues lists the known values of a label key.
func (s *Store) LabelValues(key string) []string {
	s.dictMu.RLock()
	defer s.dictMu.RUnlock()
	d, ok := s.dict[key]
	if !ok {
		// An empty list, never nil: this travels straight into
		// wire.LabelValues.Values, whose JSON tag carries no omitempty,
		// so a key the store has never seen answered `"values": null`
		// while a key it has answered `[]`. Every other list in this API
		// is a list -- QueryResponse.Series was changed for the same
		// reason -- and a dashboard variable should not have to
		// special-case "no such key" differently from "no values".
		return []string{}
	}
	// Deduplicated: a value an older build placed at two positions is one
	// value, and listing it twice put a repeated option in every
	// dashboard variable built on this key.
	out := make([]string, 0, len(d.Entries))
	seen := make(map[string]struct{}, len(d.Entries))
	for _, e := range d.Entries {
		if e == "" {
			continue
		}
		if _, dup := seen[e]; dup {
			continue
		}
		seen[e] = struct{}{}
		out = append(out, e)
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
		day := floorDiv(t.Unix(), 86400)
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

// floorDiv divides towards negative infinity. Go's / truncates towards
// zero, which put a pre-epoch timestamp in the day *after* the one it
// belongs to, while the mod below was carefully written not to. Half a
// calculation handling negatives is worse than neither half doing so.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
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
// range, oldest first, and reports whether any two of them can hold a row
// at the same instant.
func (s *Store) shardsFor(set string, fromMs, toMs int64) ([]string, bool) {
	return shardsInRange(set, s.db.Sets(), fromMs, toMs)
}

// shardsInRange is shardsFor over a shard list the caller already has.
//
// It exists because shardsFor walks and sorts every set name in the store
// on each call, so a loop that asked for one logical set at a time was
// quadratic in the shard count -- which hourly sharding makes large. The
// catalogue was moved off that shape onto shardsByLogical; the LABELS
// filter scan, which walks every set carrying a label on every dashboard
// variable refresh, was left on it.
//
// The second return value is what tells a bounded walk whether the scan
// order means anything. Shards are normally disjoint, so reading them
// oldest-first (or newest-first) visits rows in time order and a query
// with a LIMIT may stop as soon as it has enough. Two things break that:
// the unsharded "@all" shard, which covers every instant, and a set whose
// shard width was changed, which leaves a wide shard straddling narrow
// ones. Both happen through ordinary configuration changes, and both used
// to be invisible -- see runTabular.
func shardsInRange(set string, names []string, fromMs, toMs int64) ([]string, bool) {
	prefix := set + "@"
	type shard struct {
		name       string
		start, end int64
	}
	var found []shard
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := name[len(prefix):]
		if suffix == shardAll {
			// The unsharded shard covers everything, so it sorts first.
			found = append(found, shard{name, math.MinInt64, math.MaxInt64})
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
		found = append(found, shard{name, start.UnixMilli(), end.UnixMilli()})
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
	overlapping, covered := false, int64(math.MinInt64)
	for i, f := range found {
		if i > 0 && f.start < covered {
			overlapping = true
		}
		if f.end > covered {
			covered = f.end
		}
		out = append(out, f.name)
	}
	return out, overlapping
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
	dropped, forgotten := 0, 0
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
		if live, _ := s.shardsFor(logical, math.MinInt64, math.MaxInt64); len(live) > 0 {
			continue
		}
		// forgetAgedSet, not ForgetSet: what the shards took with them
		// is the *observed* schema, not the declaration a spec made
		// about the set.
		//
		// A declaration travels once per ingest process --
		// Sink.DeclareSets marks it sent and never repeats it -- so an
		// ingester that is still running when its set ages out will
		// never say it again. Dropping the declaration here therefore
		// dropped it for good: the next record re-created the set with
		// the store's own defaults, which under the documented
		// "--retention 0 globally, per-set retention from the spec"
		// posture means no retention at all, so it was routed to the
		// unsharded shard the sweep skips and then kept forever. Worse,
		// the key scheme went with it: a set declared `key: offset`
		// silently reverted to content keying, so two records sharing a
		// millisecond and a label set collapsed into one row and the
		// store's own "this set needs a key_hint" refusal stopped
		// firing. Both are silent, and both outlive the process that
		// could have corrected them.
		if s.forgetAgedSet(logical) {
			forgotten++
		}
	}
	// Persisted now rather than on the next 30-second tick, which is what
	// the admin drop already does and for the same reason: a crash in
	// that window brings the entry back, advertising fields and a time
	// range whose shards have just been range-deleted. Logged rather than
	// returned, so a failure to persist does not read as a failed sweep --
	// the shards really are gone.
	if forgotten > 0 {
		if err := s.saveCatalogue(); err != nil {
			s.cfg.Logger.Printf("ERROR saving catalogue after retention emptied %d set(s): %v", forgotten, err)
		}
	}
	return dropped, nil
}

// ForgetSet removes every trace of a logical set from the catalogue: its
// entry and the spec-supplied retention and shard overrides that were
// recorded alongside it.
//
// The overrides used to be left behind by an admin drop, so a set of the
// same name created afterwards silently inherited the policy of the one
// that was deleted -- until a restart, which loaded neither, and then it
// silently did not.
func (s *Store) ForgetSet(name string) {
	s.mu.Lock()
	delete(s.catalogue, name)
	s.mu.Unlock()
	s.retentionMu.Lock()
	delete(s.setRetention, name)
	delete(s.setShard, name)
	s.retentionMu.Unlock()
	s.catVer.Add(1)
}

// forgetAgedSet drops what a set's shards took with them -- the fields,
// the labels, the bucket sets and the observed time range -- while keeping
// what a spec declared about it. It reports whether anything changed.
//
// The split matters because the two halves are rediscovered on very
// different schedules. The observed schema comes back with the next
// sample: observeSet re-learns it from the samples themselves. The
// declaration does not, because it is sent once per ingest process and an
// ingester that is already running never repeats it. A set with nothing
// declared has nothing to keep, so it is removed outright, which is what
// this sweep has always done.
func (s *Store) forgetAgedSet(name string) bool {
	s.mu.Lock()
	e, ok := s.catalogue[name]
	if !ok {
		s.mu.Unlock()
		return false
	}
	if e.RetentionMs == nil && e.ShardMs == nil && e.KeyScheme == "" {
		delete(s.catalogue, name)
		s.mu.Unlock()
		s.retentionMu.Lock()
		delete(s.setRetention, name)
		delete(s.setShard, name)
		s.retentionMu.Unlock()
		s.catVer.Add(1)
		return true
	}
	changed := len(e.Fields) > 0 || len(e.Labels) > 0 || len(e.BucketSets) > 0 ||
		e.FirstTSMs != 0 || e.LastTSMs != 0
	e.Fields = map[string]*fieldEntry{}
	e.Labels = map[string]struct{}{}
	e.BucketSets = nil
	e.FirstTSMs, e.LastTSMs = 0, 0
	s.mu.Unlock()
	if changed {
		s.catVer.Add(1)
	}
	return changed
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
		Stale: f.stale(),
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
	// Copied under the lock for the same reason Catalogue copies: the
	// validator reads this after the lock is released, while a write
	// carrying bucket metadata may be rewriting the very same slice.
	return append([]string(nil), bs.Buckets...), ok
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
	// Copied, never aliased: the caller marshals this after the lock is
	// released, and the write path mutates the very same slice and maps
	// under the write lock. A map read racing a map write is not a data
	// race the runtime tolerates -- it is a fatal error that takes the
	// process down, and in plugin mode that process owns the data
	// directory.
	out := wire.Catalogue{
		Version:   s.catVer.Load(),
		Conflicts: append([]wire.CatalogueConflict(nil), s.conflicts...),
	}
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
				// Computed here as well as in Schema.Field, because this
				// is the form that travels over the wire: a proxy-mode
				// plugin validates against the catalogue it fetched, so
				// leaving it out meant the same query answered W203
				// "field has not been seen recently" under mode: plugin
				// and nothing at all under mode: proxy.
				Stale: fe.stale(),
			}
		}
		for l := range e.Labels {
			info.Labels = append(info.Labels, l)
		}
		sort.Strings(info.Labels)
		if len(e.BucketSets) > 0 {
			info.BucketSets = copyBucketSets(e.BucketSets)
		}
		out.Sets = append(out.Sets, info)
	}
	sort.Slice(out.Sets, func(i, j int) bool { return out.Sets[i].Name < out.Sets[j].Name })
	return out
}

// copyBucketSets deep-copies a set's bucket-set map, including the slices
// inside it: applyFieldMeta writes into both.
func copyBucketSets(in map[string]wire.BucketSetInfo) map[string]wire.BucketSetInfo {
	out := make(map[string]wire.BucketSetInfo, len(in))
	for name, bs := range in {
		out[name] = wire.BucketSetInfo{
			Buckets: append([]string(nil), bs.Buckets...),
			Edges:   append([]float64(nil), bs.Edges...),
			Unit:    bs.Unit,
		}
	}
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
