package engine

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/rglonek/mensura/pkg/model"
)

// currentStorageVersion is the on-disk layout version. Opening a directory
// written by a different version fails rather than guessing; the recovery
// path is documented in docs/design/05-storage.md section 8.
const currentStorageVersion = 1

var (
	ErrStorageVersionMismatch = errors.New("engine: on-disk storage version does not match this build")
	ErrUnknownSet             = errors.New("engine: unknown set")
	ErrClosed                 = errors.New("engine: database is closed")
)

// ColumnSpec declares one column of a set. Columns may also appear
// implicitly on first write; the schema is the union of everything seen.
type ColumnSpec struct {
	Name    string          `json:"name"`
	Type    model.ValueType `json:"type"`
	Indexed bool            `json:"indexed"`
}

type setMeta struct {
	ID       uint32
	Name     string
	Columns  map[string]ColumnSpec
	Indexed  string // name of the indexed column, empty when unindexed
	IndexCol uint32 // column id of the indexed column within this set
}

// indexedColumnID is the column id given to a set's indexed column. It is
// a constant rather than a position in the column map, whose iteration
// order is random: the id lands in every index key, so deriving it from
// anything unstable would make the keyspace depend on map ordering.
const indexedColumnID uint32 = 1

// setRef is an immutable snapshot of the schema fields the read paths
// need. They take one under the lock and then work from the copy, because
// reading the live *setMeta after unlocking races with the first write
// that widens the schema.
type setRef struct {
	id       uint32
	indexed  string
	indexCol uint32
}

func (d *DB) setRef(name string) (setRef, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	sm, ok := d.sets[name]
	if !ok {
		return setRef{}, false
	}
	return setRef{id: sm.ID, indexed: sm.Indexed, indexCol: sm.IndexCol}, true
}

// DB is the store. It is safe for concurrent use; iterators are not safe
// to share across goroutines.
type DB struct {
	opts   Options
	pdb    *pebble.DB
	closed atomic.Bool

	mu     sync.RWMutex
	sets   map[string]*setMeta
	byID   map[uint32]*setMeta
	nextID uint32

	writeOpts *pebble.WriteOptions
	// metaOpts is used for schema and version records. It syncs only when
	// the WAL is enabled: Pebble rejects a sync write outright when the
	// WAL is off, and with no WAL there is nothing for a sync to flush.
	metaOpts *pebble.WriteOptions
	stats    Stats
}

// Stats are cheap counters plus the engine's own view of the LSM.
type Stats struct {
	Puts          atomic.Int64
	Gets          atomic.Int64
	Scans         atomic.Int64
	Queries       atomic.Int64
	RowsScanned   atomic.Int64
	OpenIterators atomic.Int64
}

// StatsSnapshot is the serialisable form of Stats plus LSM metrics.
type StatsSnapshot struct {
	Puts          int64  `json:"puts"`
	Gets          int64  `json:"gets"`
	Scans         int64  `json:"scans"`
	Queries       int64  `json:"queries"`
	RowsScanned   int64  `json:"rows_scanned"`
	OpenIterators int64  `json:"open_iterators"`
	DiskBytes     uint64 `json:"disk_bytes"`
	MemtableBytes uint64 `json:"memtable_bytes"`
	L0Sublevels   int32  `json:"l0_sublevels"`
	CacheHits     int64  `json:"block_cache_hits"`
	CacheMisses   int64  `json:"block_cache_misses"`
}

// Open opens (creating if needed) the store at opts.Path.
func Open(opts Options) (*DB, error) {
	opts.applyDefaults()
	if opts.Path == "" {
		return nil, errors.New("engine: Options.Path is required")
	}
	po := opts.pebbleOptions()
	pdb, err := pebble.Open(opts.Path, po)
	if po.Cache != nil {
		// Open has taken its own reference; release ours either way, or
		// the cache outlives every DB that ever used it.
		po.Cache.Unref()
	}
	if err != nil {
		return nil, fmt.Errorf("engine: open %s: %w", opts.Path, err)
	}
	d := &DB{
		opts:      opts,
		pdb:       pdb,
		sets:      map[string]*setMeta{},
		byID:      map[uint32]*setMeta{},
		writeOpts: pebble.NoSync,
	}
	if opts.EnableWAL && opts.SyncWrites {
		d.writeOpts = pebble.Sync
	}
	d.metaOpts = pebble.NoSync
	if opts.EnableWAL {
		d.metaOpts = pebble.Sync
	}
	if err := d.loadMeta(); err != nil {
		_ = pdb.Close()
		return nil, err
	}
	return d, nil
}

func (d *DB) loadMeta() error {
	verKey := metaKey("version")
	v, closer, err := d.pdb.Get(verKey)
	switch {
	case err == pebble.ErrNotFound:
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], currentStorageVersion)
		if err := d.pdb.Set(verKey, b[:], d.metaOpts); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		if len(v) < 4 {
			_ = closer.Close()
			return fmt.Errorf("%w: version record is %d bytes", ErrStorageVersionMismatch, len(v))
		}
		got := binary.BigEndian.Uint32(v)
		_ = closer.Close()
		if got != currentStorageVersion {
			return fmt.Errorf("%w: on disk %d, this build %d", ErrStorageVersionMismatch, got, currentStorageVersion)
		}
	}

	prefix := metaKey("set", "")
	it, err := d.pdb.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		var sm setMeta
		if err := json.Unmarshal(it.Value(), &sm); err != nil {
			return err
		}
		if sm.Columns == nil {
			sm.Columns = map[string]ColumnSpec{}
		}
		cp := sm
		d.sets[sm.Name] = &cp
		d.byID[sm.ID] = &cp
		if sm.ID >= d.nextID {
			d.nextID = sm.ID + 1
		}
	}
	return it.Error()
}

func (d *DB) persistSet(sm *setMeta) error {
	b, err := json.Marshal(sm)
	if err != nil {
		return err
	}
	return d.pdb.Set(metaKey("set", sm.Name), b, d.metaOpts)
}

// RegisterSet declares a set up front. Calling it again with more columns
// widens the schema; it never narrows one.
func (d *DB) RegisterSet(name string, cols []ColumnSpec) error {
	if d.closed.Load() {
		return ErrClosed
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.setLocked(name, cols)
	return err
}

func (d *DB) setLocked(name string, cols []ColumnSpec) (*setMeta, error) {
	sm, ok := d.sets[name]
	if !ok {
		sm = &setMeta{ID: d.nextID, Name: name, Columns: map[string]ColumnSpec{}}
		d.nextID++
		d.sets[name] = sm
		d.byID[sm.ID] = sm
	}
	changed := !ok
	for _, c := range cols {
		existing, had := sm.Columns[c.Name]
		if had && existing.Type == c.Type && existing.Indexed == c.Indexed {
			continue
		}
		if !had {
			sm.Columns[c.Name] = c
			changed = true
			if c.Indexed && sm.Indexed == "" {
				sm.Indexed = c.Name
				sm.IndexCol = indexedColumnID
			}
			continue
		}
		// A widening type change is recorded; a narrowing one is ignored so
		// that one odd row cannot retype a column underneath a dashboard.
		if existing.Type == model.TypeInt && c.Type == model.TypeFloat {
			existing.Type = model.TypeFloat
			sm.Columns[c.Name] = existing
			changed = true
		}
	}
	if changed {
		if err := d.persistSet(sm); err != nil {
			return nil, err
		}
	}
	return sm, nil
}

// Sets lists every registered set name, sorted.
func (d *DB) Sets() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]string, 0, len(d.sets))
	for n := range d.sets {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Schema returns the column specs of a set, sorted by name.
func (d *DB) Schema(name string) ([]ColumnSpec, string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	sm, ok := d.sets[name]
	if !ok {
		return nil, "", false
	}
	out := make([]ColumnSpec, 0, len(sm.Columns))
	for _, c := range sm.Columns {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, sm.Indexed, true
}

// Record is a row plus its primary key.
type Record struct {
	Key [16]byte
	Row Row
}

// PutBatch commits rows into one set. It assumes each key is new, or that
// an existing row at that key carries the same indexed value — which every
// Mensura key scheme guarantees by construction, because the timestamp is
// part of the key input. That assumption is what lets the write path skip
// a read before every write.
func (d *DB) PutBatch(set string, recs []Record) error {
	if d.closed.Load() {
		return ErrClosed
	}
	if len(recs) == 0 {
		return nil
	}
	d.mu.Lock()
	cols := make([]ColumnSpec, 0, 8)
	seen := map[string]struct{}{}
	for i := range recs {
		for name, v := range recs[i].Row {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			cols = append(cols, ColumnSpec{Name: name, Type: v.T, Indexed: name == model.TimestampField})
		}
	}
	sm, err := d.setLocked(set, cols)
	if err != nil {
		d.mu.Unlock()
		return err
	}
	setID, indexed, indexCol := sm.ID, sm.Indexed, sm.IndexCol
	d.mu.Unlock()

	b := d.pdb.NewBatch()
	defer b.Close()
	for i := range recs {
		payload := encodeRow(recs[i].Row)
		if indexed != "" {
			iv, ok := recs[i].Row[indexed]
			if !ok {
				// A row without the indexed column is invisible to indexed
				// range scans by design; store it under D/ so a full scan
				// can still see it.
				if err := b.Set(dataKey(setID, recs[i].Key), payload, nil); err != nil {
					return err
				}
				continue
			}
			ts, ok := iv.AsInt()
			if !ok {
				return fmt.Errorf("engine: indexed column %q is not numeric", indexed)
			}
			ik := indexKey(setID, indexCol, ts, recs[i].Key)
			if err := b.Set(ik, payload, nil); err != nil {
				return err
			}
			var ptr [8]byte
			binary.BigEndian.PutUint64(ptr[:], biasInt(ts))
			if err := b.Set(dataKey(setID, recs[i].Key), ptr[:], nil); err != nil {
				return err
			}
		} else if err := b.Set(dataKey(setID, recs[i].Key), payload, nil); err != nil {
			return err
		}
	}
	if err := d.pdb.Apply(b, d.writeOpts); err != nil {
		return err
	}
	d.stats.Puts.Add(int64(len(recs)))
	return nil
}

// Get point-reads one row, following the forward pointer for indexed sets.
func (d *DB) Get(set string, pk [16]byte, projection ...string) (Row, bool, error) {
	if d.closed.Load() {
		return nil, false, ErrClosed
	}
	d.stats.Gets.Add(1)
	sm, ok := d.setRef(set)
	if !ok {
		return nil, false, ErrUnknownSet
	}
	val, closer, err := d.pdb.Get(dataKey(sm.id, pk))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	payload := append([]byte(nil), val...)
	_ = closer.Close()

	// On an indexed set the D/ value is normally an 8-byte forward
	// pointer, but PutBatch also stores a row that carries no indexed
	// column there, payload and all. Follow the pointer when it leads
	// somewhere and fall back to reading the bytes as a row when it does
	// not, so such a row is returned rather than reported corrupt.
	if sm.indexed != "" && len(payload) == 8 {
		ts := unbiasInt(binary.BigEndian.Uint64(payload))
		v2, c2, err := d.pdb.Get(indexKey(sm.id, sm.indexCol, ts, pk))
		switch {
		case err == nil:
			payload = append([]byte(nil), v2...)
			_ = c2.Close()
		case err != pebble.ErrNotFound:
			return nil, false, err
		}
	}
	row, err := decodeRow(payload, projectionSet(projection))
	if err != nil {
		return nil, false, err
	}
	return row, true, nil
}

// DropSet removes a set and everything in it with two range deletes, which
// is what makes retention cheap: one tombstone per shard rather than one
// per row.
func (d *DB) DropSet(name string) error {
	if d.closed.Load() {
		return ErrClosed
	}
	d.mu.Lock()
	sm, ok := d.sets[name]
	if !ok {
		d.mu.Unlock()
		return ErrUnknownSet
	}
	setID, indexCol := sm.ID, sm.IndexCol
	delete(d.sets, name)
	delete(d.byID, sm.ID)
	d.mu.Unlock()

	b := d.pdb.NewBatch()
	defer b.Close()
	dp := dataPrefix(setID)
	if err := b.DeleteRange(dp, prefixEnd(dp), nil); err != nil {
		return err
	}
	ip := indexPrefix(setID, indexCol)
	if err := b.DeleteRange(ip, prefixEnd(ip), nil); err != nil {
		return err
	}
	if err := b.Delete(metaKey("set", name), nil); err != nil {
		return err
	}
	return d.pdb.Apply(b, d.metaOpts)
}

// PutDict and GetDict hold the label dictionaries under their own prefix,
// out of the way of set data.
func (d *DB) PutDict(name string, val []byte) error {
	if d.closed.Load() {
		return ErrClosed
	}
	return d.pdb.Set(dictKey(name), val, d.writeOpts)
}

func (d *DB) GetDict(name string) ([]byte, bool, error) {
	if d.closed.Load() {
		return nil, false, ErrClosed
	}
	v, closer, err := d.pdb.Get(dictKey(name))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := append([]byte(nil), v...)
	return out, true, closer.Close()
}

// DictKeys lists every dictionary name.
func (d *DB) DictKeys() ([]string, error) {
	if d.closed.Load() {
		return nil, ErrClosed
	}
	prefix := []byte{prefixDict}
	it, err := d.pdb.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []string
	for it.First(); it.Valid(); it.Next() {
		out = append(out, string(it.Key()[1:]))
	}
	sort.Strings(out)
	return out, it.Error()
}

// Compact collapses the whole keyspace. Batch ingest runs this once at the
// end: it typically shrinks the store substantially and puts the first
// queries on one dense level.
func (d *DB) Compact() error {
	if d.closed.Load() {
		return ErrClosed
	}
	return d.pdb.Compact([]byte{0x00}, []byte{0xff, 0xff, 0xff, 0xff}, true)
}

// Flush pushes memtables to disk, which is what makes a quiesced snapshot
// meaningful.
func (d *DB) Flush() error {
	if d.closed.Load() {
		return ErrClosed
	}
	return d.pdb.Flush()
}

func (d *DB) Snapshot() StatsSnapshot {
	s := StatsSnapshot{
		Puts:          d.stats.Puts.Load(),
		Gets:          d.stats.Gets.Load(),
		Scans:         d.stats.Scans.Load(),
		Queries:       d.stats.Queries.Load(),
		RowsScanned:   d.stats.RowsScanned.Load(),
		OpenIterators: d.stats.OpenIterators.Load(),
	}
	if d.closed.Load() {
		// Pebble's metrics are not available after Close; the counters
		// above still are, and are what the admin endpoint mostly wants.
		return s
	}
	m := d.pdb.Metrics()
	s.DiskBytes = m.DiskSpaceUsage()
	s.MemtableBytes = m.MemTable.Size
	s.L0Sublevels = int32(m.Levels[0].Sublevels)
	s.CacheHits = m.BlockCache.Hits
	s.CacheMisses = m.BlockCache.Misses
	return s
}

// Close flushes memtables and releases the directory lock. A clean close is
// durable in every durability profile.
func (d *DB) Close() error {
	if d.closed.Swap(true) {
		return nil
	}
	if n := d.stats.OpenIterators.Load(); n > 0 {
		d.opts.Logger.Printf("WARNING closing with %d iterators still open", n)
	}
	// Flush explicitly: with the WAL off, anything still in a memtable is
	// lost on close, so a clean shutdown has to push it down itself. This
	// is what makes "a graceful stop is durable in every profile" true.
	if err := d.pdb.Flush(); err != nil {
		d.opts.Logger.Printf("ERROR flushing on close: %v", err)
		_ = d.pdb.Close()
		return err
	}
	return d.pdb.Close()
}

func projectionSet(cols []string) map[string]struct{} {
	if len(cols) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		m[c] = struct{}{}
	}
	return m
}
