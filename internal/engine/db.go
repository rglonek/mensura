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

	// dirty marks a schema whose in-memory form is ahead of the record on
	// disk. It is not serialised: a reloaded schema is by definition the
	// one on disk.
	//
	// It exists because the persist was conditional on *this* call having
	// changed something. A meta write that failed -- a full disk, a
	// transient I/O fault -- returned the error and left the widened
	// schema in the map, so the next call saw nothing to change, skipped
	// the write, and handed the caller a set id whose meta record does not
	// exist. PutBatch then wrote rows under that id happily: after a
	// restart they belong to no set, so no query, no drop and no retention
	// sweep can reach them. That is the same silent orphaning the indexed-
	// column promotion and the timestamp-less row are refused to avoid,
	// arriving through the one path that had no retry.
	dirty bool
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

	// dropMu excludes a drop from every write that could be creating or
	// filling the same set. It is held shared by PutBatch and RegisterSet
	// for their whole duration -- the map bookkeeping *and* the pebble
	// apply -- and exclusively by DropSet.
	//
	// mu alone is not enough, because neither half of a write happens
	// under it end to end. A drop that released mu before applying its
	// batch let a concurrent PutBatch re-create the set under a fresh id,
	// write its rows and persist its meta record -- and then deleted that
	// record, because the delete is keyed by name. The rows survived the
	// range deletes, which name the old id, so the set came back after a
	// restart as disk nobody could query, drop or expire. The mirror case
	// is a write that had already resolved its set id and applied its
	// rows after the drop's range deletes had gone in.
	dropMu sync.RWMutex

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
	// A pebble fatal calls os.Exit, so nothing deferred by the caller
	// runs. The memtable is pushed down here instead, best effort.
	if pl, ok := po.Logger.(*pebbleLogger); ok {
		pl.setFatalHook(func() error { return pdb.Flush() })
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
	if err := it.Error(); err != nil {
		return err
	}

	// The high-water mark is read back as well as derived, and the larger
	// wins. Deriving it from the surviving sets alone made it *regress*
	// across a restart: DropSet removes the meta record, so dropping the
	// highest-numbered shard -- which retention does on every sweep --
	// let the next set created after a restart take that id back. Set ids
	// are what the D/ and I/ key prefixes are built from, so reuse only
	// stays harmless while the range deletes that emptied the old set are
	// still intact, which is an assumption nothing here should depend on.
	hw, err := d.loadNextID()
	if err != nil {
		return err
	}
	if hw > d.nextID {
		d.nextID = hw
	}
	if hw != d.nextID {
		// Only when it actually moved: a directory written by an earlier
		// build has no record, and one that is merely reopened should not
		// pay a synced meta write for nothing.
		return d.persistNextID()
	}
	return nil
}

// nextIDKey holds the set-id high-water mark, so an id is never reissued
// even after the set that held it has been dropped.
func nextIDKey() []byte { return metaKey("next_set_id") }

func (d *DB) loadNextID() (uint32, error) {
	v, closer, err := d.pdb.Get(nextIDKey())
	if err == pebble.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(v) < 4 {
		return 0, nil
	}
	return binary.BigEndian.Uint32(v), nil
}

func (d *DB) persistNextID() error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], d.nextID)
	return d.pdb.Set(nextIDKey(), b[:], d.metaOpts)
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
	d.dropMu.RLock()
	defer d.dropMu.RUnlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.setLocked(name, cols)
	return err
}

func (d *DB) setLocked(name string, cols []ColumnSpec) (*setMeta, error) {
	sm, ok := d.sets[name]
	created := !ok
	if !ok {
		sm = &setMeta{ID: d.nextID, Name: name, Columns: map[string]ColumnSpec{}}
		d.nextID++
		d.sets[name] = sm
		d.byID[sm.ID] = sm
		// Persisted before the set that uses it, so a crash between the
		// two loses the set rather than reissuing its id.
		if err := d.persistNextID(); err != nil {
			// The maps go back as they were. Leaving the set behind made
			// it exist for this process and for no other: the next call
			// found it, saw nothing to change and never wrote its meta
			// record, so its rows outlived every reader of them. The id
			// is not given back -- nextID is a high-water mark and
			// reissuing one is the thing it exists to prevent.
			delete(d.sets, name)
			delete(d.byID, sm.ID)
			return nil, err
		}
	}
	if created {
		sm.dirty = true
	}
	for _, c := range cols {
		existing, had := sm.Columns[c.Name]
		if had && existing.Type == c.Type && existing.Indexed == c.Indexed {
			continue
		}
		if !had {
			if c.Indexed && sm.Indexed == "" {
				// A set that already holds rows may not gain its first
				// indexed column. Every scan of an indexed set is bounded
				// to the I/ prefix, so the rows already written under D/
				// would become unreachable the moment the promotion
				// landed: unqueryable, and invisible to the retention
				// sweep, which is the same silent orphaning PutBatch
				// refuses a timestamp-less row to avoid.
				//
				// A set created in this very call, or one registered and
				// never filled, has nothing to orphan, so the common
				// "declare then write" shape is untouched.
				if !created {
					has, err := d.hasDataRows(sm.ID)
					if err != nil {
						return nil, err
					}
					if has {
						return nil, fmt.Errorf("engine: set %q already holds unindexed rows, so it cannot gain the indexed column %q; those rows would no longer be reachable by any scan", name, c.Name)
					}
				}
				sm.Indexed = c.Name
				sm.IndexCol = indexedColumnID
			}
			sm.Columns[c.Name] = c
			sm.dirty = true
			continue
		}
		// A widening type change is recorded; a narrowing one is ignored so
		// that one odd row cannot retype a column underneath a dashboard.
		if existing.Type == model.TypeInt && c.Type == model.TypeFloat {
			existing.Type = model.TypeFloat
			sm.Columns[c.Name] = existing
			sm.dirty = true
		}
	}
	// The flag rather than a "did this call change anything" local: a
	// write that failed on an earlier call leaves the record on disk
	// behind the schema in memory, and only a later call can put that
	// right. It costs one extra small meta write on the call after a
	// failure, and nothing at all otherwise.
	if sm.dirty {
		if err := d.persistSet(sm); err != nil {
			return nil, err
		}
		sm.dirty = false
	}
	return sm, nil
}

// hasDataRows reports whether anything is stored under a set's D/
// prefix. It is one seek, and it is asked only where a set is about to
// gain its first indexed column -- at most once in a set's lifetime.
func (d *DB) hasDataRows(setID uint32) (bool, error) {
	p := dataPrefix(setID)
	it, err := d.pdb.NewIter(&pebble.IterOptions{LowerBound: p, UpperBound: prefixEnd(p)})
	if err != nil {
		return false, err
	}
	found := it.First()
	err = it.Error()
	if cerr := it.Close(); err == nil {
		err = cerr
	}
	return found, err
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
	// Held until the rows are on disk, not only while the set id is
	// resolved: a drop that landed in between would range-delete around
	// rows this batch had not written yet.
	d.dropMu.RLock()
	defer d.dropMu.RUnlock()
	// The column set is derived before the lock, not under it. This walks
	// every column of every record -- ten thousand map operations for a
	// default batch -- and touches no shared state, so holding d.mu across
	// it serialised every writer in the process behind it, and every
	// query's setRef along with them.
	cols := make([]ColumnSpec, 0, 8)
	at := map[string]int{}
	for i := range recs {
		for name, v := range recs[i].Row {
			j, ok := at[name]
			if !ok {
				at[name] = len(cols)
				cols = append(cols, ColumnSpec{Name: name, Type: v.T, Indexed: name == model.TimestampField})
				continue
			}
			// The widest type in the batch wins, not whichever row
			// happened to be walked first. A column arriving as an int in
			// one row and a float in the next was registered as int64, so
			// the schema disagreed with the payloads already written under
			// it -- and setLocked's own int-to-float widening never fired,
			// because it only ever saw the one narrow spec.
			if cols[j].Type == model.TypeInt && v.T == model.TypeFloat {
				cols[j].Type = model.TypeFloat
			}
		}
	}
	d.mu.Lock()
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
				// Refused rather than stored under D/. Every scan of an
				// indexed set is bounded to the index prefix, so nothing
				// reads such a row back: it used to be written, counted
				// as a put and never seen again. Saying so is the only
				// honest answer, and it costs the store nothing because
				// its rows always carry a timestamp.
				return fmt.Errorf("engine: set %q is indexed on %q and this row does not carry it", set, indexed)
			}
			ts, ok := iv.AsInt()
			if !ok {
				return fmt.Errorf("engine: indexed column %q is not numeric", indexed)
			}
			ik := indexKey(setID, indexCol, ts, recs[i].Key)
			if err := b.Set(ik, payload, nil); err != nil {
				return err
			}
			if err := b.Set(dataKey(setID, recs[i].Key), dataPointer(ts), nil); err != nil {
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

	// On an indexed set the D/ value is normally a tagged forward
	// pointer, but an earlier build also stored a row that carries no
	// indexed column there, payload and all -- PutBatch refuses that row
	// now, and the ones already on disk are still readable here. The tag
	// distinguishes the two; the untagged 8-byte pointer form an earlier
	// build wrote is still followed, and falls back to reading the bytes
	// as a row when the index key it names does not exist.
	if ts, isPtr := readDataPointer(payload); isPtr && sm.indexed != "" {
		v2, c2, err := d.pdb.Get(indexKey(sm.id, sm.indexCol, ts, pk))
		switch {
		case err == nil:
			payload = append([]byte(nil), v2...)
			_ = c2.Close()
		case err != pebble.ErrNotFound:
			return nil, false, err
		case taggedDataPointer(payload):
			// The tag says this is a pointer and nothing else, so a
			// missing index key means the row is gone -- half-deleted, or
			// never fully written. Falling through decoded the pointer's
			// own nine bytes as a row: encodeRow's leading column count
			// reads as zero, so Get answered "found" with an empty row
			// and every caller saw a record that carries no columns
			// rather than no record at all. Only the untagged eight-byte
			// form an earlier build wrote still falls through, because
			// there those bytes really may be a small row.
			return nil, false, nil
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
//
// It runs to completion before any write may create or fill a set again:
// the meta record is keyed by name while the rows are keyed by id, so a
// write interleaved with the delete produced rows no later drop, query or
// retention sweep could reach.
func (d *DB) DropSet(name string) error {
	if d.closed.Load() {
		return ErrClosed
	}
	d.dropMu.Lock()
	defer d.dropMu.Unlock()
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

	// A pebble batch applies whole or not at all, so a failure below
	// leaves every row and the meta record exactly where they were --
	// and the set has to come back with them. Dropping it from the maps
	// anyway made a failed delete look like a successful one to
	// everything except its caller: db.Sets() no longer listed the
	// shard, so no query scanned it and no later retention sweep tried
	// again, while the data and its meta record sat on disk until a
	// restart brought them back. Nothing may hold the maps while this
	// runs: DropSet owns dropMu exclusively, so no writer can have
	// re-created the name in the meantime.
	restore := func(err error) error {
		d.mu.Lock()
		d.sets[name] = sm
		d.byID[sm.ID] = sm
		d.mu.Unlock()
		return err
	}

	b := d.pdb.NewBatch()
	defer b.Close()
	dp := dataPrefix(setID)
	if err := b.DeleteRange(dp, prefixEnd(dp), nil); err != nil {
		return restore(err)
	}
	ip := indexPrefix(setID, indexCol)
	if err := b.DeleteRange(ip, prefixEnd(ip), nil); err != nil {
		return restore(err)
	}
	if err := b.Delete(metaKey("set", name), nil); err != nil {
		return restore(err)
	}
	if err := d.pdb.Apply(b, d.metaOpts); err != nil {
		return restore(err)
	}
	return nil
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
