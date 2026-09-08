package engine

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/rglonek/mensura/pkg/model"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	opts := Options{Path: t.TempDir(), CacheBytes: NoBlockCache, MemTableSizeBytes: 1 << 20}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func collect(t *testing.T, it *Iter) []Row {
	t.Helper()
	var out []Row
	for it.Next() {
		_, row := it.Record()
		out = append(out, row)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("close iterator: %v", err)
	}
	return out
}

// A range on a set with no indexed column has nothing to seek on, so it
// has to become a predicate. It used to be dropped on that branch alone:
// Between() returned every row in the set, which is the one failure mode
// a range test must never have.
func TestBetweenIsHonouredOnAnUnindexedSet(t *testing.T) {
	db := openTestDB(t)
	// No indexed column: the engine only indexes model.TimestampField.
	if err := db.RegisterSet("events", []ColumnSpec{{Name: "at", Type: model.TypeInt}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	var recs []Record
	for i := int64(0); i < 10; i++ {
		var pk [16]byte
		pk[0] = byte(i)
		recs = append(recs, Record{Key: pk, Row: Row{"at": model.Int(i), "v": model.Int(i * 2)}})
	}
	if err := db.PutBatch("events", recs); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, indexed, _ := db.Schema("events"); indexed != "" {
		t.Fatalf("the set gained an indexed column %q; the test is not exercising the unindexed path", indexed)
	}
	it, err := db.Query("events").Between("at", 3, 5).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	rows := collect(t, it)
	if len(rows) != 3 {
		t.Fatalf("Between(3, 5) returned %d of 10 rows; the bound was dropped", len(rows))
	}
	for _, r := range rows {
		v, _ := r["at"].AsInt()
		if v < 3 || v > 5 {
			t.Errorf("row at=%d is outside the requested range", v)
		}
	}
}

// A regex on a label expands to one IN member per matching dictionary
// value -- up to the whole cardinality budget -- and a linear walk of that
// list per row is O(rows x values). Indexing it must not change what the
// predicate means.
func TestLargeInListMatchesTheLinearWalk(t *testing.T) {
	// Both lists carry the same members; the second is padded past the
	// threshold where the expression indexes them.
	members := []model.Value{model.Int(1), model.Float(2.5), model.String("s"), model.String("7")}
	big := append([]model.Value(nil), members...)
	for i := 100; i < 164; i++ {
		big = append(big, model.Int(int64(i)))
	}

	for _, tc := range []struct {
		name string
		vals []model.Value
	}{{"small", members}, {"indexed", big}} {
		t.Run(tc.name, func(t *testing.T) {
			e := In("c", tc.vals...)
			for _, probe := range []struct {
				v    model.Value
				want bool
			}{
				{model.Int(1), true},
				{model.Float(1), true}, // numeric members compare by magnitude
				{model.Int(2), false},
				{model.Float(2.5), true},
				{model.String("s"), true},
				{model.Int(7), false},      // a string member never matches a number
				{model.String("1"), false}, // and a numeric member never matches a string
				{model.String("nope"), false},
				{model.Bool(true), false},
			} {
				row := newLazyRow(encodeRow(Row{"c": probe.v}))
				if got := e.eval(row); got != probe.want {
					t.Errorf("%v in list = %v, want %v", probe.v, got, probe.want)
				}
			}
			// A column the row does not carry never matches.
			if In("missing", tc.vals...).eval(newLazyRow(encodeRow(Row{"c": model.Int(1)}))) {
				t.Error("a row without the column matched an IN list")
			}
		})
	}
}

// A bool anywhere in the list leaves the walk in place, because valueEqual
// compares bools exactly and the index does not model them.
func TestInListWithBoolsStaysExact(t *testing.T) {
	var vals []model.Value
	for i := 0; i < 32; i++ {
		vals = append(vals, model.Int(int64(i)))
	}
	vals = append(vals, model.Bool(true))
	e := In("c", vals...)
	if !e.eval(newLazyRow(encodeRow(Row{"c": model.Bool(true)}))) {
		t.Error("a bool member stopped matching once the list grew")
	}
	if e.eval(newLazyRow(encodeRow(Row{"c": model.Bool(false)}))) {
		t.Error("the wrong bool matched")
	}
	if !e.eval(newLazyRow(encodeRow(Row{"c": model.Int(3)}))) {
		t.Error("a numeric member stopped matching alongside a bool")
	}
}

// The set-id high-water mark is persisted, not re-derived from the sets
// that happen to survive.
//
// DropSet removes the meta record, so deriving nextID from max(ID)+1 made
// it regress across a restart: dropping the highest-numbered shard --
// which retention does on every sweep -- let the next set created after a
// restart take that id back. Set ids are what the D/ and I/ key prefixes
// are built from, so reuse only stays harmless while the range deletes
// that emptied the old set are intact, which is not an assumption this
// layer should depend on.
func TestSetIDsAreNeverReissuedAfterADrop(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Path: dir, CacheBytes: NoBlockCache, MemTableSizeBytes: 1 << 20}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	put := func(db *DB, set string, v int64) {
		t.Helper()
		rec := Record{Row: Row{model.TimestampField: model.Int(v), "n": model.Int(v)}}
		rec.Key[0] = byte(v)
		if err := db.PutBatch(set, []Record{rec}); err != nil {
			t.Fatalf("put %s: %v", set, err)
		}
	}
	put(db, "a@1", 1)
	put(db, "b@2", 2)
	db.mu.RLock()
	dropped := db.sets["b@2"].ID
	db.mu.RUnlock()
	if err := db.DropSet("b@2"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	put(db2, "c@3", 3)
	db2.mu.RLock()
	reused := db2.sets["c@3"].ID
	db2.mu.RUnlock()
	if reused == dropped {
		t.Fatalf("set id %d was reissued to a new set after the set holding it was dropped", dropped)
	}
}

// An earlier build stored a row that carries no indexed column under D/,
// payload and all, while an indexed row's D/ value is a forward pointer.
// The tag tells them apart; the untagged 8-byte form an even earlier build
// wrote is still followed, and the fallback is what keeps an 8-byte *row*
// payload readable rather than being misread as a pointer.
func TestShortRowPayloadIsNotMistakenForALegacyPointer(t *testing.T) {
	db := openTestDB(t)
	// An indexed set, so Get consults readDataPointer at all.
	var indexed Record
	indexed.Key[0] = 1
	indexed.Row = Row{model.TimestampField: model.Int(42), "n": model.Int(7)}
	if err := db.PutBatch("s", []Record{indexed}); err != nil {
		t.Fatalf("put indexed: %v", err)
	}
	// A row with no timestamp column sits under D/ with its payload
	// there. encodeRow of one small column is exactly the length the
	// legacy pointer form used to be recognised by. PutBatch refuses to
	// write one now -- it would be invisible to every scan -- so the
	// record an older build left behind is written straight to pebble.
	var bare Record
	bare.Key[0] = 2
	bare.Row = Row{"ab": model.Bool(true)}
	if err := db.PutBatch("s", []Record{bare}); err == nil {
		t.Fatal("PutBatch accepted a row with no indexed column on an indexed set")
	}
	sm, _ := db.setRef("s")
	if err := db.pdb.Set(dataKey(sm.id, bare.Key), encodeRow(bare.Row), db.writeOpts); err != nil {
		t.Fatalf("write legacy bare row: %v", err)
	}
	row, ok, err := db.Get("s", bare.Key)
	if err != nil || !ok {
		t.Fatalf("get bare row: ok=%v err=%v", ok, err)
	}
	if v, present := row["ab"]; !present || v.T != model.TypeBool || !v.B {
		t.Fatalf("an unindexed row stored under D/ must read back as a row, got %+v", row)
	}
	row, ok, err = db.Get("s", indexed.Key)
	if err != nil || !ok {
		t.Fatalf("get indexed row: ok=%v err=%v", ok, err)
	}
	if v := row["n"]; v.I != 7 {
		t.Fatalf("indexed row did not follow its pointer: %+v", row)
	}
}

// A drop and a write to the same set name must not interleave.
//
// DropSet used to release the lock before applying its batch, so a
// PutBatch landing in that window re-created the set under a fresh id --
// its rows written outside the range deletes, its meta record then deleted
// by name. After a restart the shard was absent from Sets(), so it was
// invisible to every query, to the catalogue and to the retention sweep,
// while its rows still occupied disk with no way to reclaim them.
func TestDropDoesNotOrphanAConcurrentWrite(t *testing.T) {
	dir := t.TempDir()
	open := func() *DB {
		t.Helper()
		db, err := Open(Options{Path: dir, CacheBytes: NoBlockCache, MemTableSizeBytes: 1 << 20})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return db
	}
	db := open()
	rec := func(n int64) Record {
		var r Record
		r.Key[0] = byte(n)
		r.Key[1] = byte(n >> 8)
		r.Row = Row{model.TimestampField: model.Int(n), "n": model.Int(n)}
		return r
	}
	// Many rounds with several writers per drop: the window between the
	// map removal and the batch apply is short, so the odds of landing in
	// it come from repetition rather than from any one attempt.
	for round := int64(0); round < 200; round++ {
		if err := db.PutBatch("s", []Record{rec(round*100 + 1)}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(5)
		go func() {
			defer wg.Done()
			if err := db.DropSet("s"); err != nil && err != ErrUnknownSet {
				t.Errorf("drop: %v", err)
			}
		}()
		for w := int64(0); w < 4; w++ {
			go func(n int64) {
				defer wg.Done()
				if err := db.PutBatch("s", []Record{rec(n)}); err != nil {
					t.Errorf("put: %v", err)
				}
			}(round*100 + 2 + w)
		}
		wg.Wait()
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Whatever survived has to be reachable: a set holding rows must be
	// in the meta the reopen loads, or nothing can ever read or drop it.
	db = open()
	defer func() { _ = db.Close() }()
	sets := map[string]bool{}
	for _, n := range db.Sets() {
		sets[n] = true
	}
	it, err := db.pdb.NewIter(&pebble.IterOptions{})
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()
	orphans := 0
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) < 5 || (k[0] != prefixData && k[0] != prefixIndex) {
			continue
		}
		id := binary.BigEndian.Uint32(k[1:5])
		db.mu.RLock()
		_, known := db.byID[id]
		db.mu.RUnlock()
		if !known {
			orphans++
		}
	}
	if orphans > 0 {
		t.Fatalf("%d row(s) belong to a set id no meta record names: they are invisible to every query, drop and retention sweep", orphans)
	}
}

// A set that already holds rows may not gain its first indexed column.
// Every scan of an indexed set is bounded to the I/ prefix, so promoting
// one made the rows already written under D/ unreachable by any query,
// and invisible to retention -- the same silent orphaning PutBatch
// refuses a timestamp-less row to avoid.
func TestSetCannotGainAnIndexOnceItHoldsRows(t *testing.T) {
	db := openTestDB(t)
	if err := db.RegisterSet("logs", []ColumnSpec{{Name: "msg", Type: model.TypeString}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	var pk [16]byte
	pk[0] = 1
	if err := db.PutBatch("logs", []Record{{Key: pk, Row: Row{"msg": model.String("hello")}}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// The row is readable now.
	if _, ok, err := db.Get("logs", pk); err != nil || !ok {
		t.Fatalf("row is not readable before the promotion: ok=%v err=%v", ok, err)
	}

	err := db.RegisterSet("logs", []ColumnSpec{{Name: model.TimestampField, Type: model.TypeInt, Indexed: true}})
	if err == nil {
		t.Fatal("promoting a set that already holds unindexed rows was accepted; those rows are now unreachable")
	}
	if !strings.Contains(err.Error(), "unindexed rows") {
		t.Fatalf("error does not name the problem: %v", err)
	}
	// The set is untouched, so the rows stay readable rather than being
	// half-promoted into invisibility.
	if _, ok, err := db.Get("logs", pk); err != nil || !ok {
		t.Fatalf("row became unreadable after the refusal: ok=%v err=%v", ok, err)
	}
	if _, indexed, _ := db.Schema("logs"); indexed != "" {
		t.Fatalf("set was promoted to indexed on %q despite the refusal", indexed)
	}
}

// A set with no rows yet may still gain an index: "declare the set, then
// write to it" is the ordinary shape and has nothing to orphan.
func TestEmptySetMayStillGainAnIndex(t *testing.T) {
	db := openTestDB(t)
	if err := db.RegisterSet("metrics", []ColumnSpec{{Name: "msg", Type: model.TypeString}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := db.RegisterSet("metrics", []ColumnSpec{{Name: model.TimestampField, Type: model.TypeInt, Indexed: true}}); err != nil {
		t.Fatalf("an empty set was refused an index: %v", err)
	}
	if _, indexed, _ := db.Schema("metrics"); indexed != model.TimestampField {
		t.Fatalf("indexed column is %q, want %q", indexed, model.TimestampField)
	}
}

// The widest type in a batch decides the column, not whichever row the
// map walk reached first. A column arriving as an int in one row and a
// float in the next was registered as int64, so the schema disagreed with
// the payloads stored under it.
func TestPutBatchWidensColumnTypeAcrossTheBatch(t *testing.T) {
	db := openTestDB(t)
	recs := make([]Record, 0, 2)
	for i, v := range []model.Value{model.Int(1), model.Float(1.5)} {
		var pk [16]byte
		pk[0] = byte(i + 1)
		recs = append(recs, Record{Key: pk, Row: Row{
			model.TimestampField: model.Int(int64(1000 + i)),
			"latency":            v,
		}})
	}
	if err := db.PutBatch("app", recs); err != nil {
		t.Fatalf("put: %v", err)
	}
	cols, _, ok := db.Schema("app")
	if !ok {
		t.Fatal("set was not registered")
	}
	for _, c := range cols {
		if c.Name != "latency" {
			continue
		}
		if c.Type != model.TypeFloat {
			t.Fatalf("latency is %s, want float64: the batch carried a float the schema does not admit", c.Type)
		}
		return
	}
	t.Fatal("latency column is missing from the schema")
}

// False is the identity of OR. An empty disjunction returning true widened
// the query instead of narrowing it, which is the one failure mode a
// predicate must never have.
func TestEmptyOrMatchesNothing(t *testing.T) {
	db := openTestDB(t)
	var pk [16]byte
	pk[0] = 1
	if err := db.PutBatch("app", []Record{{Key: pk, Row: Row{
		model.TimestampField: model.Int(1000), "v": model.Int(7),
	}}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	it, err := db.Query("app").Where(Or()).Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rows := collect(t, it); len(rows) != 0 {
		t.Fatalf("an empty OR matched %d row(s); it must match none", len(rows))
	}
}

// A tagged forward pointer whose index key is gone means the row is gone.
// Falling through and decoding the pointer's own nine bytes as a row read
// encodeRow's leading column count as zero, so Get answered "found" with a
// row that carries no columns -- and every caller then saw a record that
// exists and holds nothing rather than no record at all. The untagged
// eight-byte form an earlier build wrote still falls through, because
// there those bytes really may be a small row.
func TestGetReportsAMissingIndexedRowAsAbsent(t *testing.T) {
	db := openTestDB(t)
	pk := [16]byte{7}
	if err := db.PutBatch("s", []Record{{Key: pk, Row: Row{
		model.TimestampField: model.Int(1000), "v": model.Int(42),
	}}}); err != nil {
		t.Fatalf("put: %v", err)
	}
	sm, ok := db.setRef("s")
	if !ok {
		t.Fatal("set was not registered")
	}
	if _, found, err := db.Get("s", pk); err != nil || !found {
		t.Fatalf("baseline get: found=%v err=%v", found, err)
	}
	// Delete the payload and leave the pointer, which is the shape a
	// half-applied delete or a torn write leaves behind.
	if err := db.pdb.Delete(indexKey(sm.id, sm.indexCol, 1000, pk), db.writeOpts); err != nil {
		t.Fatalf("delete index key: %v", err)
	}
	row, found, err := db.Get("s", pk)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if found {
		t.Fatalf("a dangling pointer read as a found row with %d column(s)", len(row))
	}
}
