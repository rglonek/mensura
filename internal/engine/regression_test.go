package engine

import (
	"context"
	"testing"

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
