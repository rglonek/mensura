package engine

import (
	"context"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
)

// NoBlockCache has to reach pebble as a cache, not as an absence of one.
//
// Leaving pebble.Options.Cache nil does not disable the block cache:
// pebble's EnsureDefaults allocates an 8 MiB one. So `db.cache_bytes: -1`
// -- the setting an operator reaches for precisely to reclaim that memory
// on a small host -- silently handed them 8 MiB of it, and nothing said
// so. Pebble has no "no cache" mode, so the sentinel is translated into
// the smallest cache it will build.
func TestNoBlockCacheAllocatesAMinimalCache(t *testing.T) {
	opts := DefaultOptions()
	opts.CacheBytes = NoBlockCache
	po := opts.pebbleOptions()
	if po.Cache == nil {
		t.Fatal("NoBlockCache left pebble to allocate its own 8 MiB default")
	}
	po.Cache.Unref()

	sized := DefaultOptions()
	sized.CacheBytes = 1 << 20
	spo := sized.pebbleOptions()
	if spo.Cache == nil {
		t.Fatal("an explicit cache size allocated no cache")
	}
	spo.Cache.Unref()
}

// And a store opened that way still reads and writes.
func TestStoreOpensWithNoBlockCache(t *testing.T) {
	opts := DefaultOptions()
	opts.Path = t.TempDir()
	opts.CacheBytes = NoBlockCache
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	var recs []Record
	for i := 0; i < 200; i++ {
		var pk [16]byte
		pk[0], pk[1] = byte(i), byte(i>>8)
		recs = append(recs, Record{Key: pk, Row: Row{
			model.TimestampField: model.Int(int64(i)),
			"v":                  model.Int(int64(i)),
		}})
	}
	if err := db.PutBatch("app", recs); err != nil {
		t.Fatalf("put: %v", err)
	}
	it, err := db.Query("app").Between(model.TimestampField, 0, 1000).Run(context.Background())
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer it.Close()
	n := 0
	for it.Next() {
		n++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != len(recs) {
		t.Fatalf("scanned %d rows, expected %d", n, len(recs))
	}
}
