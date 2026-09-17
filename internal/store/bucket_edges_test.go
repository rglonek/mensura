package store

import (
	"testing"

	"github.com/rglonek/mensura/pkg/wire"
)

// A stored bucket set whose two slices have drifted apart -- a shorter
// edge list than bucket list, which a half-written catalogue save, a hand
// edit or an older build can leave behind -- used to take the write path
// down with an index out of range: both slices were grown against
// len(Buckets), so the difference survived and the edge assignment ran
// off the end. In plugin mode that panic is not recovered and it kills
// the process that owns the data directory.
func TestWriteRepairsADriftedBucketSet(t *testing.T) {
	s := openTestStore(t)
	// Two buckets, declared the ordinary way.
	for i, name := range []string{"b0", "b1"} {
		if _, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{{
			Set: "app", Field: name, BucketSet: "hdr", BucketIndex: i, BucketEdge: float64(i),
		}}}, "", "test"); err != nil {
			t.Fatalf("declaring bucket %s: %v", name, err)
		}
	}
	// Drift the record the way a truncated save would.
	s.mu.Lock()
	e := s.catalogue["app"]
	bs := e.BucketSets["hdr"]
	bs.Edges = bs.Edges[:0]
	e.BucketSets["hdr"] = bs
	s.mu.Unlock()

	if _, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{{
		Set: "app", Field: "b2", BucketSet: "hdr", BucketIndex: 2, BucketEdge: 2,
	}}}, "", "test"); err != nil {
		t.Fatalf("declaring a bucket over a drifted record: %v", err)
	}

	cat := s.Catalogue()
	var got wire.BucketSetInfo
	for _, set := range cat.Sets {
		if set.Name == "app" {
			got = set.BucketSets["hdr"]
		}
	}
	if len(got.Buckets) != 3 {
		t.Fatalf("buckets = %v, want three", got.Buckets)
	}
	if len(got.Edges) != len(got.Buckets) {
		t.Fatalf("the edge list was left at %d for %d buckets; a heatmap names a series after its edge, so the short tail draws series with no name at all",
			len(got.Edges), len(got.Buckets))
	}
	if got.Edges[2] != 2 {
		t.Fatalf("edges = %v, want the declared edge at position 2", got.Edges)
	}
}
