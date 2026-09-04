package store

import (
	"errors"
	"math"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// A bucket index outside the accepted range is named, not skipped.
//
// It used to `continue`, which jumped over the change detection at the
// bottom of applyFieldMeta's loop -- so the kind, unit and limits carried
// in the same declaration were written into the catalogue with
// CatalogueVersion standing still, and every client holding the old ETag
// was answered 304 for a catalogue that had moved.
func TestFieldMetaRefusesAnOutOfRangeBucketIndex(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{{
		Set: "app", Field: "b0", Kind: model.KindGauge,
		BucketSet: "latency", BucketIndex: maxBucketIndex + 1,
	}}}, "", "test")
	if err == nil {
		t.Fatal("an out-of-range bucket index was accepted")
	}
	var bad *ErrBadRequest
	if !errors.As(err, &bad) {
		t.Fatalf("expected a client fault, got %T: %v", err, err)
	}
	// A client fault is a 400, so the sink drops it once instead of
	// retrying it at full rate forever.
	if s.CatalogueVersion() != 0 {
		t.Fatalf("the refused declaration still moved the catalogue version to %d", s.CatalogueVersion())
	}
}

// Metadata that changes something bumps the version; metadata that
// repeats itself does not. The bucket-set arm used to be able to swallow
// the first of those.
func TestFieldMetaVersionTracksRealChanges(t *testing.T) {
	s := openTestStore(t)
	declare := func(kind model.Kind) {
		t.Helper()
		if _, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{{
			Set: "app", Field: "b0", Kind: kind,
			BucketSet: "latency", BucketIndex: 0, BucketEdge: 0,
		}}}, "", "test"); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	declare(model.KindGauge)
	first := s.CatalogueVersion()
	if first == 0 {
		t.Fatal("a new set and field did not move the catalogue version")
	}
	declare(model.KindGauge)
	if s.CatalogueVersion() != first {
		t.Fatalf("repeating a declaration moved the version from %d to %d", first, s.CatalogueVersion())
	}
	declare(model.KindCounter)
	if s.CatalogueVersion() == first {
		t.Fatal("a kind change alongside a bucket declaration did not move the version")
	}
}

// The ingest-progress set is the one reserved name a client may write, so
// a spec may declare its retention. applySetMeta refused every reserved
// name, which made the one set every ingester produces also the one set no
// spec could age out.
func TestSetMetaAcceptsTheIngestSet(t *testing.T) {
	s := openTestStore(t)
	ms := int64(3_600_000)
	if _, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{
		Set: model.IngestSet, RetentionMs: &ms, ShardMs: &ms,
	}}}, "", "test"); err != nil {
		t.Fatalf("declaring retention for the ingest set: %v", err)
	}
	if got := s.retentionFor(model.IngestSet); got.Milliseconds() != ms {
		t.Fatalf("retention for %s is %s, want %dms", model.IngestSet, got, ms)
	}
	// Every other reserved name is still refused.
	if _, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{
		Set: model.CatalogueSet, RetentionMs: &ms,
	}}}, "", "test"); err == nil {
		t.Fatalf("a reserved set other than %s was accepted", model.IngestSet)
	}
}

// shardsInRange is shardsFor over a list the caller already holds, so the
// LABELS filter scan can enumerate every set's shards from one pass rather
// than walking the whole store once per set. The two must agree.
func TestShardsInRangeMatchesShardsFor(t *testing.T) {
	s := openTestStore(t)
	base := int64(1756382400000)
	for i := 0; i < 3; i++ {
		writeSamples(t, s, "app", []model.Sample{{
			TSMs:   base + int64(i)*24*3600*1000,
			Labels: map[string]string{"host": "web1"},
			Fields: map[string]model.Value{"v": model.Int(int64(i))},
		}})
	}
	all := s.db.Sets()
	for _, r := range []struct{ from, to int64 }{
		{math.MinInt64, math.MaxInt64},
		{base, base},
		{base + 24*3600*1000, base + 2*24*3600*1000},
		{base - 10*24*3600*1000, base - 9*24*3600*1000},
	} {
		want := s.shardsFor("app", r.from, r.to)
		got := shardsInRange("app", all, r.from, r.to)
		if len(want) != len(got) {
			t.Fatalf("range %d..%d: shardsFor %v, shardsInRange %v", r.from, r.to, want, got)
		}
		for i := range want {
			if want[i] != got[i] {
				t.Fatalf("range %d..%d: shardsFor %v, shardsInRange %v", r.from, r.to, want, got)
			}
		}
	}
}

// labelIndicesMatching replaces a whole-dictionary copy plus one lock
// acquisition per match. It must resolve the same values, and report the
// key's full size so a regex that matches everything can still be folded.
func TestLabelIndicesMatching(t *testing.T) {
	s := openTestStore(t)
	for _, host := range []string{"web1", "web2", "db1"} {
		if _, err := s.intern("host", host); err != nil {
			t.Fatalf("intern %s: %v", host, err)
		}
	}
	vals, total := s.labelIndicesMatching("host", func(v string) bool { return len(v) > 3 && v[:3] == "web" })
	if total != 3 {
		t.Fatalf("total = %d, want 3", total)
	}
	if len(vals) != 2 {
		t.Fatalf("matched %d values, want 2", len(vals))
	}
	for _, v := range vals {
		idx, _ := v.AsInt()
		got, ok := s.labelValue("host", int32(idx))
		if !ok || got[:3] != "web" {
			t.Fatalf("index %d resolves to %q", idx, got)
		}
	}
	// An unknown key is empty rather than an error.
	if vals, total := s.labelIndicesMatching("nosuch", func(string) bool { return true }); vals != nil || total != 0 {
		t.Fatalf("unknown key: %v %d", vals, total)
	}
}
