package store

import (
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// CatalogueVersion is the version of the catalogue *schema*, and its own
// contract is that only a real change moves it: it is the ETag on
// /v1/catalogue and the number clients watch for a metadata change.
// `applyFieldMeta` and `SetRetentionFor` each compare before bumping.
// `SetKeyScheme` did not, so a spec declaring `key:` for a set moved the
// version on every write that carried the declaration -- missing every
// cached ETag, waking every watcher, and making Write persist the whole
// catalogue record because it sees the version has moved.
func TestRepeatedDeclarationsDoNotMoveTheVersion(t *testing.T) {
	s := openTestStore(t)
	retention, shard := int64(30000), int64(3600000)
	req := func() *wire.WriteRequest {
		return &wire.WriteRequest{
			FieldMeta: []wire.FieldMeta{{
				Set: "app", Field: "tx", Kind: model.KindCounter, Unit: "bytes",
				UnitHint: "bytes", Description: "bytes sent", MaxIntervalMs: 30000,
				LimitMin: floatPtr(0), LimitMax: floatPtr(100),
			}},
			SetMeta: []wire.SetMeta{{
				Set: "app", RetentionMs: &retention, ShardMs: &shard,
				KeyScheme: model.KeyOffset,
			}},
			Batches: []model.Batch{{Set: "app", Samples: []model.Sample{{
				TSMs: base(), Labels: map[string]string{"host": "a"}, KeyHint: "h",
				Fields: map[string]model.Value{"tx": model.Int(1)},
			}}}},
		}
	}
	// The first two writes settle the schema; whatever they move is real.
	for i := 0; i < 2; i++ {
		if _, err := s.Write(req(), "", ""); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	settled := s.CatalogueVersion()
	etag := s.CatalogueETag()
	// A third ingest process starting with the same spec repeats the whole
	// declaration verbatim.
	for i := 0; i < 3; i++ {
		if _, err := s.Write(req(), "", ""); err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
	}
	if got := s.CatalogueVersion(); got != settled {
		t.Fatalf("a declaration repeated verbatim moved the catalogue version %d -> %d", settled, got)
	}
	if got := s.CatalogueETag(); got != etag {
		t.Fatalf("the catalogue ETag moved from %s to %s with no change behind it", etag, got)
	}
	// A real change still moves it, and still takes effect.
	if err := s.applySetMeta([]wire.SetMeta{{Set: "app", KeyScheme: model.KeyContent}}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if s.CatalogueVersion() == settled {
		t.Fatal("changing the key scheme did not move the catalogue version")
	}
	if got := s.keyScheme("app"); got != model.KeyContent {
		t.Fatalf("key scheme = %q, want content", got)
	}
}

// Dropping a set the catalogue never held changes nothing, so it must not
// move the version either.
func TestForgettingAnUnknownSetIsNotAChange(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "app", []model.Sample{{
		TSMs: base(), Labels: map[string]string{"host": "a"},
		Fields: map[string]model.Value{"tx": model.Int(1)},
	}})
	before := s.CatalogueVersion()
	s.ForgetSet("never-existed")
	if got := s.CatalogueVersion(); got != before {
		t.Fatalf("forgetting an unknown set moved the version %d -> %d", before, got)
	}
	s.ForgetSet("app")
	if got := s.CatalogueVersion(); got == before {
		t.Fatal("forgetting a real set did not move the version")
	}
	if len(s.Sets()) != 0 {
		t.Fatalf("app survived ForgetSet: %v", s.Sets())
	}
}
