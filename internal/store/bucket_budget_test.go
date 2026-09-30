package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/wire"
)

// A bucket-set declaration is a catalogue entry a client names, and it was
// the last one with no budget at all.
//
// applyFieldMeta grows a bucket set's column and edge lists to the
// declared index -- up to maxBucketIndex, 4096 -- so one wire.FieldMeta
// retains about 140 KiB. Every one of them may name the *same* field, so
// MaxFieldsPerSet counted a number that never moved and MaxSets counted
// one set: a single write request at the store's default 32 MiB body
// limit holds on the order of half a million such declarations, which is
// tens of gigabytes of catalogue -- persisted, because the catalogue is
// one JSON record.
func TestABucketSetIsHeldToABudget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.MaxBucketSetsPerSet = 4
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var metas []wire.FieldMeta
	for i := 0; i < 5; i++ {
		metas = append(metas, wire.FieldMeta{
			Set: "app", Field: "shared", BucketSet: fmt.Sprintf("bs%d", i), BucketIndex: 4095,
		})
	}
	_, err = s.Write(&wire.WriteRequest{FieldMeta: metas}, "", "")
	var bad *ErrBadRequest
	if !errors.As(err, &bad) {
		t.Fatalf("five bucket sets past a limit of four were accepted: err = %v", err)
	}
	if !strings.Contains(bad.Msg, "bs4") {
		t.Errorf("the refusal does not name the offending bucket set: %s", bad.Msg)
	}
	// Refused as a whole: the budget is weighed before any of the request
	// is applied, which is the shape the set and field budgets beside it
	// already have.
	for _, si := range s.Catalogue().Sets {
		if si.Name == "app" && len(si.BucketSets) != 0 {
			t.Errorf("%d bucket set(s) landed from a refused request", len(si.BucketSets))
		}
	}

	// Four still fit, and re-declaring them is not growth.
	if _, err := s.Write(&wire.WriteRequest{FieldMeta: metas[:4]}, "", ""); err != nil {
		t.Fatalf("four bucket sets within the limit were refused: %v", err)
	}
	if _, err := s.Write(&wire.WriteRequest{FieldMeta: metas[:4]}, "", ""); err != nil {
		t.Fatalf("re-declaring the same four bucket sets was refused: %v", err)
	}
	// And a non-positive budget switches the gate off, as it does for
	// max_sets and max_fields_per_set.
	cfg2 := cfg
	cfg2.DataDir = t.TempDir()
	cfg2.MaxBucketSetsPerSet = -1
	s2, err := Open(cfg2)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if _, err := s2.Write(&wire.WriteRequest{FieldMeta: metas}, "", ""); err != nil {
		t.Fatalf("a disabled budget still refused: %v", err)
	}
}

// The bucket-set name is the one client-chosen name in a declaration that
// nothing checked, and it becomes a key of the catalogue's bucket-set map,
// is persisted inside the single catalogue record, and is served from
// /v1/catalogue.
func TestABucketSetNameIsValidated(t *testing.T) {
	s := openTestStore(t)
	for _, name := range []string{
		strings.Repeat("x", 200), // longer than any identifier this API accepts
		"evil@shard",             // the separator between a set and its shard suffix
		"has\x00nul",             // a NUL, which the dictionary key format uses as a separator
		"spaces here",            //
		"",                       // handled as "no bucket set", checked below
	} {
		_, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{
			{Set: "app", Field: "f", BucketSet: name, BucketIndex: 0},
		}}, "", "")
		if name == "" {
			if err != nil {
				t.Errorf("an absent bucket set was refused: %v", err)
			}
			continue
		}
		var bad *ErrBadRequest
		if !errors.As(err, &bad) {
			t.Errorf("bucket set name %q was accepted: err = %v", name, err)
		}
	}
	// An ordinary name still works, including the digit-leading form the
	// bucket columns themselves use.
	if _, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{
		{Set: "app", Field: "00", BucketSet: "hdr24", BucketIndex: 0},
		{Set: "app", Field: "01", BucketSet: "0hdr", BucketIndex: 1},
	}}, "", ""); err != nil {
		t.Fatalf("an ordinary bucket set name was refused: %v", err)
	}
}

// The free text a field declaration carries is held in memory and written
// inside the one JSON record the whole catalogue is persisted as. The
// entry count is capped by MaxFieldsPerSet; the bytes per entry were not.
func TestFieldMetadataTextIsBounded(t *testing.T) {
	s := openTestStore(t)
	long := strings.Repeat("d", maxMetaTextBytes+1)
	for _, m := range []wire.FieldMeta{
		{Set: "app", Field: "f", Unit: long},
		{Set: "app", Field: "f", UnitHint: long},
		{Set: "app", Field: "f", Description: long},
	} {
		_, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{m}}, "", "")
		var bad *ErrBadRequest
		if !errors.As(err, &bad) {
			t.Errorf("%d bytes of free text were accepted: err = %v", len(long), err)
		}
	}
	// Anything a real declaration carries still lands.
	if _, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{
		{Set: "app", Field: "f", Unit: "bytes", UnitHint: "bytes", Description: "requests served"},
	}}, "", ""); err != nil {
		t.Fatalf("ordinary metadata was refused: %v", err)
	}
}
