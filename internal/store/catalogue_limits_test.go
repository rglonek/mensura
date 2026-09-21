package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// A set name and a field name are chosen by the sender on every write
// path, and the catalogue that records them is one in-memory map and one
// JSON record on disk. Both used to grow without any bound at all, while
// max_label_keys and max_label_cardinality watched the two dimensions
// beside them.
func TestNewSetsAreBoundedOnTheWritePath(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxSets = 3
	s := openStore(t, cfg)

	for i := 0; i < 3; i++ {
		resp := writeOne(t, s, fmt.Sprintf("set%d", i), "v")
		if resp.Refused() != 0 {
			t.Fatalf("set%d refused: %v", i, resp.Rejected)
		}
	}
	resp := writeOne(t, s, "set3", "v")
	if resp.Refused() != 1 {
		t.Fatalf("a fourth set was accepted past a limit of 3: %+v", resp)
	}
	if !strings.Contains(resp.Rejected[0].Reason, "limit of 3") {
		t.Errorf("rejection does not name the limit: %s", resp.Rejected[0].Reason)
	}
	// A set the catalogue already holds still works, which is the rule
	// the label budgets follow.
	if resp := writeOne(t, s, "set0", "v"); resp.Refused() != 0 {
		t.Errorf("an existing set was refused: %v", resp.Rejected)
	}
}

func TestNewFieldsAreBoundedPerSet(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxFieldsPerSet = 2
	s := openStore(t, cfg)

	if resp := writeOne(t, s, "app", "a", "b"); resp.Refused() != 0 {
		t.Fatalf("two fields refused: %v", resp.Rejected)
	}
	resp := writeOne(t, s, "app", "c")
	if resp.Refused() != 1 {
		t.Fatalf("a third field was accepted past a limit of 2: %+v", resp)
	}
	if !strings.Contains(resp.Rejected[0].Reason, `"c"`) {
		t.Errorf("rejection does not name the field: %s", resp.Rejected[0].Reason)
	}
	if resp := writeOne(t, s, "app", "a"); resp.Refused() != 0 {
		t.Errorf("an existing field was refused: %v", resp.Rejected)
	}
}

// Declarations create catalogue entries too, so the cheapest way past the
// cap must not be the one that carries no data.
func TestDeclarationsAreBoundedToo(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxSets = 1
	cfg.MaxFieldsPerSet = 2
	s := openStore(t, cfg)

	_, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{
		{Set: "app", Field: "a", Kind: model.KindGauge},
		{Set: "app", Field: "b", Kind: model.KindGauge},
	}}, "", "")
	if err != nil {
		t.Fatalf("two declared fields refused: %v", err)
	}
	_, err = s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{
		{Set: "app", Field: "c", Kind: model.KindGauge},
	}}, "", "")
	var bad *ErrBadRequest
	if !errors.As(err, &bad) {
		t.Fatalf("a third declared field was accepted: %v", err)
	}
	_, err = s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{
		{Set: "other", KeyScheme: model.KeyContent},
	}}, "", "")
	if !errors.As(err, &bad) {
		t.Fatalf("a second declared set was accepted past a limit of 1: %v", err)
	}
	// And nothing of the refused request landed.
	for _, set := range s.Sets() {
		if set == "other" {
			t.Error("a refused set declaration was applied anyway")
		}
	}
}

// A refused whole request must not leave half of itself behind.
func TestARefusedDeclarationAppliesNothing(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxFieldsPerSet = 2
	s := openStore(t, cfg)

	_, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{
		{Set: "app", Field: "a", Kind: model.KindCounter},
		{Set: "app", Field: "b", Kind: model.KindCounter},
		{Set: "app", Field: "c", Kind: model.KindCounter},
	}}, "", "")
	var bad *ErrBadRequest
	if !errors.As(err, &bad) {
		t.Fatalf("want ErrBadRequest, got %v", err)
	}
	if _, known := s.Schema().Field("app", "a"); known {
		t.Error("the first field of a refused declaration was applied")
	}
}

func TestZeroDisablesTheCatalogueBudgets(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxSets = -1
	cfg.MaxFieldsPerSet = -1
	s := openStore(t, cfg)
	for i := 0; i < 40; i++ {
		if resp := writeOne(t, s, fmt.Sprintf("s%d", i), fmt.Sprintf("f%d", i)); resp.Refused() != 0 {
			t.Fatalf("a disabled budget refused a write: %v", resp.Rejected)
		}
	}
}

func writeOne(t *testing.T, s *Store, set string, fields ...string) *wire.WriteResponse {
	t.Helper()
	f := map[string]model.Value{}
	for _, name := range fields {
		f[name] = model.Int(1)
	}
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{
		Set:     set,
		Samples: []model.Sample{{TSMs: 1_700_000_000_000, Labels: map[string]string{"host": "h"}, Fields: f}},
	}}}, "", "")
	if err != nil {
		t.Fatalf("write %s: %v", set, err)
	}
	return resp
}

func testConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	return cfg
}

func openStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
