package store

import (
	"encoding/json"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// runTabular does no render walk at all, so the modifiers a plan resolves
// describe stages that never execute under FORMAT table or logs.
//
// Validate refuses an *explicit* DELTA, CLAMP, GAP or SSE there, but
// resolveField installs the catalogue's own defaults whatever the format
// asks for: a declared `max_interval` became gap_ms and a declared
// `limits:` pair became clamp_min/clamp_max. Explain then reported a
// clamp and a gap on a query that applies neither -- the same lie
// downsample_window was zeroed here to stop telling.
func TestExplainOmitsRenderStagesUnderTabularFormats(t *testing.T) {
	s := openTestStore(t)
	lo, hi := 0.0, 100.0
	writeSamples(t, s, "app", []model.Sample{{
		TSMs:   1_700_000_000_000,
		Labels: map[string]string{"host": "a"},
		Fields: map[string]model.Value{"cpu": model.Int(1)},
	}}, wire.FieldMeta{
		Set: "app", Field: "cpu", Kind: model.KindGauge,
		MaxIntervalMs: 15_000, LimitMin: &lo, LimitMax: &hi,
	})

	req := &wire.QueryRequest{FromMs: 1, ToMs: 1_700_000_001_000, MaxPoints: 100}
	renderStages := []string{"delta", "per_second", "negate", "gap_ms", "sse", "clamp_else_raw", "clamp_min", "clamp_max"}

	for _, text := range []string{
		"FROM app SELECT cpu FORMAT table",
		"FROM app SELECT cpu FORMAT logs",
	} {
		q, err := mql.Parse(text)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := s.Explain(q, req)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		fields, _ := plan["resolved_fields"].([]map[string]any)
		if len(fields) != 1 {
			b, _ := json.Marshal(plan)
			t.Fatalf("%s: want one resolved field, got %s", text, b)
		}
		for _, k := range renderStages {
			if _, reported := fields[0][k]; reported {
				t.Errorf("%s: Explain reports %q, which runTabular never applies", text, k)
			}
		}
		// What the plan really does carry stays reported.
		for _, k := range []string{"field", "as", "required"} {
			if _, reported := fields[0][k]; !reported {
				t.Errorf("%s: Explain dropped %q, which is part of the plan", text, k)
			}
		}
	}

	// A timeseries query still reports every stage, including the clamp
	// a declared `limits:` pair installs.
	q, err := mql.Parse("FROM app SELECT cpu")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.Explain(q, req)
	if err != nil {
		t.Fatal(err)
	}
	fields, _ := plan["resolved_fields"].([]map[string]any)
	if len(fields) != 1 {
		t.Fatalf("want one resolved field, got %d", len(fields))
	}
	for _, k := range renderStages {
		if _, reported := fields[0][k]; !reported {
			t.Errorf("a timeseries plan no longer reports %q", k)
		}
	}
}

// Lists on the wire are lists. Neither `sets` nor `labels` carries
// omitempty, so a store with no sets answered `"sets": null` and a set
// with no label key answered `"labels": null` -- and the plugin's GET
// /labels resource hands that straight to the query builder.
func TestCatalogueListsAreNeverNull(t *testing.T) {
	s := openTestStore(t)
	b, err := json.Marshal(s.Catalogue())
	if err != nil {
		t.Fatal(err)
	}
	if !jsonContains(b, `"sets":[]`) {
		t.Errorf("an empty catalogue does not render sets as a list: %s", b)
	}
	writeSamples(t, s, "app", []model.Sample{{
		TSMs: 1_700_000_000_000, Fields: map[string]model.Value{"x": model.Int(1)},
	}})
	b, err = json.Marshal(s.Catalogue())
	if err != nil {
		t.Fatal(err)
	}
	if jsonContains(b, `"labels":null`) {
		t.Errorf("a set with no labels renders them as null: %s", b)
	}
	if !jsonContains(b, `"labels":[]`) {
		t.Errorf("a set with no labels does not render them as a list: %s", b)
	}
}

func jsonContains(b []byte, sub string) bool {
	s := string(b)
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
