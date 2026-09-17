package store

import (
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// resolveField installs a field's declared `limits:` as the default
// CLAMP ... ELSE RAW, and that default has now been the subject of two
// separate bugs -- a crossed pair on disk, and the interaction with
// NEGATE -- both of which drew the raw counter under a rate legend with
// no diagnostic anywhere. Explain is the endpoint whose whole job is to
// say what the executor will do, and it reported the escape-hatch flag
// without the bounds it applies to, so the clamp an operator was hunting
// for was invisible on the one screen built to show it.
func TestExplainReportsTheResolvedClamp(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.Write(&wire.WriteRequest{
		FieldMeta: []wire.FieldMeta{{
			Set: "app", Field: "tx", Kind: model.KindCounter,
			LimitMin: floatPtr(0), LimitMax: floatPtr(100),
		}},
		Batches: []model.Batch{{Set: "app", Samples: []model.Sample{{
			TSMs: 1700000000000, Labels: map[string]string{"host": "a"},
			Fields: map[string]model.Value{"tx": model.Int(1)},
		}}}},
	}, "", "test"); err != nil {
		t.Fatalf("write: %v", err)
	}

	q, err := mql.Parse(`FROM app SELECT tx RATE`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	plan, err := s.Explain(q, &wire.QueryRequest{FromMs: 0, ToMs: 1700000100000, MaxPoints: 100})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	fields, _ := plan["resolved_fields"].([]map[string]any)
	if len(fields) != 1 {
		t.Fatalf("resolved_fields = %v, want one entry", plan["resolved_fields"])
	}
	f := fields[0]
	if f["clamp_else_raw"] != true {
		t.Fatalf("the declared limits install the escape hatch; explain says %v", f["clamp_else_raw"])
	}
	if got, ok := f["clamp_min"].(float64); !ok || got != 0 {
		t.Fatalf("explain reports the escape hatch without the bound it escapes from: clamp_min = %v", f["clamp_min"])
	}
	if got, ok := f["clamp_max"].(float64); !ok || got != 100 {
		t.Fatalf("clamp_max = %v, want the declared 100", f["clamp_max"])
	}
	// The other two resolved modifiers Explain used to leave out.
	if f["sse"] != "const 0" {
		t.Fatalf("sse = %v, want the resolved default", f["sse"])
	}
	if f["required"] != false {
		t.Fatalf("required = %v, want false", f["required"])
	}

	// NEGATE withholds the default, and Explain has to say so rather than
	// reporting a clamp the executor will not install.
	neg, err := mql.Parse(`FROM app SELECT tx RATE NEGATE`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	plan, err = s.Explain(neg, &wire.QueryRequest{FromMs: 0, ToMs: 1700000100000, MaxPoints: 100})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	fields, _ = plan["resolved_fields"].([]map[string]any)
	if len(fields) != 1 {
		t.Fatalf("resolved_fields = %v, want one entry", plan["resolved_fields"])
	}
	if _, present := fields[0]["clamp_min"]; present {
		t.Fatalf("explain reports a clamp under NEGATE, where resolveField installs none: %v", fields[0])
	}
}
