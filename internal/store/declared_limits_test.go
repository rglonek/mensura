package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// A field's declared `limits:` become the query layer's default clamp, and
// that default carried ELSE RAW unconditionally -- which substitutes the
// pre-transform sample.
//
// Without DELTA there is no transform: stage 3 of the walk sets val = raw
// and the only stages between it and the clamp are DELTA and NEGATE, and
// NEGATE already withholds the default. So the clamp replaced every
// out-of-range value with itself: a declaration the store takes, persists
// and reports in the plan, and then provably does not act on. The example
// in 03-extraction.md is exactly this shape -- a gauge declaring
// `limits: {min: 0, max: 100}` -- and a source reporting 150 drew 150.
func TestDeclaredLimitsBoundAGauge(t *testing.T) {
	s := openTestStore(t)
	rows := []model.Sample{
		{TSMs: base(), Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"cpu_pct": model.Float(150)}},
		{TSMs: base() + 1000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"cpu_pct": model.Float(-20)}},
		{TSMs: base() + 2000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"cpu_pct": model.Float(42)}},
	}
	writeSamples(t, s, "app", rows, wire.FieldMeta{
		Set: "app", Field: "cpu_pct", Kind: model.KindGauge,
		LimitMin: floatPtr(0), LimitMax: floatPtr(100),
	})
	got := queryValues(t, s, `FROM app SELECT cpu_pct BY host`)
	want := []float64{100, 0, 42}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("point %d = %g, want %g (the declared limits bounded nothing): %v", i, got[i], want[i], got)
		}
	}
}

// The escape hatch is still installed where it means something: a counter
// reset makes one difference briefly negative, and the raw counter is the
// interpretable answer there.
func TestDeclaredLimitsKeepTheHatchUnderDelta(t *testing.T) {
	s := openTestStore(t)
	// 1000, 1100, then a reset to 5: the delta is -1095.
	rows := []model.Sample{
		{TSMs: base(), Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"tx": model.Float(1000)}},
		{TSMs: base() + 1000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"tx": model.Float(1100)}},
		{TSMs: base() + 2000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"tx": model.Float(5)}},
	}
	writeSamples(t, s, "app", rows, wire.FieldMeta{
		Set: "app", Field: "tx", Kind: model.KindCounter, LimitMin: floatPtr(0),
	})
	got := queryValues(t, s, `FROM app SELECT tx DELTA BY host`)
	want := []float64{100, 5}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("point %d = %g, want %g: the counter-reset hatch is gone from the case it exists for: %v", i, got[i], want[i], got)
		}
	}
	// Explain has to report the escape hatch exactly where the executor
	// installs it, and not where it does not.
	if raw := explainElseRaw(t, s, `FROM app SELECT tx DELTA`); raw != true {
		t.Fatalf("explain says clamp_else_raw=%v under DELTA", raw)
	}
	if raw := explainElseRaw(t, s, `FROM app SELECT tx`); raw != false {
		t.Fatalf("explain says clamp_else_raw=%v with no transform, where it is a no-op", raw)
	}
}

// An explicit CLAMP still wins over the declaration, in both directions.
func TestExplicitClampStillWins(t *testing.T) {
	s := openTestStore(t)
	rows := []model.Sample{
		{TSMs: base(), Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"cpu_pct": model.Float(150)}},
		{TSMs: base() + 1000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"cpu_pct": model.Float(150)}},
	}
	writeSamples(t, s, "app", rows, wire.FieldMeta{
		Set: "app", Field: "cpu_pct", Kind: model.KindGauge,
		LimitMin: floatPtr(0), LimitMax: floatPtr(100),
	})
	for _, v := range queryValues(t, s, `FROM app SELECT cpu_pct CLAMP MAX 120 ELSE RAW BY host`) {
		if v != 150 {
			t.Fatalf("an explicit ELSE RAW drew %g, want the raw 150", v)
		}
	}
	for _, v := range queryValues(t, s, `FROM app SELECT cpu_pct CLAMP MAX 120 BY host`) {
		if v != 120 {
			t.Fatalf("an explicit CLAMP drew %g, want 120", v)
		}
	}
}

// A series that emits nothing still answers with arrays. DELTA over a
// single sample consumes it to seed the previous value, and the JSON tags
// carry no omitempty, so such a series used to travel as
// `"ts_ms": null, "values": null`.
func TestEmptySeriesIsAListNotNull(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "app", []model.Sample{
		{TSMs: base(), Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"tx": model.Int(7)}},
	})
	q, err := mql.Parse(`FROM app SELECT tx DELTA BY host`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: base() - 1000, ToMs: base() + 10_000, MaxPoints: 100,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Series) != 1 || len(resp.Series[0].TSMs) != 0 {
		t.Fatalf("expected one empty series, got %+v", resp.Series)
	}
	b, err := json.Marshal(resp.Series[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"ts_ms", "values"} {
		if string(decoded[key]) != "[]" {
			t.Fatalf("%s = %s, want []", key, decoded[key])
		}
	}
}

func queryValues(t *testing.T, s *Store, text string) []float64 {
	t.Helper()
	q, err := mql.Parse(text)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: base() - 1000, ToMs: base() + 10_000, MaxPoints: 1000, IntervalMs: 1,
	})
	if err != nil {
		t.Fatalf("query %q: %v", text, err)
	}
	if len(resp.Series) != 1 {
		t.Fatalf("%q: %d series, want 1", text, len(resp.Series))
	}
	return resp.Series[0].Values
}

func explainElseRaw(t *testing.T, s *Store, text string) any {
	t.Helper()
	q, err := mql.Parse(text)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	plan, err := s.Explain(q, &wire.QueryRequest{FromMs: base() - 1000, ToMs: base() + 10_000, MaxPoints: 100})
	if err != nil {
		t.Fatalf("explain %q: %v", text, err)
	}
	fields, _ := plan["resolved_fields"].([]map[string]any)
	if len(fields) != 1 {
		t.Fatalf("%q: resolved_fields = %v", text, plan["resolved_fields"])
	}
	return fields[0]["clamp_else_raw"]
}
