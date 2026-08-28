package mql

import (
	"encoding/json"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
)

func mustParse(t *testing.T, src string) *Query {
	t.Helper()
	q, err := Parse(src)
	if err != nil {
		t.Fatalf("parse %q: %v", src, err)
	}
	return q
}

func TestParseFullQuery(t *testing.T) {
	q := mustParse(t, `
FROM http
SELECT requests_total RATE GAP 30s AS "req/s",
       errors_total DELTA CLAMP MIN 0 ELSE RAW SSE REPEAT,
       inflight
WHERE  host IN ($host) AND dc = "eu-west-1" AND HAS requests_total
BY     host, dc
EVERY  1m
FORMAT timeseries
LIMIT  SERIES 500
-- trailing comment
`)
	if q.From != "http" || len(q.Select) != 3 {
		t.Fatalf("unexpected query: %+v", q)
	}
	if !q.Select[0].Modifiers.Delta || !q.Select[0].Modifiers.PerSecond {
		t.Fatalf("RATE must set both delta and per-second")
	}
	if got := *q.Select[0].Modifiers.GapMs; got != 30_000 {
		t.Fatalf("GAP 30s should be 30000ms, got %d", got)
	}
	if q.Select[1].Modifiers.Clamp.Else != "raw" || *q.Select[1].Modifiers.Clamp.Min != 0 {
		t.Fatalf("clamp not parsed: %+v", q.Select[1].Modifiers.Clamp)
	}
	if q.Select[1].Modifiers.SSE.Mode != "repeat" {
		t.Fatalf("SSE REPEAT not parsed")
	}
	if *q.EveryMs != 60_000 || *q.Limits.Series != 500 {
		t.Fatalf("EVERY/LIMIT not parsed: %+v", q)
	}
	if len(q.By) != 2 {
		t.Fatalf("BY not parsed: %+v", q.By)
	}
	if vars := Variables(q); len(vars) != 1 || vars[0] != "host" {
		t.Fatalf("expected the host variable, got %v", vars)
	}
}

// The AST is canonical, so text must survive a round trip through it.
func TestRoundTrip(t *testing.T) {
	cases := []string{
		`FROM cpu SELECT total_pct`,
		`FROM http SELECT requests_total RATE AS "req/s" WHERE host = "web1"`,
		`FROM http SELECT a DELTA, b PER SECOND, c NEGATE SSE OFF`,
		`FROM http SELECT a CLAMP MIN 0, MAX 100 ELSE RAW REQUIRED`,
		`FROM s SELECT v WHERE (a = "1" OR b = "2") AND NOT c =~ /^x.*/`,
		`FROM s SELECT v WHERE MISSING host OR HAS v`,
		`FROM s SELECT v BY host, dc EVERY 90s FORMAT table LIMIT POINTS 50`,
		`FROM lat SELECT HISTOGRAM(hdr24) AS "latency" FORMAT heatmap`,
		`LABELS host WHERE dc = "eu1"`,
		`FIELDS FROM http`,
		`LABEL KEYS FROM http`,
		`SETS`,
		`FROM s SELECT "select" AS "weird name" WHERE "from" = "x"`,
	}
	for _, src := range cases {
		q1 := mustParse(t, src)
		printed := Print(q1)
		q2, err := Parse(printed)
		if err != nil {
			t.Fatalf("reparse of %q (printed from %q): %v", printed, src, err)
		}
		if Print(q2) != printed {
			t.Fatalf("printing is not idempotent:\nfirst:  %q\nsecond: %q", printed, Print(q2))
		}
		j1, _ := json.Marshal(q1)
		j2, _ := json.Marshal(q2)
		if string(j1) != string(j2) {
			t.Fatalf("AST changed across a round trip:\n%s\n%s", j1, j2)
		}
	}
}

// Modifier order must not change meaning: the printer normalises it.
func TestModifierOrderIsCanonical(t *testing.T) {
	a := mustParse(t, `FROM s SELECT v PER SECOND DELTA`)
	b := mustParse(t, `FROM s SELECT v RATE`)
	if Print(a) != Print(b) {
		t.Fatalf("expected canonical order to collapse both forms:\n%q\n%q", Print(a), Print(b))
	}
}

func TestDuplicateModifierRejected(t *testing.T) {
	if _, err := Parse(`FROM s SELECT v DELTA DELTA`); err == nil {
		t.Fatal("expected a duplicate-modifier error")
	}
}

func TestParseErrorsCarryPosition(t *testing.T) {
	for _, src := range []string{
		`FROM`,
		`FROM s`,
		`FROM s SELECT`,
		`FROM s SELECT v WHERE host`,
		`FROM s SELECT v WHERE host = `,
		`FROM s SELECT v FORMAT wat`,
		`FROM s SELECT v EVERY 5`,
		`FROM s SELECT v GAP`,
		`FROM s SELECT v CLAMP ELSE RAW`,
		`SELECT v FROM s`,
	} {
		if _, err := Parse(src); err == nil {
			t.Fatalf("expected %q to fail", src)
		}
	}
}

type testSchema struct{}

func (testSchema) HasSet(s string) bool { return s == "http" }
func (testSchema) Sets() []string       { return []string{"http"} }
func (testSchema) Field(set, f string) (FieldInfo, bool) {
	switch f {
	case "requests_total":
		return FieldInfo{Kind: model.KindCounter, MaxInterval: 30_000}, true
	case "inflight":
		return FieldInfo{Kind: model.KindGauge, MaxInterval: 30_000}, true
	case "note":
		return FieldInfo{Kind: model.KindString}, true
	}
	return FieldInfo{}, false
}
func (testSchema) HasLabel(set, k string) bool { return k == "host" || k == "dc" }
func (testSchema) BucketSet(set, n string) ([]string, bool) {
	if n == "hdr24" {
		return []string{"00", "01"}, true
	}
	return nil, false
}

func TestValidate(t *testing.T) {
	s := testSchema{}

	// A counter plotted raw warns but runs.
	w, err := Validate(mustParse(t, `FROM http SELECT requests_total`), s, 1000, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasCode(w, "W102") {
		t.Fatalf("expected W102, got %+v", w)
	}

	// Unknown set is fatal and names the alternatives.
	if _, err := Validate(mustParse(t, `FROM nope SELECT x`), s, 1000, 10000); err == nil {
		t.Fatal("expected E002")
	}
	// Unknown label in WHERE is fatal.
	if _, err := Validate(mustParse(t, `FROM http SELECT inflight WHERE nope = "x"`), s, 1000, 10000); err == nil {
		t.Fatal("expected E004")
	}
	// A required missing field is fatal; a non-required one warns.
	if _, err := Validate(mustParse(t, `FROM http SELECT nothere REQUIRED`), s, 1000, 10000); err == nil {
		t.Fatal("expected E003")
	}
	if _, err := Validate(mustParse(t, `FROM http SELECT nothere`), s, 1000, 10000); err != nil {
		t.Fatalf("a missing non-required field should only warn: %v", err)
	}
	// Numeric modifiers on a string field are fatal.
	if _, err := Validate(mustParse(t, `FROM http SELECT note DELTA`), s, 1000, 10000); err == nil {
		t.Fatal("expected E005")
	}
	// Limits may only narrow.
	if _, err := Validate(mustParse(t, `FROM http SELECT inflight LIMIT SERIES 5000`), s, 1000, 10000); err == nil {
		t.Fatal("expected E007")
	}
	// Heatmap needs a histogram.
	if _, err := Validate(mustParse(t, `FROM http SELECT inflight FORMAT heatmap`), s, 1000, 10000); err == nil {
		t.Fatal("expected E008")
	}
	// Unknown bucket set is fatal.
	if _, err := Validate(mustParse(t, `FROM http SELECT HISTOGRAM(nope) FORMAT heatmap`), s, 1000, 10000); err == nil {
		t.Fatal("expected E009")
	}
	// Duplicate display names are fatal: two series cannot share a legend.
	if _, err := Validate(mustParse(t, `FROM http SELECT inflight AS "x", requests_total AS "x"`), s, 1000, 10000); err == nil {
		t.Fatal("expected E006")
	}
}

func hasCode(ds []Diag, code string) bool {
	for _, d := range ds {
		if d.Code == code {
			return true
		}
	}
	return false
}

func TestParseDuration(t *testing.T) {
	for in, want := range map[string]int64{
		"500ms": 500, "30s": 30_000, "5m": 300_000, "2h": 7_200_000, "1d": 86_400_000,
	} {
		got, err := ParseDuration(in)
		if err != nil || got != want {
			t.Fatalf("%q: got %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := ParseDuration("1m30s"); err == nil {
		t.Fatal("compound durations must be rejected")
	}
}
