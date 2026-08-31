package mql

import (
	"strings"
	"testing"
)

// A limit may only narrow, and a negative one used to reach the tabular
// executor as a slice bound.
func TestNonPositiveLimitsRejected(t *testing.T) {
	for _, text := range []string{
		`FROM app SELECT v FORMAT table LIMIT POINTS -1`,
		`FROM app SELECT v FORMAT table LIMIT POINTS 0`,
		`FROM app SELECT v LIMIT SERIES -5`,
	} {
		q, err := Parse(text)
		if err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
		if _, err := Validate(q, nil, 0, 0); err == nil {
			t.Fatalf("%q was accepted", text)
		}
	}
	q, err := Parse(`FROM app SELECT v FORMAT table LIMIT POINTS 10`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Validate(q, nil, 0, 0); err != nil {
		t.Fatalf("a positive limit was rejected: %v", err)
	}
}

// The predicate JSON is a tagged union; a node setting two arms executed
// as only one of them.
func TestMultiArmPredicateRejected(t *testing.T) {
	q := &Query{
		Kind: KindQuery, From: "app", Select: []FieldExpr{{Field: "v"}},
		Where: Expr{
			And: []Expr{{Eq: &Compare{Label: "a", Value: "1"}}},
			Eq:  &Compare{Label: "b", Value: "2"},
		},
	}
	if _, err := Validate(q, nil, 0, 0); err == nil {
		t.Fatal("a predicate node with two arms was accepted")
	}
}

// Values the lexer cannot decode must not be printed with escapes it does
// not implement.
func TestPrintParseRoundTripsAwkwardValues(t *testing.T) {
	for _, v := range []string{
		"plain", "with space", "tab\there", "new\nline", `back\slash`,
		`quote"inside`, "ctrl\x01byte", "unicode-é", "FROM", "$notavar!",
	} {
		q := &Query{
			Kind: KindQuery, From: "app", Select: []FieldExpr{{Field: "v"}},
			Where: Expr{Eq: &Compare{Label: "host", Value: v}},
		}
		text := Print(q)
		back, err := Parse(text)
		if err != nil {
			t.Fatalf("value %q printed as %q, which does not parse: %v", v, text, err)
		}
		if back.Where.Eq == nil || back.Where.Eq.Value != v {
			got := "<nil>"
			if back.Where.Eq != nil {
				got = back.Where.Eq.Value
			}
			t.Fatalf("value %q round-tripped to %q via %q", v, got, text)
		}
	}
}

// A display name and an awkward identifier survive the same round trip.
func TestPrintParseRoundTripsNames(t *testing.T) {
	q := &Query{
		Kind: KindQuery, From: "app",
		Select: []FieldExpr{{Field: "00", As: `req "per" s`}},
	}
	back, err := Parse(Print(q))
	if err != nil {
		t.Fatalf("%q does not parse: %v", Print(q), err)
	}
	if back.Select[0].Field != "00" || back.Select[0].As != `req "per" s` {
		t.Fatalf("round trip lost the names: %+v", back.Select[0])
	}
}

// The heatmap executor renders one bucket set; accepting several and
// drawing one is a silently incomplete panel.
func TestHeatmapAcceptsOneHistogram(t *testing.T) {
	q := &Query{
		Kind: KindQuery, From: "h", Format: FormatHeatmap,
		Select: []FieldExpr{{Histogram: "a"}, {Histogram: "b"}},
	}
	_, err := Validate(q, nil, 0, 0)
	if err == nil || !strings.Contains(err.Error(), "one HISTOGRAM") {
		t.Fatalf("expected a single-histogram error, got %v", err)
	}
}

// LABELS needs a key, and its documented filter is shape-checked.
func TestLabelsQueryValidation(t *testing.T) {
	if _, err := Validate(&Query{Kind: KindLabels}, nil, 0, 0); err == nil {
		t.Fatal("LABELS with no key was accepted")
	}
	q, err := Parse(`LABELS host WHERE dc = "eu"`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Validate(q, nil, 0, 0); err != nil {
		t.Fatalf("a valid LABELS filter was rejected: %v", err)
	}
}
