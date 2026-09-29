package mql

import (
	"fmt"
	"strings"
	"testing"
)

// widthSchema knows one set with one field and one label, so a query that
// names anything else is still shape-checked rather than short-circuited
// by an unknown set.
type widthSchema struct{}

func (widthSchema) HasSet(set string) bool { return set == "app" }
func (widthSchema) Sets() []string         { return []string{"app"} }
func (widthSchema) Field(set, field string) (FieldInfo, bool) {
	return FieldInfo{}, set == "app" && field == "cpu"
}
func (widthSchema) HasLabel(set, key string) bool { return set == "app" && key == "host" }
func (widthSchema) BucketSet(string, string) ([]string, bool) {
	return nil, false
}

// The width of a query is bounded, as its depth is.
//
// Every selected field is read off every scanned row and every BY slot is
// resolved and length-prefixed into that row's grouping key, and neither
// cost is covered by the datasource ceilings: a field the catalogue does
// not carry yields no datapoint and opens no series, so
// max_datapoints_received and max_series_per_graph both watch a counter
// that never moves. An AST arrives as JSON, so a body inside
// max_request_bytes can name a million of them.
func TestASelectWiderThanTheLimitIsRefused(t *testing.T) {
	q := &Query{From: "app"}
	for i := 0; i <= MaxSelectFields; i++ {
		q.Select = append(q.Select, FieldExpr{Field: "f", As: itoaField(i)})
	}
	_, err := Validate(q, widthSchema{}, 0, 0)
	if err == nil {
		t.Fatal("a query selecting more fields than the limit was accepted")
	}
	var d Diag
	if !asDiagnostic(err, &d) || d.Code != "E007" {
		t.Fatalf("%v is not an E007", err)
	}
	if !strings.Contains(d.Msg, "row") {
		t.Errorf("%q does not say why the width matters", d.Msg)
	}
}

func TestASelectAtTheLimitIsAccepted(t *testing.T) {
	q := &Query{From: "app"}
	for i := 0; i < MaxSelectFields; i++ {
		q.Select = append(q.Select, FieldExpr{Field: "f", As: itoaField(i)})
	}
	if _, err := Validate(q, widthSchema{}, 0, 0); err != nil {
		t.Fatalf("a query exactly at the limit was refused: %v", err)
	}
}

func TestAGroupingWiderThanTheLimitIsRefused(t *testing.T) {
	q := &Query{From: "app", Select: []FieldExpr{{Field: "cpu"}}}
	for i := 0; i <= MaxByLabels; i++ {
		q.By = append(q.By, "host")
	}
	_, err := Validate(q, widthSchema{}, 0, 0)
	if err == nil {
		t.Fatal("a query grouping by more labels than the limit was accepted")
	}
	var d Diag
	if !asDiagnostic(err, &d) || d.Code != "E007" {
		t.Fatalf("%v is not an E007", err)
	}
}

// An ordinary query is nowhere near either bound.
func TestAnOrdinaryQueryIsNotBoundedAway(t *testing.T) {
	q, err := Parse(`FROM app SELECT cpu BY host`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := Validate(q, widthSchema{}, 0, 0); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func itoaField(n int) string {
	const digits = "0123456789"
	if n == 0 {
		return "f0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{digits[n%10]}, b...)
		n /= 10
	}
	return "f" + string(b)
}

// The "no such set" diagnostic is bounded.
//
// The store allows ten thousand sets of up to 128 characters, so a
// mistyped FROM answered 400 with over a megabyte of set names -- which
// the plugin then renders as a panel-level error string. The list exists
// so an operator can spot the typo, and that takes a handful of names.
func TestUnknownSetDiagnosticIsBounded(t *testing.T) {
	sets := make([]string, 0, 3000)
	for i := 0; i < 3000; i++ {
		sets = append(sets, strings.Repeat("s", 120)+fmt.Sprint(i))
	}
	got := namedSets(sets)
	if len(got) > maxNamedSets*160 {
		t.Fatalf("the known-set list is %d bytes for %d sets", len(got), len(sets))
	}
	if !strings.Contains(got, "and 2950 more") {
		t.Fatalf("the remainder is not reported: %q", got[len(got)-40:])
	}
	if namedSets(nil) != "(none)" {
		t.Fatalf("an empty store should say so, got %q", namedSets(nil))
	}
	if got := namedSets([]string{"a", "b"}); got != "a, b" {
		t.Fatalf("a short list must be listed in full, got %q", got)
	}
}
