package mql

import (
	"encoding/json"
	"testing"
)

// nestedAnd builds a predicate n levels deep, which is the shape a JSON
// body can carry up to encoding/json's own nesting limit.
func nestedAnd(n int) Expr {
	e := Expr{Eq: &Compare{Label: "host", Value: "a"}}
	for i := 0; i < n; i++ {
		e = Expr{And: []Expr{e}}
	}
	return e
}

// Print returns a string rather than an error, so it was the one entry
// point in this package that would walk a predicate deeper than both the
// parser and the validator refuse -- rebuilding the whole sub-expression
// at every level. CheckPredicateDepth is what the print endpoints gate on.
func TestCheckPredicateDepthRefusesWhatParseRefuses(t *testing.T) {
	deep := &Query{Kind: KindQuery, From: "a", Select: []FieldExpr{{Field: "b"}}, Where: nestedAnd(MaxPredicateDepth + 1)}
	err := CheckPredicateDepth(deep)
	if err == nil {
		t.Fatal("a predicate past MaxPredicateDepth was accepted for printing")
	}
	var d Diag
	if !asDiagForTest(err, &d) || d.Code != "E001" {
		t.Fatalf("want an E001 diagnostic, got %v", err)
	}
	// And Validate, which is the gate for anything that executes, agrees.
	if _, verr := Validate(deep, nil, 0, 0); verr == nil {
		t.Fatal("Validate accepted the same predicate")
	}
}

// A shallow predicate, and a half-built query with no SELECT at all, must
// still print: the builder round-trips one on every edit.
func TestCheckPredicateDepthAcceptsOrdinaryQueries(t *testing.T) {
	for _, q := range []*Query{
		{Kind: KindQuery, From: "a", Select: []FieldExpr{{Field: "b"}}, Where: nestedAnd(MaxPredicateDepth - 1)},
		{Kind: KindQuery, From: "a"},
		{},
	} {
		if err := CheckPredicateDepth(q); err != nil {
			t.Fatalf("CheckPredicateDepth refused an ordinary query: %v", err)
		}
	}
}

// The bound has to survive the JSON surface, which is how an AST really
// arrives.
func TestCheckPredicateDepthOverJSON(t *testing.T) {
	raw, err := json.Marshal(&Query{Kind: KindQuery, From: "a", Select: []FieldExpr{{Field: "b"}}, Where: nestedAnd(500)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var q Query
	if err := json.Unmarshal(raw, &q); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := CheckPredicateDepth(&q); err == nil {
		t.Fatal("a 500-deep predicate decoded from JSON was accepted for printing")
	}
}

func asDiagForTest(err error, out *Diag) bool {
	d, ok := err.(Diag)
	if ok {
		*out = d
	}
	return ok
}
