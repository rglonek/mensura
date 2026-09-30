package mql

import (
	"strings"
	"testing"
)

// A predicate is bounded in size as well as in depth. The store resolves
// every regex clause against the whole label-value dictionary before it
// reads a row, so the clause count is a multiplier on work nothing else
// gates -- and on /v1/query that lowering happens before the query takes
// an execution slot.
func TestPredicateSizeIsBounded(t *testing.T) {
	clauses := make([]string, 0, MaxPredicateNodes+10)
	for i := 0; i < MaxPredicateNodes+10; i++ {
		clauses = append(clauses, `host =~ /a/`)
	}
	text := "FROM app SELECT cpu WHERE " + strings.Join(clauses, " OR ")
	if _, err := Parse(text); err == nil {
		t.Fatal("a predicate past MaxPredicateNodes parsed")
	} else if !strings.Contains(err.Error(), "clauses") {
		t.Fatalf("unexpected error %v", err)
	}
}

// The AST surface is held to the same bound, because that is the one a
// stored panel travels on and the one with no MaxQueryBytes in front of it.
func TestWidePredicateASTIsRefused(t *testing.T) {
	arms := make([]Expr, 0, MaxPredicateNodes+10)
	for i := 0; i < MaxPredicateNodes+10; i++ {
		arms = append(arms, Expr{Match: &MatchExpr{Label: "host", Regex: "a"}})
	}
	q := &Query{From: "app", Select: []FieldExpr{{Field: "cpu"}}, Where: Expr{Or: arms}}
	if _, err := Validate(q, nil, 0, 0); err == nil {
		t.Fatal("Validate accepted a predicate past MaxPredicateNodes")
	}
	// And the print/explain surface, which does not go through Validate.
	if err := CheckPredicateDepth(q); err == nil {
		t.Fatal("CheckPredicateDepth accepted a predicate past MaxPredicateNodes")
	}
}

// A LABELS filter is a real predicate and is held to the bound too: it is
// validated against a nil schema, so nothing else in Validate looks at it.
func TestLabelsPredicateSizeIsBounded(t *testing.T) {
	arms := make([]Expr, 0, MaxPredicateNodes+10)
	for i := 0; i < MaxPredicateNodes+10; i++ {
		arms = append(arms, Expr{Match: &MatchExpr{Label: "host", Regex: "a"}})
	}
	q := &Query{Kind: KindLabels, Label: "host", Where: Expr{Or: arms}}
	if _, err := Validate(q, nil, 0, 0); err == nil {
		t.Fatal("Validate accepted a wide LABELS predicate")
	}
}

// Anything a dashboard plausibly emits still parses and validates.
func TestOrdinaryPredicateStillFits(t *testing.T) {
	clauses := make([]string, 0, 64)
	for i := 0; i < 64; i++ {
		clauses = append(clauses, `host = "web"`)
	}
	text := "FROM app SELECT cpu WHERE " + strings.Join(clauses, " OR ")
	if _, err := Parse(text); err != nil {
		t.Fatalf("an ordinary 64-clause predicate was refused: %v", err)
	}
}
