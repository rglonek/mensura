package mql

import (
	"strings"
	"testing"
)

// A recursive-descent parser with no bound on its recursion is a remote
// kill switch, not a performance problem: Go's stack overflow is a fatal
// runtime error rather than a panic, so net/http's per-request recovery
// cannot catch it and the process that owns the data directory simply
// dies. `WHERE ((((…` with a million parentheses fits comfortably inside
// the store's default 32 MiB body limit and is exactly what
// POST /v1/parse accepts.
func TestDeeplyNestedPredicateIsRefusedNotCrashed(t *testing.T) {
	for _, n := range []int{MaxPredicateDepth + 1, 10_000, 200_000} {
		src := "FROM app SELECT cpu WHERE " + strings.Repeat("(", n) + `host = "a"` + strings.Repeat(")", n)
		q, err := Parse(src)
		if err == nil {
			t.Fatalf("%d nested groups: accepted, want a depth error", n)
		}
		if q != nil {
			t.Fatalf("%d nested groups: returned a query alongside the error", n)
		}
		if !strings.Contains(err.Error(), "nests deeper") {
			t.Fatalf("%d nested groups: %v, want the depth message", n, err)
		}
	}
}

// NOT is the other nesting construct and shares the bound.
func TestDeeplyNestedNotIsRefused(t *testing.T) {
	src := "FROM app SELECT cpu WHERE " + strings.Repeat("NOT ", 100_000) + `host = "a"`
	if _, err := Parse(src); err == nil || !strings.Contains(err.Error(), "nests deeper") {
		t.Fatalf("got %v, want a depth error", err)
	}
}

// A predicate at the limit still parses, so the bound is a guard rail and
// not a narrowing of the language.
func TestPredicateAtTheDepthLimitParses(t *testing.T) {
	n := MaxPredicateDepth - 1
	src := "FROM app SELECT cpu WHERE " + strings.Repeat("(", n) + `host = "a"` + strings.Repeat(")", n)
	if _, err := Parse(src); err != nil {
		t.Fatalf("depth %d: %v", n, err)
	}
}

// Lexing materialises a token per character before the parser sees any of
// them, so an over-long query is a memory amplifier even once the
// recursion is bounded.
func TestOverlongQueryIsRefused(t *testing.T) {
	src := "FROM app SELECT " + strings.Repeat("x", MaxQueryBytes)
	_, err := Parse(src)
	if err == nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("got %v, want a length error", err)
	}
	var pe *ParseError
	if !asParseErr(err, &pe) || pe.Pos != 0 {
		t.Fatalf("want a positioned parse error at 0, got %v", err)
	}
}

// The AST is the canonical form and arrives as JSON, whose decoder allows
// far deeper nesting than the printer, the store's lowering or the
// engine's evaluator are written for. Refusing it at the same depth the
// text grammar refuses keeps the two surfaces one language: an AST that
// validates is one Print -> Parse can round-trip.
func TestDeeplyNestedASTIsRefusedByValidate(t *testing.T) {
	inner := Expr{Eq: &Compare{Label: "host", Value: "a"}}
	for i := 0; i < MaxPredicateDepth+5; i++ {
		e := inner
		inner = Expr{Not: &e}
	}
	q := &Query{Kind: KindQuery, From: "app", Format: FormatTimeseries,
		Select: []FieldExpr{{Field: "cpu"}}, Where: inner}
	if _, err := Validate(q, nil, 0, 0); err == nil {
		t.Fatal("accepted an AST deeper than the parser can read back")
	}
	// And the same shape one level inside the bound is still accepted.
	shallow := Expr{Eq: &Compare{Label: "host", Value: "a"}}
	for i := 0; i < MaxPredicateDepth-2; i++ {
		e := shallow
		shallow = Expr{Not: &e}
	}
	q.Where = shallow
	if _, err := Validate(q, nil, 0, 0); err != nil {
		t.Fatalf("refused an AST inside the bound: %v", err)
	}
	if _, err := Parse(Print(q)); err != nil {
		t.Fatalf("an AST Validate accepts must print and reparse: %v", err)
	}
}

func asParseErr(err error, out **ParseError) bool {
	pe, ok := err.(*ParseError)
	if ok {
		*out = pe
	}
	return ok
}
