package mql

import (
	"strings"
	"testing"
)

type nodeBoundSchema struct{}

func (nodeBoundSchema) HasSet(string) bool { return true }
func (nodeBoundSchema) Sets() []string     { return []string{"a"} }
func (nodeBoundSchema) Field(string, string) (FieldInfo, bool) {
	return FieldInfo{Kind: "gauge", MaxInterval: 1000}, true
}
func (nodeBoundSchema) HasLabel(string, string) bool              { return true }
func (nodeBoundSchema) BucketSet(string, string) ([]string, bool) { return nil, false }

// The text surface and the AST surface are the same language, so a
// predicate the parser accepts is one Validate accepts.
//
// The parser used to charge a node per parseUnary entry, which counts the
// arms of an AND chain and not the node holding them: at exactly
// MaxPredicateNodes clauses Parse succeeded and Validate answered E007.
func TestParseAndValidateAgreeOnPredicateSize(t *testing.T) {
	for _, n := range []int{1, 2, MaxPredicateNodes - 2, MaxPredicateNodes - 1, MaxPredicateNodes, MaxPredicateNodes + 1} {
		clauses := make([]string, n)
		for i := range clauses {
			clauses[i] = `h = "x"`
		}
		src := `FROM a SELECT f WHERE ` + strings.Join(clauses, " AND ")
		q, perr := Parse(src)
		if perr != nil {
			continue // refused by the parser; Validate never sees it
		}
		if _, verr := Validate(q, nodeBoundSchema{}, 0, 0); verr != nil {
			t.Fatalf("%d clauses: the parser accepted a predicate Validate refuses: %v", n, verr)
		}
	}
}

// And the bound is still enforced: a predicate past it is refused by the
// parser rather than reaching the planner.
func TestPredicateSizeStillBounded(t *testing.T) {
	clauses := make([]string, MaxPredicateNodes+8)
	for i := range clauses {
		clauses[i] = `h = "x"`
	}
	if _, err := Parse(`FROM a SELECT f WHERE ` + strings.Join(clauses, " AND ")); err == nil {
		t.Fatal("a predicate past MaxPredicateNodes parsed cleanly")
	}
}

// A parenthesised group builds no Expr of its own, so it must not be
// charged a node the AST checker will not charge.
func TestParenGroupIsNotCountedAsANode(t *testing.T) {
	// One AND chain of n clauses, each wrapped in its own group: the AST
	// holds n + 1 nodes however many parentheses were written.
	n := MaxPredicateNodes - 1
	clauses := make([]string, n)
	for i := range clauses {
		clauses[i] = `(h = "x")`
	}
	q, err := Parse(`FROM a SELECT f WHERE ` + strings.Join(clauses, " AND "))
	if err != nil {
		t.Fatalf("%d parenthesised clauses were refused: %v", n, err)
	}
	if _, verr := Validate(q, nodeBoundSchema{}, 0, 0); verr != nil {
		t.Fatalf("validate: %v", verr)
	}
}
