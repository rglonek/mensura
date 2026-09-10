package mql

import (
	"strings"
	"testing"
)

// W101 is documented in 06-query.md sections 4.3 and 12: modifiers are a
// set, the engine applies them in one canonical order, and a query that
// writes them in a misleading order is linted. Nothing produced the code,
// and it cannot come from Validate: the order does not survive into the
// AST at all, so the lint has to be raised where the text is read.
func TestModifierOrderLint(t *testing.T) {
	cases := []struct {
		name string
		text string
		want bool
	}{
		{"canonical", `FROM app SELECT cpu RATE NEGATE CLAMP MIN 0 GAP 30s SSE OFF REQUIRED`, false},
		{"delta then per second", `FROM app SELECT cpu DELTA PER SECOND`, false},
		{"per second then delta", `FROM app SELECT cpu PER SECOND DELTA`, true},
		{"negate before rate", `FROM app SELECT cpu NEGATE RATE`, true},
		{"required first", `FROM app SELECT cpu REQUIRED GAP 30s`, true},
		{"clamp after gap", `FROM app SELECT cpu GAP 30s CLAMP MIN 0`, true},
		{"no modifiers", `FROM app SELECT cpu`, false},
		{"second field only", `FROM app SELECT cpu, mem SSE OFF GAP 5s`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, diags, err := ParseDiags(c.text)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got := false
			for _, d := range diags {
				if d.Code == "W101" {
					got = true
				}
			}
			if got != c.want {
				t.Fatalf("W101 = %v, want %v (diags %v)", got, c.want, diags)
			}
			// Whatever the order, the AST and the canonical text are the
			// same: the lint is advisory, never a semantic change.
			canon, err := Parse(Print(q))
			if err != nil {
				t.Fatalf("reparse %q: %v", Print(q), err)
			}
			if Print(canon) != Print(q) {
				t.Fatalf("printing is not idempotent: %q then %q", Print(q), Print(canon))
			}
		})
	}
}

// The lint names the field it is about, so a query with several of them
// says which one to rewrite.
func TestModifierOrderLintNamesTheField(t *testing.T) {
	_, diags, err := ParseDiags(`FROM app SELECT cpu RATE, mem SSE OFF GAP 5s AS "memory"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(diags) != 1 {
		t.Fatalf("got %d diagnostics, want 1: %v", len(diags), diags)
	}
	if !strings.Contains(diags[0].Msg, "memory") {
		t.Errorf("the lint does not name the field it is about: %q", diags[0].Msg)
	}
}

// Parse keeps its signature and its behaviour; ParseDiags is the form that
// carries the lint.
func TestParseStillReturnsTheSameAST(t *testing.T) {
	text := `FROM app SELECT cpu PER SECOND DELTA`
	a, err := Parse(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	b, _, err := ParseDiags(text)
	if err != nil {
		t.Fatalf("parse with diags: %v", err)
	}
	if Print(a) != Print(b) {
		t.Fatalf("Parse and ParseDiags disagree: %q vs %q", Print(a), Print(b))
	}
}
