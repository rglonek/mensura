package mql

import (
	"strings"
	"testing"
)

// The bound list is comma-separated, and the comma is what tells a second
// bound apart from the next SELECT field. Writing the bounds without it is
// the ordinary typo, and the parser used to answer `unexpected "MAX" after
// end of query` -- at a position past the whole SELECT list, naming
// neither the clause nor the fix.
func TestClampWithoutACommaNamesTheClause(t *testing.T) {
	_, err := Parse(`FROM app SELECT cpu CLAMP MIN 0 MAX 100`)
	if err == nil {
		t.Fatal("a comma-less bound list parsed")
	}
	if !strings.Contains(err.Error(), "CLAMP bounds are separated by a comma") {
		t.Fatalf("error %q does not name the clause or the fix", err)
	}
	// The same for the other order, and with a following field.
	_, err = Parse(`FROM app SELECT cpu CLAMP MAX 100 MIN 0, mem`)
	if err == nil || !strings.Contains(err.Error(), "separated by a comma") {
		t.Fatalf("error %v", err)
	}
}

// The documented spellings still parse, and still mean what they say.
func TestClampCommaFormsStillParse(t *testing.T) {
	for _, text := range []string{
		`FROM app SELECT cpu CLAMP MIN 0, MAX 100`,
		`FROM app SELECT cpu CLAMP MIN 0, MAX 100 ELSE RAW`,
		`FROM app SELECT cpu CLAMP MIN 0`,
		`FROM app SELECT cpu CLAMP MAX 100, mem`,
		`FROM app SELECT cpu CLAMP MIN 0, mem`,
	} {
		q, err := Parse(text)
		if err != nil {
			t.Fatalf("%q: %v", text, err)
		}
		if got := Print(q); Print(mustReparse(t, got)) != got {
			t.Fatalf("%q does not round-trip", text)
		}
	}
	// Two fields, not one bound list.
	q, err := Parse(`FROM app SELECT cpu CLAMP MIN 0, mem`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Select) != 2 {
		t.Fatalf("selected %d fields, want 2", len(q.Select))
	}
	// One field with two bounds.
	q, err = Parse(`FROM app SELECT cpu CLAMP MIN 0, MAX 100`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Select) != 1 || q.Select[0].Modifiers.Clamp == nil ||
		q.Select[0].Modifiers.Clamp.Min == nil || q.Select[0].Modifiers.Clamp.Max == nil {
		t.Fatalf("want one field with both bounds, got %+v", q.Select)
	}
}

func mustReparse(t *testing.T, text string) *Query {
	t.Helper()
	q, err := Parse(text)
	if err != nil {
		t.Fatalf("reparse %q: %v", text, err)
	}
	return q
}
