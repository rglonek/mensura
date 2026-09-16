package mql

import (
	"strings"
	"testing"
)

// The AST holds one value per safety gate, so a repeated LIMIT clause kept
// whichever was written last and said nothing. 06-query.md section 12
// lists a duplicate clause within one query under E006.
func TestDuplicateLimitClauseIsRefused(t *testing.T) {
	for _, text := range []string{
		`FROM http SELECT inflight LIMIT SERIES 10, SERIES 5`,
		`FROM http SELECT inflight LIMIT POINTS 10, POINTS 5`,
		`FROM http SELECT inflight LIMIT POINTS 10, SERIES 4, POINTS 5`,
	} {
		_, err := Parse(text)
		if err == nil {
			t.Fatalf("%q was accepted; the later limit silently wins", text)
		}
		if !strings.Contains(err.Error(), "E006") {
			t.Fatalf("%q: expected E006, got %v", text, err)
		}
	}
	// One of each is still a query.
	if _, err := Parse(`FROM http SELECT inflight LIMIT SERIES 10, POINTS 5`); err != nil {
		t.Fatalf("one of each must still parse: %v", err)
	}
}

// A repeated grouping slot is the same grouping rendered twice: the
// legend joins the slots with " : ", so `BY host, host` drew every series
// as "web1 : web1 : cpu" while costing a second dictionary lookup on every
// scanned row.
func TestDuplicateByLabelIsRefused(t *testing.T) {
	q, err := Parse(`FROM http SELECT inflight BY host, host`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, verr := Validate(q, testSchema{}, 0, 0)
	if verr == nil {
		t.Fatal("BY host, host was accepted")
	}
	d, ok := verr.(Diag)
	if !ok || d.Code != "E006" {
		t.Fatalf("expected E006, got %v", verr)
	}
	// Two distinct labels still group.
	q2, err := Parse(`FROM http SELECT inflight BY host, dc`)
	if err != nil {
		t.Fatal(err)
	}
	if _, verr := Validate(q2, testSchema{}, 0, 0); verr != nil {
		t.Fatalf("two distinct labels must still validate: %v", verr)
	}
}
