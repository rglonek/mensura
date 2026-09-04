package mql

import (
	"encoding/json"
	"strings"
	"testing"
)

// An IN clause with no values is refused rather than folded away.
//
// It used to validate clean. The store's lowering turns it into a constant
// false with no diagnostic, so the panel came back empty with nothing at
// all saying why -- the exact failure this validator exists to prevent --
// and Print emitted `host IN ()`, which does not parse, so the AST and its
// canonical text stopped round-tripping.
func TestEmptyInListIsRefused(t *testing.T) {
	raw := `{"kind":"query","from":"app","select":[{"field":"req","modifiers":{}}],` +
		`"where":{"in":{"label":"host","values":[]}}}`
	var q Query
	if err := json.Unmarshal([]byte(raw), &q); err != nil {
		t.Fatalf("decode: %v", err)
	}
	_, err := Validate(&q, nil, 0, 0)
	if err == nil {
		t.Fatal("an empty IN list validated; it can never match and its text does not parse")
	}
	var d Diag
	if !asDiagnostic(err, &d) || d.Code != "E007" {
		t.Fatalf("expected E007, got %v", err)
	}
	if !strings.Contains(d.Msg, "host") {
		t.Fatalf("the diagnostic does not name the label: %s", d.Msg)
	}
}

// The same node nested inside a compound predicate, which is where a
// builder is most likely to produce one.
func TestEmptyInListIsRefusedInsideAnAndArm(t *testing.T) {
	raw := `{"kind":"query","from":"app","select":[{"field":"req","modifiers":{}}],` +
		`"where":{"and":[{"eq":{"label":"dc","value":"eu"}},{"in":{"label":"host","values":[]}}]}}`
	var q Query
	if err := json.Unmarshal([]byte(raw), &q); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, err := Validate(&q, nil, 0, 0); err == nil {
		t.Fatal("an empty IN list nested in an AND validated")
	}
}

// A list with values is still accepted, and still round-trips.
func TestPopulatedInListStillRoundTrips(t *testing.T) {
	q, err := Parse(`FROM app SELECT req WHERE host IN ("a", "b")`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := Validate(q, nil, 0, 0); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if _, err := Parse(Print(q)); err != nil {
		t.Fatalf("round trip: %v", err)
	}
}

func asDiagnostic(err error, out *Diag) bool {
	d, ok := err.(Diag)
	if ok {
		*out = d
	}
	return ok
}
