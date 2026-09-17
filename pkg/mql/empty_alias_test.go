package mql

import "testing"

// `AS ""` parsed into an alias Print does not emit, so the AST and its
// canonical text stopped round-tripping -- the property every other
// empty-name refusal in this package exists to keep.
func TestEmptyDisplayNameIsRefused(t *testing.T) {
	if _, err := Parse(`FROM app SELECT cpu AS ""`); err == nil {
		t.Fatal("an empty display name was accepted")
	}
	q, err := Parse(`FROM app SELECT cpu AS "user cpu"`)
	if err != nil {
		t.Fatalf("a real display name was refused: %v", err)
	}
	if q.Select[0].As != "user cpu" {
		t.Errorf("As = %q", q.Select[0].As)
	}
	back, err := Parse(Print(q))
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if back.Select[0].As != q.Select[0].As {
		t.Errorf("round trip lost the alias: %q", back.Select[0].As)
	}
}
