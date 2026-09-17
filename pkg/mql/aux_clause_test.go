package mql

import (
	"encoding/json"
	"testing"
)

// The auxiliary query kinds each read a fixed handful of fields, and the
// executor ignores the rest while Print emits only what the grammar has a
// place for. A clause they do not read therefore did nothing *and* did
// not survive a round trip through the query's own canonical text, which
// is the property every other empty-node refusal in this package keeps.
func TestAuxiliaryKindsRefuseClausesTheyDoNotRead(t *testing.T) {
	ptrInt := func(n int) *int { return &n }
	ptrMs := func(n int64) *int64 { return &n }
	cases := []struct {
		name string
		q    Query
		want string
	}{
		{"fields with where", Query{Kind: KindFields, From: "app",
			Where: Expr{Eq: &Compare{Label: "host", Value: "a"}}}, "WHERE"},
		{"fields with by", Query{Kind: KindFields, From: "app", By: []string{"host"}}, "BY"},
		{"fields with select", Query{Kind: KindFields, From: "app",
			Select: []FieldExpr{{Field: "cpu"}}}, "SELECT"},
		{"label keys with limit", Query{Kind: KindLabelKeys, From: "app",
			Limits: Limits{Points: ptrInt(10)}}, "LIMIT POINTS"},
		{"sets with from", Query{Kind: KindSets, From: "app"}, "FROM"},
		{"labels with from", Query{Kind: KindLabels, Label: "host", From: "app"}, "store-wide"},
		{"labels with every", Query{Kind: KindLabels, Label: "host",
			EveryMs: ptrMs(1000)}, "EVERY"},
		{"labels with format", Query{Kind: KindLabels, Label: "host",
			Format: FormatTable}, "FORMAT"},
		{"sets with a label key", Query{Kind: KindSets, Label: "host"}, "label key"},
	}
	for _, c := range cases {
		q := c.q
		_, err := Validate(&q, nil, 0, 0)
		if err == nil {
			t.Errorf("%s: accepted", c.name)
			continue
		}
		d, ok := err.(Diag)
		if !ok || d.Code != "E008" {
			t.Errorf("%s: want E008, got %v", c.name, err)
			continue
		}
		if !contains(d.Msg, c.want) {
			t.Errorf("%s: message does not name the clause (%q): %s", c.name, c.want, d.Msg)
		}
	}
}

// The shapes the grammar really produces still validate, including the
// default Kind and Format that Query.MarshalJSON writes into every AST
// that has been through JSON.
func TestAuxiliaryKindsAcceptWhatTheParserBuilds(t *testing.T) {
	for _, text := range []string{
		"SETS",
		"FIELDS FROM app",
		"LABEL KEYS FROM app",
		"LABELS host",
		`LABELS host WHERE dc = "eu-west-1"`,
	} {
		q, err := Parse(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		if _, err := Validate(q, nil, 0, 0); err != nil {
			t.Errorf("%s: refused after parsing: %v", text, err)
		}
		// Through JSON, which is how a stored panel reaches the store:
		// MarshalJSON fills in Kind and Format, and neither is a clause.
		b, err := json.Marshal(q)
		if err != nil {
			t.Fatal(err)
		}
		var back Query
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatal(err)
		}
		if _, err := Validate(&back, nil, 0, 0); err != nil {
			t.Errorf("%s: refused after a JSON round trip (%s): %v", text, b, err)
		}
		// The canonical text reparses to the same AST, which is the
		// round trip these refusals exist to keep.
		again, err := Parse(Print(&back))
		if err != nil {
			t.Errorf("%s: printed back as text that will not parse (%q): %v", text, Print(&back), err)
			continue
		}
		if again.Kind != back.Kind || again.From != back.From || again.Label != back.Label ||
			Print(again) != Print(&back) {
			t.Errorf("%s: did not round trip: %q vs %q", text, Print(&back), Print(again))
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
