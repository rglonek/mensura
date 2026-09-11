package mql

import "testing"

// W102 recommends RATE, and E008 refuses RATE under FORMAT table or logs.
// The warning was not gated on the format, so every counter column of
// every table panel came back advising the one thing the same validator
// would then reject -- and a warning an operator cannot act on teaches
// them to ignore the ones that matter. W103 was already gated this way.
func TestCounterAdviceOnlyWhereItCanBeTaken(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{`FROM http SELECT requests_total`, true},
		{`FROM http SELECT requests_total FORMAT timeseries`, true},
		{`FROM http SELECT requests_total FORMAT table`, false},
		{`FROM http SELECT requests_total FORMAT logs`, false},
	}
	for _, c := range cases {
		q, err := Parse(c.text)
		if err != nil {
			t.Fatalf("%s: parse: %v", c.text, err)
		}
		warns, verr := Validate(q, testSchema{}, 0, 0)
		if verr != nil {
			t.Fatalf("%s: validate: %v", c.text, verr)
		}
		got := false
		for _, w := range warns {
			if w.Code == "W102" {
				got = true
			}
		}
		if got != c.want {
			t.Fatalf("%s: W102 = %v, want %v (warnings %v)", c.text, got, c.want, warns)
		}
	}
	// And the advice really is refused under those formats, which is why
	// offering it there was wrong.
	q, err := Parse(`FROM http SELECT requests_total RATE FORMAT table`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, verr := Validate(q, testSchema{}, 0, 0); verr == nil {
		t.Fatal("RATE under FORMAT table must be refused")
	}
}
