package mql

import "testing"

// Printing must always produce text the lexer can read back. 'g'
// formatting turns a million into "1e+06", which the number lexer cannot
// parse, so a query with a large CLAMP bound would not survive a round
// trip through the builder.
func TestRoundTripSurvivesExtremeNumbersAndUnicode(t *testing.T) {
	for _, src := range []string{
		`FROM http SELECT inflight CLAMP MAX 1000000`,
		`FROM http SELECT inflight CLAMP MIN 0, MAX 2000000 ELSE RAW`,
		`FROM http SELECT inflight SSE 1000000`,
		`FROM http SELECT inflight CLAMP MAX 0.0000001`,
		`FROM http SELECT inflight WHERE host = "$not a var"`,
		`FROM http SELECT inflight WHERE host =~ /a\\b/`,
		`FROM http SELECT inflight WHERE host IN ($h, "x")`,
		`FROM "café" SELECT "naïve" BY "dc"`,
	} {
		q, err := Parse(src)
		if err != nil {
			t.Errorf("parse %q: %v", src, err)
			continue
		}
		printed := Print(q)
		q2, err := Parse(printed)
		if err != nil {
			t.Errorf("printed form does not re-parse\n  src     %s\n  printed %s\n  err     %v", src, printed, err)
			continue
		}
		if again := Print(q2); again != printed {
			t.Errorf("round trip is not stable\n  src   %s\n  once  %s\n  twice %s", src, printed, again)
		}
	}
}

// A "$name" that survives into execution matches nothing, so the query
// would draw an empty panel with no explanation. It has to be an error.
func TestUnsubstitutedVariableIsAnError(t *testing.T) {
	q, err := Parse(`FROM http SELECT inflight WHERE host = $host`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, verr := Validate(q, testSchema{}, 0, 0)
	if verr == nil {
		t.Fatal("an unsubstituted variable must not validate")
	}
	d, ok := verr.(Diag)
	if !ok || d.Code != "E010" {
		t.Fatalf("want E010, got %v", verr)
	}

	// A substituted value validates normally.
	q2, err := Parse(`FROM http SELECT inflight WHERE host = "web1"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, verr := Validate(q2, testSchema{}, 0, 0); verr != nil {
		t.Fatalf("a literal value must validate: %v", verr)
	}
}

// A number with two decimal points is a parse error with a position, not a
// bare strconv failure.
func TestMalformedNumberIsAPositionedParseError(t *testing.T) {
	_, err := Parse(`FROM http SELECT inflight CLAMP MAX 1.2.3`)
	if err == nil {
		t.Fatal("expected a parse error")
	}
	if _, ok := err.(*ParseError); !ok {
		t.Fatalf("want *ParseError, got %T: %v", err, err)
	}
}

// The executor switches on Format with a timeseries default and the
// render layer falls back to the constant SSE mode, so an unrecognised
// value used to draw a plausible panel of the wrong shape instead of
// being refused.
func TestValidateRefusesUnrunnableModifiers(t *testing.T) {
	sc := testSchema{}
	neg := int64(-1000)
	badMode := &SSE{Mode: "nope"}
	lo, hi := 10.0, 1.0
	for _, tc := range []struct {
		name string
		q    *Query
	}{
		{"unknown format", &Query{From: "http", Format: "pie", Select: []FieldExpr{{Field: "inflight"}}}},
		{"negative every", &Query{From: "http", Select: []FieldExpr{{Field: "inflight"}}, EveryMs: &neg}},
		{"negative gap", &Query{From: "http", Select: []FieldExpr{{Field: "inflight", Modifiers: Modifiers{GapMs: &neg}}}}},
		{"unknown sse mode", &Query{From: "http", Select: []FieldExpr{{Field: "inflight", Modifiers: Modifiers{SSE: badMode}}}}},
		{"inverted clamp", &Query{From: "http", Select: []FieldExpr{{Field: "inflight", Modifiers: Modifiers{Clamp: &Clamp{Min: &lo, Max: &hi}}}}}},
		{"unknown clamp else", &Query{From: "http", Select: []FieldExpr{{Field: "inflight", Modifiers: Modifiers{Clamp: &Clamp{Min: &hi, Else: "shrug"}}}}}},
	} {
		if _, err := Validate(tc.q, sc, 0, 0); err == nil {
			t.Errorf("%s: accepted, want a diagnostic", tc.name)
		}
	}
}

// Print emits what the lexer reads back. A negative duration out of an
// unvalidated AST used to print as "-30s" and then fail to parse.
func TestNegativeDurationRoundTrips(t *testing.T) {
	ms, err := ParseDuration("-30s")
	if err != nil {
		t.Fatalf("ParseDuration: %v", err)
	}
	if ms != -30000 {
		t.Fatalf("got %d ms, want -30000", ms)
	}
}

// An AST whose "format" key is absent decodes to "" and executes as a
// timeseries -- that is the shape a hand-authored panel model is stored
// in, because there is no frontend yet. Every format-dependent check
// compared against the constant, so exactly that shape skipped the
// warning saying outages will be drawn as continuous lines.
func TestAbsentFormatIsValidatedAsTimeseries(t *testing.T) {
	sc := oneFieldSchema{}
	withFormat := &Query{Kind: KindQuery, From: "app", Format: FormatTimeseries,
		Select: []FieldExpr{{Field: "latency"}}}
	absent := &Query{Kind: KindQuery, From: "app",
		Select: []FieldExpr{{Field: "latency"}}}

	want, err := Validate(withFormat, sc, 0, 0)
	if err != nil {
		t.Fatalf("explicit timeseries: %v", err)
	}
	if !hasCode(want, "W103") {
		t.Fatal("explicit timeseries did not warn about the missing cadence")
	}
	got, err := Validate(absent, sc, 0, 0)
	if err != nil {
		t.Fatalf("absent format: %v", err)
	}
	if !hasCode(got, "W103") {
		t.Fatalf("an absent FORMAT skipped W103; got %+v", got)
	}
}

// The timeseries-only modifier check must key off the same normalised
// format, and an unknown one must still be refused.
func TestFormatNormalisationDoesNotWidenWhatIsAccepted(t *testing.T) {
	sc := oneFieldSchema{}
	bad := &Query{Kind: KindQuery, From: "app", Format: Format("sideways"),
		Select: []FieldExpr{{Field: "latency"}}}
	if _, err := Validate(bad, sc, 0, 0); err == nil {
		t.Fatal("an unknown format was accepted")
	}
	tabular := &Query{Kind: KindQuery, From: "app", Format: FormatTable,
		Select: []FieldExpr{{Field: "latency", Modifiers: Modifiers{Delta: true}}}}
	if _, err := Validate(tabular, sc, 0, 0); err == nil {
		t.Fatal("a timeseries-only modifier was accepted with FORMAT table")
	}
}

// oneFieldSchema is a catalogue holding one gauge with no declared
// cadence, which is what W103 fires on.
type oneFieldSchema struct{}

func (oneFieldSchema) HasSet(set string) bool { return set == "app" }
func (oneFieldSchema) Sets() []string         { return []string{"app"} }
func (oneFieldSchema) Field(set, field string) (FieldInfo, bool) {
	if set == "app" && field == "latency" {
		return FieldInfo{Kind: "gauge"}, true
	}
	return FieldInfo{}, false
}
func (oneFieldSchema) HasLabel(string, string) bool              { return true }
func (oneFieldSchema) BucketSet(string, string) ([]string, bool) { return nil, false }
