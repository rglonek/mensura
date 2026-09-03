package mql

import (
	"errors"
	"math"
	"strings"
	"testing"
)

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

// A variable that survives into a regex is the same configuration error as
// one that survives into an equality. `host = "$env"` was a clear E010
// while `host =~ /$env/` compiled as a literal regex, matched nothing, and
// produced only a "matches no value" warning.
func TestUnsubstitutedVariableInARegexIsDiagnosed(t *testing.T) {
	sc := testSchema{}
	q := &Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Field: "inflight"}},
		Where: Expr{Match: &MatchExpr{Label: "host", Regex: "$env"}}}
	_, err := Validate(q, sc, 0, 0)
	d, ok := err.(Diag)
	if !ok || d.Code != "E010" {
		t.Fatalf("got %v, want E010", err)
	}
	// The same must hold for NOT MATCHES.
	q.Where = Expr{NoMatch: &MatchExpr{Label: "host", Regex: "web-$env-.*"}}
	if _, err := Validate(q, sc, 0, 0); err == nil {
		t.Fatal("a variable inside a NOT MATCHES regex was accepted")
	}
	// And a real regex must still be accepted: '$' is the end-of-line
	// anchor far more often than it is a variable, and an escaped '$' is
	// a literal dollar.
	for _, re := range []string{"web-.*$", `^\$total$`, "a|b$"} {
		q.Where = Expr{Match: &MatchExpr{Label: "host", Regex: re}}
		if _, err := Validate(q, sc, 0, 0); err != nil {
			t.Errorf("regex /%s/ was refused: %v", re, err)
		}
	}
}

// Variables() drives the plugin's dependency reporting, so a variable used
// only inside a regex has to be listed or the panel is never re-run when
// it changes.
func TestVariablesFindsRegexReferences(t *testing.T) {
	q := &Query{From: "http", Where: Expr{Or: []Expr{
		{Match: &MatchExpr{Label: "host", Regex: "^$env-"}},
		{Eq: &Compare{Label: "dc", Value: "$region"}},
	}}}
	got := Variables(q)
	want := map[string]bool{"env": true, "region": true}
	if len(got) != 2 || !want[got[0]] || !want[got[1]] {
		t.Fatalf("Variables() = %v, want env and region", got)
	}
}

// HAS and MISSING name a label just as the comparisons do. Exempting them
// from the unknown-label check meant `MISSING nosuchlabel` validated and
// matched every row.
func TestHasAndMissingCheckTheLabelName(t *testing.T) {
	sc := testSchema{}
	for _, e := range []Expr{{Has: "nosuchlabel"}, {Missing: "nosuchlabel"}} {
		q := &Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Field: "inflight"}}, Where: e}
		if _, err := Validate(q, sc, 0, 0); err == nil {
			t.Errorf("%+v was accepted against a catalogue that has no such label", e)
		}
	}
	// A label that does exist is still fine.
	q := &Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Field: "inflight"}}, Where: Expr{Has: "host"}}
	if _, err := Validate(q, sc, 0, 0); err != nil {
		t.Fatalf("HAS host was refused: %v", err)
	}
}

// The lexer emits "1.5" as a number, so ParseInt on it surfaced a bare
// strconv error with no position in it while every other syntax fault in
// the parser comes back positioned.
func TestFractionalIntegerIsAPositionedParseError(t *testing.T) {
	_, err := Parse(`FROM http SELECT v LIMIT SERIES 1.5`)
	if err == nil {
		t.Fatal("a fractional LIMIT SERIES was accepted")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("got %T (%v), want a *ParseError", err, err)
	}
}

// A HISTOGRAM under anything but FORMAT heatmap has no series to draw:
// the planner resolves no field for a bucket set, so the executor
// iterates an empty list and the panel comes back empty -- with no error
// and no warning, which is the failure this validator exists to prevent.
// Only the mirror image, heatmap without HISTOGRAM, used to be refused.
func TestHistogramNeedsFormatHeatmap(t *testing.T) {
	s := testSchema{}
	for _, src := range []string{
		`FROM http SELECT HISTOGRAM(hdr24)`,
		`FROM http SELECT HISTOGRAM(hdr24) FORMAT timeseries`,
		`FROM http SELECT HISTOGRAM(hdr24) FORMAT table`,
		`FROM http SELECT HISTOGRAM(hdr24) FORMAT logs`,
	} {
		q := mustParse(t, src)
		_, err := Validate(q, s, 1000, 10000)
		if err == nil {
			t.Fatalf("%s: expected the bucket set to be refused outside FORMAT heatmap", src)
		}
		var d Diag
		if !errors.As(err, &d) || d.Code != "E008" {
			t.Fatalf("%s: expected E008, got %v", src, err)
		}
	}
	// A hand-authored AST with no format at all decodes to "" and
	// executes as a timeseries, so it has to be refused too.
	q := &Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Histogram: "hdr24"}}}
	if _, err := Validate(q, s, 1000, 10000); err == nil {
		t.Fatal("expected an AST with no format to be refused")
	}
	// The one form that is meant to work still does.
	if _, err := Validate(mustParse(t, `FROM http SELECT HISTOGRAM(hdr24) FORMAT heatmap`), s, 1000, 10000); err != nil {
		t.Fatalf("FORMAT heatmap should validate: %v", err)
	}
}

// `WHERE HAS ""` parsed into Expr{Has: ""}, which Expr.Empty reports as
// carrying no predicate at all: Print dropped the whole clause and the
// store's lowering produced a nil expression, so a query that asked for
// one thing silently widened to every row in the set.
func TestEmptyNameIsRefusedRatherThanSilentlyDroppingThePredicate(t *testing.T) {
	for _, src := range []string{
		`FROM http SELECT x WHERE HAS ""`,
		`FROM http SELECT x WHERE MISSING ""`,
		`FROM http SELECT x WHERE "" = "y"`,
		`FROM "" SELECT x`,
		`FROM http SELECT ""`,
		`FROM http SELECT x BY ""`,
	} {
		if _, err := Parse(src); err == nil {
			t.Fatalf("%s: expected an empty name to be refused", src)
		}
	}
}

// The number lexer has to read back what printFloat writes, or a query
// carrying a large CLAMP bound does not survive Print -> Parse.
func TestLargeAndSmallNumbersRoundTrip(t *testing.T) {
	for _, v := range []float64{1e6, 1e300, -1e300, 1.5e-8, math.MaxFloat64, -math.MaxFloat64, 0.5, 0, -3} {
		txt := printFloat(v)
		q, err := Parse(`FROM http SELECT x CLAMP MIN ` + txt)
		if err != nil {
			t.Fatalf("printFloat(%g) = %q, which does not lex back: %v", v, txt, err)
		}
		got := *q.Select[0].Modifiers.Clamp.Min
		if got != v {
			t.Fatalf("printFloat(%g) = %q parsed back as %g", v, txt, got)
		}
		if len(txt) > maxPlainFloatDigits+2 {
			t.Fatalf("printFloat(%g) = %q is %d characters; the exponent form should have been used", v, txt, len(txt))
		}
	}
	// A bare "e" is still an identifier, not an exponent.
	if _, err := Parse(`FROM http SELECT x GAP 30s`); err != nil {
		t.Fatalf("durations must still lex: %v", err)
	}
}

// A regex is escaped for its delimiters, not doubled wholesale. Doubling
// every backslash round-tripped but printed /\\d+/ for the pattern \d+,
// so the canonical text of a query -- what the builder shows and what
// Grafana reports as the executed query -- read as a different regex from
// the one that was written.
func TestRegexPrintsReadablyAndRoundTrips(t *testing.T) {
	for _, re := range []string{
		`\d+`, `a/b`, `a\/b`, `a\\b`, `\.`, `^x$`, `[a-z]\s*=\s*\d`, `back\`, `\n`, `\t`,
		`(?P<n>\w+)`, `a\\/b`, `//`, `\\`,
	} {
		q := &Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Field: "x"}},
			Where: Expr{Match: &MatchExpr{Label: "host", Regex: re}}}
		txt := Print(q)
		back, err := Parse(txt)
		if err != nil {
			t.Fatalf("regex %q printed as %q, which does not parse: %v", re, txt, err)
		}
		if got := back.Where.Match.Regex; got != re {
			t.Fatalf("regex %q printed as %q and parsed back as %q", re, txt, got)
		}
	}
	// The readable form is the point: a plain regex escape is not doubled.
	q := &Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Field: "x"}},
		Where: Expr{Match: &MatchExpr{Label: "host", Regex: `\d+`}}}
	if txt := Print(q); !strings.Contains(txt, `/\d+/`) {
		t.Fatalf("expected /\\d+/ in the printed query, got %q", txt)
	}
}

// Every syntax fault in this parser comes back positioned, so the editor
// can underline it. A number that will not parse used to be the one
// exception, and it moved the cursor past the token on the way out.
func TestBadNumberIsAPositionedError(t *testing.T) {
	_, err := Parse(`FROM http SELECT x CLAMP MIN 1.2.3`)
	if err == nil {
		t.Fatal("expected a parse error")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected a positioned ParseError, got %T: %v", err, err)
	}
	if pe.Pos <= 0 {
		t.Fatalf("expected a position pointing into the query, got %d", pe.Pos)
	}
}

// An empty node inside and/or is refused rather than folded away. The
// store lowers it to a constant true, so it executed fine, but Print
// emitted "( AND host = \"x\")" for it -- text the parser cannot read --
// and the AST and its canonical text are documented to round-trip
// losslessly.
func TestEmptyPredicateArmIsRefused(t *testing.T) {
	eq := Expr{Eq: &Compare{Label: "host", Value: "x"}}
	for _, tc := range []struct {
		name string
		q    Query
	}{
		{"and", Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Field: "v"}}, Where: Expr{And: []Expr{{}, eq}}}},
		{"or", Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Field: "v"}}, Where: Expr{Or: []Expr{eq, {}}}}},
		{"not", Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Field: "v"}}, Where: Expr{Not: &Expr{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.q
			if _, err := Validate(&q, nil, 0, 0); err == nil {
				t.Fatalf("an empty %s arm validated, and prints as %q", tc.name, Print(&q))
			}
		})
	}
	// A predicate that is absent altogether is still fine: that means
	// "no predicate", not "an arm that sets nothing".
	q := Query{Kind: KindQuery, From: "http", Select: []FieldExpr{{Field: "v"}}, Where: Expr{}}
	if _, err := Validate(&q, nil, 0, 0); err != nil {
		t.Fatalf("an absent predicate was refused: %v", err)
	}
}
