package mql

import (
	"strings"
	"testing"
)

// A declaration that does nothing is worse than one that is refused: the
// panel looks right and is the wrong shape. These three were accepted and
// dropped in silence.

// A bucket set is reduced by sum per window, so the heatmap executor
// never builds a render.Spec and every per-field modifier on a
// HISTOGRAM() selection was ignored -- `HISTOGRAM(h) RATE` drew raw
// counts under a legend the author read as a rate.
func TestHistogramModifiersAreRefused(t *testing.T) {
	s := testSchema{}
	for _, src := range []string{
		`FROM http SELECT HISTOGRAM(hdr24) RATE FORMAT heatmap`,
		`FROM http SELECT HISTOGRAM(hdr24) DELTA FORMAT heatmap`,
		`FROM http SELECT HISTOGRAM(hdr24) NEGATE FORMAT heatmap`,
		`FROM http SELECT HISTOGRAM(hdr24) GAP 30s FORMAT heatmap`,
		`FROM http SELECT HISTOGRAM(hdr24) SSE OFF FORMAT heatmap`,
		`FROM http SELECT HISTOGRAM(hdr24) CLAMP MIN 0 FORMAT heatmap`,
		`FROM http SELECT HISTOGRAM(hdr24) REQUIRED FORMAT heatmap`,
	} {
		q, err := Parse(src)
		if err != nil {
			t.Fatalf("parse %q: %v", src, err)
		}
		_, verr := Validate(q, s, 0, 0)
		if verr == nil {
			t.Fatalf("%q validated; a modifier on a bucket set has no series to apply to", src)
		}
		var d Diag
		if !asDiagnostic(verr, &d) || d.Code != "E008" {
			t.Fatalf("%q: want E008, got %v", src, verr)
		}
	}
	// AS names the series and is not a modifier, so it still validates.
	q, err := Parse(`FROM http SELECT HISTOGRAM(hdr24) AS "latency" FORMAT heatmap`)
	if err != nil {
		t.Fatal(err)
	}
	if _, verr := Validate(q, s, 0, 0); verr != nil {
		t.Fatalf("AS on a histogram must still validate: %v", verr)
	}
}

// A tabular format does no downsampling and produces rows rather than
// series, so EVERY and LIMIT SERIES are read by nothing on that path.
func TestTimeseriesOnlyClausesAreRefusedOnTabularFormats(t *testing.T) {
	s := testSchema{}
	for _, src := range []string{
		`FROM http SELECT requests_total EVERY 90s FORMAT table`,
		`FROM http SELECT requests_total EVERY 90s FORMAT logs`,
		`FROM http SELECT requests_total FORMAT table LIMIT SERIES 10`,
		`FROM http SELECT requests_total FORMAT logs LIMIT SERIES 10`,
	} {
		q, err := Parse(src)
		if err != nil {
			t.Fatalf("parse %q: %v", src, err)
		}
		_, verr := Validate(q, s, 1000, 100000)
		if verr == nil {
			t.Fatalf("%q validated; the clause is read by nothing under a tabular format", src)
		}
		var d Diag
		if !asDiagnostic(verr, &d) || d.Code != "E008" {
			t.Fatalf("%q: want E008, got %v", src, verr)
		}
	}
	// The grammar still accepts them, so Print -> Parse stays total for
	// an AST that was stored before the rule existed.
	q, err := Parse(`FROM http SELECT requests_total EVERY 90s FORMAT table LIMIT SERIES 10`)
	if err != nil {
		t.Fatalf("the text surface must still parse: %v", err)
	}
	if _, err := Parse(Print(q)); err != nil {
		t.Fatalf("printing it must still round trip: %v", err)
	}
	// And they stay legal where they mean something.
	for _, src := range []string{
		`FROM http SELECT requests_total EVERY 90s`,
		`FROM http SELECT requests_total LIMIT SERIES 10`,
		`FROM http SELECT HISTOGRAM(hdr24) EVERY 1m FORMAT heatmap LIMIT SERIES 10`,
	} {
		q, err := Parse(src)
		if err != nil {
			t.Fatalf("parse %q: %v", src, err)
		}
		if _, verr := Validate(q, s, 1000, 100000); verr != nil {
			t.Fatalf("%q must still validate: %v", src, verr)
		}
	}
}

// The store switches on Kind with a data-query default, so an
// unrecognised value executed as a timeseries query rather than being
// refused -- the same hazard the unknown-FORMAT check was added for.
func TestUnknownQueryKindIsRefused(t *testing.T) {
	s := testSchema{}
	q := &Query{Kind: Kind("Labels"), Label: "host"}
	_, verr := Validate(q, s, 0, 0)
	if verr == nil {
		t.Fatal("an unrecognised kind must be refused, not executed as a data query")
	}
	var d Diag
	if !asDiagnostic(verr, &d) || d.Code != "E008" {
		t.Fatalf("want E008, got %v", verr)
	}
	if !strings.Contains(d.Msg, "Labels") {
		t.Fatalf("the diagnostic must name the kind: %s", d.Msg)
	}
	// An absent kind is a data query, which is what every hand-authored
	// panel omits.
	ok := &Query{From: "http", Select: []FieldExpr{{Field: "requests_total"}}}
	if _, verr := Validate(ok, s, 0, 0); verr != nil {
		t.Fatalf("an absent kind must still validate as a data query: %v", verr)
	}
}
