package mql

import (
	"strings"
	"testing"
)

// printDuration emits a bare "0" for a zero, and the parser accepts that
// token wherever a duration is expected -- but ParseDuration, the function
// the extraction spec reads its durations through, refused it. So
// printDuration -> ParseDuration was not a round trip at the one value
// both halves of the package already agree on, and
// `sets: {app: {retention: 0}}` -- the documented way to withdraw a set's
// retention (05-storage.md section 7.1) -- failed to compile.
func TestParseDurationAcceptsABareZero(t *testing.T) {
	for _, in := range []string{"0", "+0", "-0", "0.0"} {
		ms, err := ParseDuration(in)
		if err != nil {
			t.Fatalf("ParseDuration(%q): %v", in, err)
		}
		if ms != 0 {
			t.Fatalf("ParseDuration(%q) = %d, want 0", in, ms)
		}
	}
}

// A number other than zero still needs a unit: there is no default one,
// and guessing would silently change what a spec asks for.
func TestParseDurationStillRequiresAUnit(t *testing.T) {
	for _, in := range []string{"30", "1.5", "-7"} {
		if _, err := ParseDuration(in); err == nil {
			t.Fatalf("ParseDuration(%q) was accepted; a unit-less non-zero has no meaning", in)
		}
	}
}

// The pair that has to agree: whatever printDuration emits, ParseDuration
// must read back to the same number of milliseconds.
func TestPrintDurationRoundTripsThroughParseDuration(t *testing.T) {
	for _, ms := range []int64{0, 1, 999, 1000, 90_000, 3_600_000, 86_400_000, 172_800_000} {
		text := printDuration(ms)
		got, err := ParseDuration(text)
		if err != nil {
			t.Fatalf("ParseDuration(printDuration(%d)=%q): %v", ms, text, err)
		}
		if got != ms {
			t.Fatalf("printDuration(%d) = %q, which reads back as %d", ms, text, got)
		}
	}
}

// The text grammar accepted a bare zero before this change and must keep
// doing so.
func TestZeroDurationStillParsesInAQuery(t *testing.T) {
	q, err := Parse(`FROM a SELECT b GAP 0`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if q.Select[0].Modifiers.GapMs == nil || *q.Select[0].Modifiers.GapMs != 0 {
		t.Fatalf("GAP 0 did not parse to 0: %+v", q.Select[0].Modifiers.GapMs)
	}
	if text := Print(q); !strings.Contains(text, "GAP 0") {
		t.Fatalf("GAP 0 printed as %q", text)
	}
}
