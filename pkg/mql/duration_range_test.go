package mql

import (
	"math"
	"strings"
	"testing"
)

// ParseDuration multiplies a unit count by a millisecond factor and used
// to convert the result on trust. Converting a float outside the int64
// range is undefined by the Go spec -- amd64 yields the indefinite value
// and arm64 saturates -- so the same spec text produced two different
// durations on two architectures, neither of them the one written.
//
// 06-query.md section 3 records that this is the function the extraction
// spec reads `retention:`, `shard:` and `max_interval:` through, so the
// value reaches the store as a set's retention.
func TestOutOfRangeDurationIsRefused(t *testing.T) {
	for _, s := range []string{
		"99999999999999999999d",
		"-99999999999999999999d",
		"9223372036854775807ms",
		"100000000000000000h",
	} {
		got, err := ParseDuration(s)
		if err == nil {
			t.Fatalf("ParseDuration(%q) = %d, want an error: the value cannot be held in milliseconds", s, got)
		}
		if !strings.Contains(err.Error(), "range") {
			t.Fatalf("ParseDuration(%q) error %q does not say the value is out of range", s, err)
		}
	}
}

// The ordinary values still parse, including the largest one that really
// does fit, and the boundary is not moved by the check.
func TestInRangeDurationsStillParse(t *testing.T) {
	for _, c := range []struct {
		text string
		want int64
	}{
		{"0", 0}, {"500ms", 500}, {"30s", 30_000}, {"5m", 300_000},
		{"1h", 3_600_000}, {"30d", 30 * 86_400_000}, {"-30s", -30_000},
		{"1.5s", 1500},
	} {
		got, err := ParseDuration(c.text)
		if err != nil {
			t.Fatalf("ParseDuration(%q): %v", c.text, err)
		}
		if got != c.want {
			t.Fatalf("ParseDuration(%q) = %d, want %d", c.text, got, c.want)
		}
	}
	// One that is large and still exact.
	if got, err := ParseDuration("100000000000d"); err != nil || got != 100000000000*86_400_000 {
		t.Fatalf("ParseDuration(100000000000d) = (%d, %v)", got, err)
	}
	if maxIntFloat != math.Ldexp(1, 63) {
		t.Fatalf("maxIntFloat is %v, want 2^63", maxIntFloat)
	}
}

// RATE is sugar for DELTA PER SECOND (06-query.md section 3), so writing
// it beside either half sets the same flag twice. The duplicate was
// accepted, folded away and printed back as plain RATE, while the
// spelling `x DELTA DELTA` was refused as E006 -- the same duplicate,
// diagnosed on one spelling and silent on the other.
func TestRateBesideItsHalvesIsADuplicate(t *testing.T) {
	for _, text := range []string{
		`FROM a SELECT b RATE DELTA`,
		`FROM a SELECT b DELTA RATE`,
		`FROM a SELECT b RATE PER SECOND`,
		`FROM a SELECT b PER SECOND RATE`,
		`FROM a SELECT b RATE RATE`,
	} {
		_, err := Parse(text)
		if err == nil {
			t.Fatalf("%q parsed; a modifier that sets the same flag twice is E006", text)
		}
		if !strings.Contains(err.Error(), "E006") {
			t.Fatalf("%q: expected E006, got %v", text, err)
		}
	}
}

// The combination that is not a duplicate still parses, and still means
// RATE.
func TestDeltaPerSecondIsStillRate(t *testing.T) {
	for _, text := range []string{
		`FROM a SELECT b DELTA PER SECOND`,
		`FROM a SELECT b PER SECOND`,
		`FROM a SELECT b DELTA`,
		`FROM a SELECT b RATE`,
	} {
		q, err := Parse(text)
		if err != nil {
			t.Fatalf("%q: %v", text, err)
		}
		if len(q.Select) != 1 {
			t.Fatalf("%q selected %d fields", text, len(q.Select))
		}
	}
	q, err := Parse(`FROM a SELECT b DELTA PER SECOND`)
	if err != nil {
		t.Fatal(err)
	}
	if !q.Select[0].Modifiers.Delta || !q.Select[0].Modifiers.PerSecond {
		t.Fatalf("DELTA PER SECOND did not set both flags: %+v", q.Select[0].Modifiers)
	}
}
