package render

import (
	"math/rand"
	"testing"
)

// A connect-break must not put a synthetic value beside a point that is
// already drawn as part of a line.
//
// Padding was decided per window, and at any realistic zoom a window holds
// one sample, so the last reading before an outage was padded with the
// default `SSE const 0`. The panel then drew a healthy series diving to
// zero 500 ms before every break -- a number no source reported.
func TestNoPaddingBesideAConnectedPoint(t *testing.T) {
	var pts []Point
	for i := 0; i < 5; i++ {
		pts = append(pts, Point{Value: 45 + float64(i), TSMs: int64(i) * 60_000})
	}
	// A ten-minute outage, then the source comes back.
	for i := 0; i < 3; i++ {
		pts = append(pts, Point{Value: 50 + float64(i), TSMs: 840_000 + int64(i)*60_000})
	}
	out := Series(pts, Spec{GapMs: 60_000, SSE: SSE{Mode: SSEConst}}, 43_200)

	for i, o := range out {
		if o.Null || o.Value != 0 {
			continue
		}
		t.Fatalf("output %d is a synthetic zero at %d, beside data that never reached zero: %+v", i, o.TSMs, out)
	}
	// Every real sample still reaches the panel, and the break with them.
	reals, nulls := 0, 0
	for _, o := range out {
		if o.Null {
			nulls++
			continue
		}
		reals++
	}
	if reals != len(pts) || nulls != 1 {
		t.Fatalf("expected %d real points and one break, got %d and %d: %+v", len(pts), reals, nulls, out)
	}
}

// A point the finished series really does strand -- a break on both sides
// -- is still padded, which is what the padding is for.
func TestStrandedPointIsStillPadded(t *testing.T) {
	pts := []Point{
		{Value: 1, TSMs: 0},
		{Value: 2, TSMs: 600_000},
		{Value: 3, TSMs: 1_200_000},
	}
	out := Series(pts, Spec{GapMs: 60_000, SSE: SSE{Mode: SSERepeat}}, 43_200)
	// The middle sample sits between two breaks: it needs neighbours or it
	// draws as a zero-length mark.
	var padded int
	for i, o := range out {
		if o.Null || o.Value != 2 {
			continue
		}
		if i > 0 && !out[i-1].Null && out[i-1].Value == 2 {
			padded++
		}
		if i+1 < len(out) && !out[i+1].Null && out[i+1].Value == 2 {
			padded++
		}
	}
	if padded != 2 {
		t.Fatalf("the stranded sample was padded on %d side(s), want 2: %+v", padded, out)
	}
}

// C6 still holds: the whole-series singular case is the same rule, not a
// special case beside it.
func TestSingleSampleStillPads(t *testing.T) {
	out := Series([]Point{{Value: 42, TSMs: 5000}}, Spec{}, 60_000)
	if len(out) != 3 || out[0].Value != 0 || out[1].Value != 42 || out[2].Value != 0 {
		t.Fatalf("expected a padded triple, got %+v", out)
	}
}

// C1 over random series with gaps, padding and every SSE mode: the pass
// runs over the finished output, so it is the one place that could
// reorder it.
func TestC1AfterPaddingOverRandomSeries(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	modes := []SSE{{Mode: SSEConst}, {Mode: SSEConst, Value: -3}, {Mode: SSERepeat}, {Mode: SSEOff}}
	for iter := 0; iter < 20_000; iter++ {
		n := rng.Intn(12)
		pts := make([]Point, 0, n)
		ts := int64(rng.Intn(1000))
		for i := 0; i < n; i++ {
			ts += int64(rng.Intn(4000))
			pts = append(pts, Point{Value: rng.NormFloat64() * 100, TSMs: ts})
		}
		spec := Spec{
			GapMs: int64(rng.Intn(3) * 500),
			SSE:   modes[rng.Intn(len(modes))],
		}
		if rng.Intn(2) == 0 {
			spec.EndMs = ts + int64(rng.Intn(8000))
		}
		out := Series(pts, spec, int64(rng.Intn(3000)))
		for i := 1; i < len(out); i++ {
			if out[i].TSMs <= out[i-1].TSMs {
				t.Fatalf("C1 broken at %d for spec %+v: %+v", i, spec, out)
			}
		}
	}
}
