package render

import (
	"math"
	"math/rand"
	"testing"
)

func f64(v float64) *float64 { return &v }

// randomSeries builds a series with gaps, duplicates, plateaus and resets,
// which are exactly the shapes the walk's stages exist to handle.
func randomSeries(rnd *rand.Rand, n int) []Point {
	pts := make([]Point, 0, n)
	ts := int64(1_700_000_000_000)
	v := float64(rnd.Intn(100))
	for i := 0; i < n; i++ {
		switch rnd.Intn(10) {
		case 0: // gap
			ts += int64(30_000 + rnd.Intn(120_000))
		case 1: // duplicate timestamp
		default:
			ts += int64(500 + rnd.Intn(1500))
		}
		switch rnd.Intn(12) {
		case 0: // counter reset
			v = 0
		case 1: // plateau
		default:
			v += float64(rnd.Intn(50))
		}
		pts = append(pts, Point{Value: v, TSMs: ts})
	}
	return pts
}

// C1: output timestamps are strictly increasing.
func TestC1MonotonicTime(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	for trial := 0; trial < 200; trial++ {
		pts := randomSeries(rnd, 1+rnd.Intn(120))
		spec := Spec{
			Delta:     rnd.Intn(2) == 0,
			PerSecond: rnd.Intn(2) == 0,
			Negate:    rnd.Intn(2) == 0,
			GapMs:     int64(rnd.Intn(2) * 20_000),
			SSE:       SSE{Mode: SSEMode(rnd.Intn(3))},
		}
		out := Series(pts, spec, int64(rnd.Intn(60_000)))
		for i := 1; i < len(out); i++ {
			if out[i].TSMs <= out[i-1].TSMs {
				t.Fatalf("trial %d: non-monotonic output at %d: %d then %d (spec %+v)",
					trial, i, out[i-1].TSMs, out[i].TSMs, spec)
			}
		}
	}
}

// C2: a raw gap wider than GapMs yields a null just before the later sample.
func TestC2NoFalseContinuity(t *testing.T) {
	pts := []Point{
		{Value: 1, TSMs: 1000},
		{Value: 2, TSMs: 2000},
		{Value: 3, TSMs: 60000}, // 58 s gap, cadence declared as 10 s
		{Value: 4, TSMs: 61000},
	}
	out := Series(pts, Spec{GapMs: 10_000, SSE: SSE{Mode: SSEOff}}, 0)
	found := false
	for _, o := range out {
		if o.Null && o.TSMs == 59999 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a null at 59999, got %+v", out)
	}
}

func TestC2GapDisabledEmitsNoNull(t *testing.T) {
	pts := []Point{{Value: 1, TSMs: 1000}, {Value: 2, TSMs: 900000}}
	for _, o := range Series(pts, Spec{SSE: SSE{Mode: SSEOff}}, 0) {
		if o.Null {
			t.Fatalf("GapMs=0 must disable gap detection, got %+v", o)
		}
	}
}

// C3: a clamp violation with ELSE RAW emits the raw sample, and rate
// normalisation still applies to the substituted value.
func TestC3RawValueRecoverableOnClamp(t *testing.T) {
	// A counter that resets: the delta goes negative, the clamp fires.
	pts := []Point{
		{Value: 100, TSMs: 1000},
		{Value: 150, TSMs: 2000},
		{Value: 7, TSMs: 3000}, // reset
	}
	spec := Spec{Delta: true, ClampMin: f64(0), ClampElseRaw: true, SSE: SSE{Mode: SSEOff}}
	out := Series(pts, spec, 0)
	last := out[len(out)-1]
	if last.Value != 7 {
		t.Fatalf("expected the raw counter value 7 after reset, got %v (%+v)", last.Value, out)
	}

	// Without ELSE RAW the value clamps to the bound instead.
	pts = []Point{{Value: 100, TSMs: 1000}, {Value: 150, TSMs: 2000}, {Value: 7, TSMs: 3000}}
	out = Series(pts, Spec{Delta: true, ClampMin: f64(0), SSE: SSE{Mode: SSEOff}}, 0)
	if last = out[len(out)-1]; last.Value != 0 {
		t.Fatalf("expected clamp to bound 0, got %v", last.Value)
	}

	// With PER SECOND the substituted raw value is still divided by the
	// elapsed seconds, which is the documented interaction.
	pts = []Point{{Value: 100, TSMs: 1000}, {Value: 150, TSMs: 2000}, {Value: 8, TSMs: 4000}}
	out = Series(pts, Spec{Delta: true, PerSecond: true, ClampMin: f64(0), ClampElseRaw: true, SSE: SSE{Mode: SSEOff}}, 0)
	if last = out[len(out)-1]; math.Abs(last.Value-4) > 1e-9 {
		t.Fatalf("expected raw 8 divided by 2 s = 4, got %v", last.Value)
	}
}

// C4: both extrema of every window are emitted, at their raw timestamps.
func TestC4ExtremesPreserved(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	for trial := 0; trial < 100; trial++ {
		pts := randomSeries(rnd, 20+rnd.Intn(200))
		window := int64(1 + rnd.Intn(30_000))
		spec := Spec{SSE: SSE{Mode: SSEOff}}
		out := Series(pts, spec, window)

		// Brute-force reference: replay the accepted samples, bucket them
		// the same way, and check every bucket's extrema appear in out.
		emitted := map[int64]float64{}
		for _, o := range out {
			if !o.Null {
				emitted[o.TSMs] = o.Value
			}
		}
		var start int64
		var have bool
		var mn, mx Point
		var lastTS int64 = -1
		check := func() {
			if !have {
				return
			}
			for _, p := range []Point{mn, mx} {
				v, ok := emitted[p.TSMs]
				if !ok || v != p.Value {
					t.Fatalf("trial %d: extremum (%v @ %d) missing from output", trial, p.Value, p.TSMs)
				}
			}
		}
		for _, p := range pts {
			if p.TSMs == lastTS {
				continue
			}
			lastTS = p.TSMs
			if !have {
				start = p.TSMs
			}
			if have && p.TSMs-start > window {
				check()
				start, have = p.TSMs, false
			}
			if !have {
				mn, mx, have = p, p, true
				continue
			}
			if p.Value < mn.Value {
				mn = p
			}
			if p.Value > mx.Value {
				mx = p
			}
		}
		check()
	}
}

// C5: ties are won by the earliest sample, which keeps the leading edge of
// a plateau visible.
func TestC5EarliestExtremumWins(t *testing.T) {
	pts := []Point{
		{Value: 5, TSMs: 1000},
		{Value: 9, TSMs: 2000},
		{Value: 9, TSMs: 3000},
		{Value: 5, TSMs: 4000},
	}
	out := Series(pts, Spec{SSE: SSE{Mode: SSEOff}}, 10_000)
	for _, o := range out {
		if o.Value == 9 && o.TSMs != 2000 {
			t.Fatalf("expected the earliest maximum at 2000, got %d", o.TSMs)
		}
	}
}

// C6: a series with any accepted sample never renders as nothing.
func TestC6NoSilentSeriesLoss(t *testing.T) {
	out := Series([]Point{{Value: 42, TSMs: 5000}}, Spec{}, 60_000)
	if len(out) != 3 {
		t.Fatalf("expected a padded triple, got %+v", out)
	}
	if out[1].Value != 42 || out[0].Value != 0 || out[2].Value != 0 {
		t.Fatalf("expected constant-0 padding around the real point, got %+v", out)
	}

	out = Series([]Point{{Value: 42, TSMs: 5000}}, Spec{SSE: SSE{Mode: SSERepeat}}, 60_000)
	if out[0].Value != 42 || out[2].Value != 42 {
		t.Fatalf("REPEAT must pad with the real value, got %+v", out)
	}

	out = Series([]Point{{Value: 42, TSMs: 5000}}, Spec{SSE: SSE{Mode: SSEOff}}, 60_000)
	if len(out) != 1 {
		t.Fatalf("SSE OFF must opt out of padding, got %+v", out)
	}
}

// C7: duplicates do not mask a later real gap.
func TestC7DuplicateResilience(t *testing.T) {
	pts := []Point{
		{Value: 1, TSMs: 1000},
		{Value: 2, TSMs: 1000}, // duplicate, dropped
		{Value: 3, TSMs: 1000}, // duplicate, dropped
		{Value: 4, TSMs: 40000},
	}
	out := Series(pts, Spec{GapMs: 5_000, SSE: SSE{Mode: SSEOff}}, 0)
	nulls := 0
	reals := 0
	for _, o := range out {
		if o.Null {
			nulls++
			if o.TSMs != 39999 {
				t.Fatalf("expected the null at 39999, got %d", o.TSMs)
			}
		} else {
			reals++
		}
	}
	if nulls != 1 {
		t.Fatalf("expected exactly one null, got %d (%+v)", nulls, out)
	}
	if reals != 2 {
		t.Fatalf("expected the duplicates to be dropped, got %d real points", reals)
	}
}

// C8: a zero-width window emits every accepted sample, in order.
func TestC8GracefulDegeneracy(t *testing.T) {
	pts := []Point{
		{Value: 1, TSMs: 1000},
		{Value: 2, TSMs: 1001},
		{Value: 3, TSMs: 1002},
		{Value: 4, TSMs: 1003},
	}
	out := Series(pts, Spec{SSE: SSE{Mode: SSEOff}}, 0)
	if len(out) != len(pts) {
		t.Fatalf("expected one output per sample, got %d: %+v", len(out), out)
	}
	for i := range pts {
		if out[i].TSMs != pts[i].TSMs || out[i].Value != pts[i].Value {
			t.Fatalf("output %d does not match input: %+v vs %+v", i, out[i], pts[i])
		}
	}
}

func TestWindowFormula(t *testing.T) {
	// Render budget dominates.
	if got := Window(3_600_000, 1200, 60_000); got != 6000 {
		t.Fatalf("expected 2*(3600000/1200)=6000, got %d", got)
	}
	// Interval hint dominates.
	if got := Window(3_600_000, 1200, 1000); got != 2000 {
		t.Fatalf("expected 2*1000=2000, got %d", got)
	}
	// Short range: integer division yields 0, which is the "do not
	// downsample" case rather than an error.
	if got := Window(100, 1200, 0); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestDeltaFirstSampleIsConsumed(t *testing.T) {
	pts := []Point{{Value: 10, TSMs: 1000}, {Value: 25, TSMs: 2000}}
	out := Series(pts, Spec{Delta: true, SSE: SSE{Mode: SSEOff}}, 0)
	if len(out) != 1 || out[0].Value != 15 || out[0].TSMs != 2000 {
		t.Fatalf("expected a single delta of 15 at 2000, got %+v", out)
	}
}

func TestChronologicalEmitOrder(t *testing.T) {
	// Max occurs before min: the max must be emitted first, or the line
	// would slope the wrong way.
	pts := []Point{{Value: 100, TSMs: 1000}, {Value: 1, TSMs: 2000}}
	out := Series(pts, Spec{SSE: SSE{Mode: SSEOff}}, 60_000)
	if len(out) != 2 || out[0].Value != 100 || out[1].Value != 1 {
		t.Fatalf("expected chronological order, got %+v", out)
	}
}

// Padding beside a null must not reorder the series: a null injected one
// millisecond before a point leaves no room for a full-width pad.
func TestPaddingNeverReordersAgainstAdjacentNull(t *testing.T) {
	pts := []Point{
		{Value: 1, TSMs: 1000},
		{Value: 2, TSMs: 100_000}, // gap: null lands at 99999
	}
	out := Series(pts, Spec{GapMs: 5_000, SSE: SSE{Mode: SSEConst}}, 0)
	for i := 1; i < len(out); i++ {
		if out[i].TSMs <= out[i-1].TSMs {
			t.Fatalf("padding broke monotonic time: %+v", out)
		}
	}
}

// A trailing gap is a gap. A null was only ever injected when a later
// sample arrived, so a series that stopped mid-range ended at its last
// point: a source that went away drew as a line that simply stops, which
// is the false continuity the whole walk exists to prevent.
func TestTrailingGapDrawsAConnectBreak(t *testing.T) {
	t0 := int64(1_700_000_000_000)
	pts := []Point{{1, t0}, {2, t0 + 1000}, {3, t0 + 2000}}
	spec := Spec{GapMs: 2000, EndMs: t0 + 60_000, SSE: SSE{Mode: SSEOff}}
	out := Series(pts, spec, 0)
	if len(out) == 0 {
		t.Fatal("no output")
	}
	last := out[len(out)-1]
	if !last.Null {
		t.Fatalf("the series ends at a real point (%+v); a source that stopped 58s ago draws as a continuous line", last)
	}
	if last.TSMs != t0+2000+2000 {
		t.Fatalf("the break is at %d, want %d -- the moment the declared cadence was first missed", last.TSMs, t0+4000)
	}
	for i := 1; i < len(out); i++ {
		if out[i].TSMs <= out[i-1].TSMs {
			t.Fatalf("output timestamps are not strictly increasing at %d (C1)", i)
		}
	}
}

// A series that is still arriving on its cadence must not gain a break,
// and neither must one whose field declares no cadence at all.
func TestNoTrailingBreakWhenTheCadenceIsHonoured(t *testing.T) {
	t0 := int64(1_700_000_000_000)
	pts := []Point{{1, t0}, {2, t0 + 1000}}
	for _, spec := range []Spec{
		{GapMs: 5000, EndMs: t0 + 3000, SSE: SSE{Mode: SSEOff}},
		{GapMs: 0, EndMs: t0 + 600_000, SSE: SSE{Mode: SSEOff}},
		{GapMs: 5000, EndMs: 0, SSE: SSE{Mode: SSEOff}},
	} {
		out := Series(append([]Point(nil), pts...), spec, 0)
		if len(out) > 0 && out[len(out)-1].Null {
			t.Errorf("spec %+v invented a trailing break", spec)
		}
	}
}

// A series whose only sample is consumed by DELTA or RATE emitted nothing
// and then drew a trailing connect-break anyway: lastPointTime is set
// before those stages drop the sample, so the panel showed a break
// starting just after a moment when data did arrive. The real reason is
// that a rate needs two samples, and saying nothing says that better than
// saying something false.
func TestASingleSampleConsumedByRateDrawsNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec Spec
	}{
		{"rate", Spec{PerSecond: true, GapMs: 1000, EndMs: 100000}},
		{"delta", Spec{Delta: true, GapMs: 1000, EndMs: 100000}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := Series([]Point{{Value: 5, TSMs: 1000}}, tc.spec, 0)
			if len(out) != 0 {
				t.Fatalf("a series that emitted no value produced %d point(s): %+v", len(out), out)
			}
		})
	}
	// A series that did emit still gets its trailing break.
	out := Series([]Point{{Value: 5, TSMs: 1000}, {Value: 7, TSMs: 2000}}, Spec{PerSecond: true, GapMs: 1000, EndMs: 100000}, 0)
	if len(out) == 0 || !out[len(out)-1].Null {
		t.Fatalf("the trailing connect-break was lost for a series that did emit: %+v", out)
	}
}
