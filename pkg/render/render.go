// Package render implements the per-series post-processing walk described
// in "A Correctness-First, Single-Pass Downsampling Pipeline for
// Operational Time-Series" (Glonek, 2026) and in
// docs/design/07-downsampling.md.
//
// The walk runs once per series over an already-buffered slice of raw
// samples. It sorts, then makes a single pass applying ten stages in a
// fixed order whose invariants compose: gap detection and the window
// boundary read raw timestamps, the clamp fallback reads the raw value,
// and rate normalisation reads the raw inter-sample interval. The order is
// part of the contract, not a preference.
package render

import "sort"

// Point is one raw input sample.
type Point struct {
	Value float64
	TSMs  int64
}

// Output is one emitted point. Null marks a connect-break: absence of data
// must be distinguishable from a legitimate zero all the way to the
// renderer.
type Output struct {
	TSMs  int64
	Value float64
	Null  bool
}

// SSEMode selects how a series that reduces to a single visible point is
// padded so that it actually draws.
type SSEMode uint8

const (
	SSEConst  SSEMode = iota // pad with a constant (default 0)
	SSERepeat                // pad by repeating the real value
	SSEOff                   // do not pad
)

// SSE is the singular-series-extension setting for one field.
type SSE struct {
	Mode  SSEMode
	Value float64
}

// Spec is the per-field control surface: the declarative modifiers an MQL
// query resolves to. Stage numbers refer to docs/design/07-downsampling.md.
type Spec struct {
	Delta        bool     // stage 4: counter to per-sample difference
	PerSecond    bool     // stage 7: divide by raw elapsed seconds
	Negate       bool     // stage 5: mirror below the axis
	GapMs        int64    // stage 1: declared cadence; 0 disables gap detection
	ClampMin     *float64 // stage 6
	ClampMax     *float64 // stage 6
	ClampElseRaw bool     // stage 6: substitute the raw sample, not the bound
	SSE          SSE
	// EndMs is the end of the requested range. It is what makes a
	// *trailing* gap drawable: a null is otherwise only ever injected
	// when a later sample arrives, so a series that stopped mid-range
	// ended at its last point and a source that went away drew as a line
	// that simply stopped. Zero leaves the behaviour unchanged.
	EndMs int64
}

// ssePadMs is the visual half-width of singular-series padding: long
// enough to draw at any realistic zoom, short enough not to mislead about
// when the event happened.
const ssePadMs = 500

// Window computes the downsample window width for one series.
//
//	w = min(rangeMs / maxDataPoints, intervalMs) * 2
//
// The inner min honours both the panel's render budget and its minimum
// interval hint. The doubling is deliberate: each window emits both its
// min and its max, so a window sized to two points' worth of budget keeps
// the output count at what was asked for while doubling the information
// density. w == 0 is not special-cased; the strict boundary test below
// degenerates gracefully to "no downsampling".
func Window(rangeMs int64, maxDataPoints int, intervalMs int64) int64 {
	return SingleWindow(rangeMs, maxDataPoints, intervalMs) * 2
}

// SingleWindow is the same width without the doubling, for a reduction
// that emits one point per window instead of a min/max pair.
//
// The doubling in Window is not a general property of the window, it is
// bookkeeping for the walk: two points come out of each one, so the
// window is sized to two points' worth of the render budget and the
// output still lands on what the panel asked for. A heatmap column is a
// single summed value, so charging it the doubled width spends the whole
// budget on half the columns -- the panel asked for n and drew n/2, with
// nothing saying so and no way to ask for the rest short of EVERY.
func SingleWindow(rangeMs int64, maxDataPoints int, intervalMs int64) int64 {
	if maxDataPoints <= 0 {
		maxDataPoints = 1
	}
	w := rangeMs / int64(maxDataPoints)
	if intervalMs > 0 && intervalMs < w {
		w = intervalMs
	}
	if w < 0 {
		w = 0
	}
	return w
}

// Series applies the walk to one series and returns the points to draw.
// The input slice is sorted in place.
func Series(points []Point, spec Spec, window int64) []Output {
	sort.SliceStable(points, func(i, j int) bool { return points[i].TSMs < points[j].TSMs })

	out := make([]Output, 0, 8)
	var (
		lastPointTime int64 = -1
		prevPointTime int64 = -1
		lastValue     float64
		isFirstValue  = true
		windowStart   int64
		haveWindow    bool
		wMin, wMax    Point
		haveExtrema   bool
		nulls         []int64
	)

	for _, s := range points {
		raw := s.Value
		ts := s.TSMs

		// 1) Gap detection, on raw timestamps, before anything can consume
		//    or drop this sample.
		if lastPointTime != -1 && spec.GapMs != 0 && ts-lastPointTime > spec.GapMs {
			nulls = append(nulls, ts-1)
		}

		// 2) Duplicate-timestamp drop. lastPointTime is updated before the
		//    skip, so a run of duplicates cannot mask a later real gap.
		prevPointTime = lastPointTime
		lastPointTime = ts
		if prevPointTime == ts {
			continue
		}

		// 3) Raw capture: the value stage 6 may fall back to.
		val := raw

		// 4) Delta conversion. The first accepted sample seeds the previous
		//    value and emits nothing; lastValue is always the raw previous
		//    sample, never a delta and never a negated value.
		if spec.Delta {
			if isFirstValue {
				isFirstValue = false
				lastValue = raw
				continue
			}
			val = raw - lastValue
			lastValue = raw
		}

		// 5) Negate.
		if spec.Negate {
			val = -val
		}

		// 6) Clamp, with the raw-value escape hatch for counter resets.
		if spec.ClampMin != nil && val < *spec.ClampMin {
			if spec.ClampElseRaw {
				val = raw
			} else {
				val = *spec.ClampMin
			}
		}
		if spec.ClampMax != nil && val > *spec.ClampMax {
			if spec.ClampElseRaw {
				val = raw
			} else {
				val = *spec.ClampMax
			}
		}

		// 7) Rate normalisation, on the raw inter-sample interval, so the
		//    reported rate does not depend on where a window boundary fell.
		//    The first accepted sample has no interval to divide by, so
		//    there is no rate to report: it is dropped rather than emitted
		//    at the un-normalised scale, which would draw as a spike.
		//    (With DELTA the first sample is already consumed above.)
		if spec.PerSecond {
			if prevPointTime < 0 {
				continue
			}
			if tr := float64(lastPointTime-prevPointTime) / 1000; tr > 0 {
				val /= tr
			}
		}

		// 9) Window start bootstrap (before the boundary test, so the first
		//    accepted sample never triggers a spurious flush).
		if !haveWindow {
			windowStart = ts
			haveWindow = true
		}
		// 8) Window boundary. Strict >, so the crossing sample opens the
		//    new window and w == 0 degenerates to one sample per window.
		if ts-windowStart > window {
			out = append(out, emitWindow(wMin, wMax, haveExtrema, nulls, spec.SSE)...)
			windowStart = ts
			nulls = nulls[:0]
			haveExtrema = false
		}

		// 10) Min/max accumulation with strict comparisons, so the earliest
		//     sample holding an extreme value wins and the leading edge of a
		//     plateau survives.
		if !haveExtrema {
			wMin = Point{val, ts}
			wMax = Point{val, ts}
			haveExtrema = true
		} else {
			if val < wMin.Value {
				wMin = Point{val, ts}
			}
			if val > wMax.Value {
				wMax = Point{val, ts}
			}
		}
	}

	// Tail flush.
	if haveExtrema {
		out = append(out, emitWindow(wMin, wMax, haveExtrema, nulls, spec.SSE)...)
	}

	// Post-downsample singular-series extension: a lone point is invisible
	// on a line chart, so it is wrapped unless the field opted out.
	if len(out) == 1 && !out[0].Null {
		if lo, hi, ok := ssePair(spec.SSE, out[0]); ok {
			out = []Output{lo, out[0], hi}
		}
	}

	// Trailing connect-break: the declared cadence was missed between the
	// last sample and the end of the range, so the line stops there
	// rather than running to the edge of the panel. The break is placed
	// at the moment the cadence was first missed, and only if it is
	// strictly later than everything already emitted, because points must
	// stay strictly increasing in time (C1).
	//
	// A series that emitted nothing gets nothing. lastPointTime is set
	// before the DELTA and RATE stages consume the first sample, so a
	// series holding a single sample used to render as one lone null: a
	// break drawn just after a moment when data did arrive, whose real
	// cause is that a rate needs two samples. Saying nothing is the
	// honest answer.
	if len(out) > 0 && spec.GapMs != 0 && spec.EndMs > 0 && lastPointTime != -1 && spec.EndMs-lastPointTime > spec.GapMs {
		at := lastPointTime + spec.GapMs
		if at > out[len(out)-1].TSMs {
			out = append(out, Output{TSMs: at, Null: true})
		}
	}
	return out
}

// emitWindow flushes one window: at most one null per classification slot,
// the extrema in chronological order, and singular-series padding wherever
// a real point would otherwise render as a zero-length mark between
// connect-breaks.
func emitWindow(wMin, wMax Point, have bool, nulls []int64, sse SSE) []Output {
	if !have {
		return nil
	}
	// Classify nulls into before / mid / after. Only the last of each slot
	// survives: at a wide zoom, dozens of outages collapse into a handful
	// of visually distinct breaks, and zooming in recovers each one.
	before, mid, after := int64(-1), int64(-1), int64(-1)
	lo, hi := wMin.TSMs, wMax.TSMs
	if lo > hi {
		lo, hi = hi, lo
	}
	for _, n := range nulls {
		switch {
		case n < lo:
			before = n
		case n > hi:
			after = n
		case n > lo && n < hi:
			mid = n
		default:
			// The break coincides with an extremum's own timestamp.
			// Emitting both would put two points at one instant and
			// break the strictly-increasing-time guarantee (C1), so the
			// real sample wins and the break is dropped.
		}
	}

	dps := make([]Output, 0, 5)
	if before > -1 {
		dps = append(dps, Output{TSMs: before, Null: true})
	}
	// Chronologically earlier extremum first: a line chart interpolates
	// between successive points, so emitting the later one first would draw
	// a slope that never existed.
	if wMin.TSMs < wMax.TSMs {
		dps = append(dps, Output{TSMs: wMin.TSMs, Value: wMin.Value})
	} else {
		dps = append(dps, Output{TSMs: wMax.TSMs, Value: wMax.Value})
	}
	if mid > -1 {
		dps = append(dps, Output{TSMs: mid, Null: true})
	}
	if wMin.TSMs > wMax.TSMs {
		dps = append(dps, Output{TSMs: wMin.TSMs, Value: wMin.Value})
	} else if wMin.TSMs < wMax.TSMs {
		dps = append(dps, Output{TSMs: wMax.TSMs, Value: wMax.Value})
	}
	if after > -1 {
		dps = append(dps, Output{TSMs: after, Null: true})
	}
	return padAgainstNulls(dps, sse)
}

// padAgainstNulls splices singular-series padding beside any real point
// whose neighbour on that side is a connect-break, so the point draws as a
// segment rather than a zero-length mark.
//
// Padding timestamps are clamped into the gap between the point and its
// neighbours. The whitepaper writes the offsets as a flat +/-500 ms; a flat
// offset can land on the far side of a null injected at ts-1 and break the
// strictly-increasing-time guarantee (C1) that the same paper asserts.
// Clamping keeps both properties: the padding is as wide as it can be
// without reordering anything.
func padAgainstNulls(dps []Output, sse SSE) []Output {
	if sse.Mode == SSEOff || len(dps) == 0 {
		return dps
	}
	out := make([]Output, 0, len(dps)+4)
	for i, p := range dps {
		if p.Null {
			out = append(out, p)
			continue
		}
		// SSE REPEAT repeats the value of the point being padded, not one
		// fixed value for the window: padding the maximum with the
		// minimum's value would draw a step that never happened.
		value := sse.Value
		if sse.Mode == SSERepeat {
			value = p.Value
		}
		leftIsNull := i > 0 && dps[i-1].Null
		rightIsNull := i+1 < len(dps) && dps[i+1].Null
		if leftIsNull {
			ts := p.TSMs - ssePadMs
			if lower := dps[i-1].TSMs + 1; ts < lower {
				ts = lower
			}
			if ts < p.TSMs {
				out = append(out, Output{TSMs: ts, Value: value})
			}
		}
		out = append(out, p)
		if rightIsNull {
			ts := p.TSMs + ssePadMs
			if upper := dps[i+1].TSMs - 1; ts > upper {
				ts = upper
			}
			if ts > p.TSMs {
				out = append(out, Output{TSMs: ts, Value: value})
			}
		}
	}
	return out
}

// ssePair builds the two padding points around a lone real point.
func ssePair(sse SSE, p Output) (Output, Output, bool) {
	if sse.Mode == SSEOff {
		return Output{}, Output{}, false
	}
	v := sse.Value
	if sse.Mode == SSERepeat {
		v = p.Value
	}
	return Output{TSMs: p.TSMs - ssePadMs, Value: v},
		Output{TSMs: p.TSMs + ssePadMs, Value: v}, true
}
