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
	return w * 2
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
		if spec.PerSecond && prevPointTime >= 0 {
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
	return padAgainstNulls(dps, wMin, sse)
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
func padAgainstNulls(dps []Output, ref Point, sse SSE) []Output {
	if sse.Mode == SSEOff || len(dps) == 0 {
		return dps
	}
	value := sse.Value
	if sse.Mode == SSERepeat {
		value = ref.Value
	}
	out := make([]Output, 0, len(dps)+4)
	for i, p := range dps {
		if p.Null {
			out = append(out, p)
			continue
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
