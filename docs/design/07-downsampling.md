# 07 — The render pipeline

This is the implementation specification for the per-series post-processing
walk. It restates the whitepaper (Glonek 2026, §3 and Appendix A) as a
component contract, and adds the parts the whitepaper leaves to the
implementation: where it sits in the query path, how it maps onto Grafana data
frames, and how it is tested.

**The whitepaper is normative.** Where this document and the paper disagree,
the paper wins and the difference is a bug in this document.

## 1. Position in the query path

```
scan → group into series → [ per series: sort → WALK → emit ] → frames
```

The walk runs once per series, on an already-buffered, already-sorted slice of
`(value, tsMs)` pairs, and produces the final ordered output the panel draws.
It performs no I/O and re-reads nothing.

Complexity per series of `N` samples: `O(N log N)` for the sort plus `O(N)` for
the walk, with `O(1)` extra state during the walk. Memory is `O(N)` for the
buffered input — the query buffers a full series in order to sort it — plus an
output bounded by `5⌈R/w⌉ + 2`.

## 2. Window sizing

Computed once per series, before the walk:

```
w = min( rangeMs / maxDataPoints, intervalMs ) * 2
```

The inner `min` honours both the panel's render budget and Grafana's own
minimum-interval hint. The doubling is the operationally important part: each
window emits **both** its min and its max, so sizing the window to two points'
worth of budget keeps the output point count at what Grafana asked for while
doubling the information density relative to a one-aggregator rollup.

`w == 0` (very short ranges, integer division) is not special-cased. Because the
boundary test is a strict `>`, every new distinct timestamp opens a new window
and the algorithm degenerates gracefully to "no downsampling" — which is the
correct behaviour when the raw data already fits the budget.

An `EVERY d` clause replaces the computed `w` with `d`.

The doubling belongs to the walk, not to the window. A reduction that
emits one point per window rather than a min/max pair — `FORMAT heatmap`,
which sums bucket counts ([06](06-query.md) §6) — uses the undoubled
width, or it would spend the whole render budget on half the columns the
panel asked for.

## 3. The ten stages

Per raw sample, in chronological order, in exactly this sequence:

1. **Gap detection.** If `lastPointTime != -1`, the field's `gapMs != 0`, and
   `ts - lastPointTime > gapMs`, enqueue a synthetic null at `ts - 1` into the
   current window's null list. Uses **raw** timestamps, so it is invariant
   under every later transform.
2. **Duplicate-timestamp drop.** `prev = last; last = ts; if prev == ts:
   continue`. `last` is updated **before** the skip, so a run of duplicates does
   not mask a real gap on the next distinct sample.
3. **Raw capture.** `raw = sample.value; val = raw`. `raw` survives the whole
   sample's processing and is what stage 6 falls back to.
4. **Delta conversion.** With `DELTA`: the first accepted sample seeds
   `lastValue = raw` and is consumed (`continue`, so it skips window
   accumulation too); later samples emit `raw - lastValue` and set
   `lastValue = raw`. `lastValue` is always the **raw** previous sample, never a
   delta and never a negated value. This invariant is what makes stages 5–7
   safe.
5. **Negate.** With `NEGATE`: `val = -val`.
6. **Clamp with raw fallback.** With limits: if `val < min`, `val` becomes
   `raw` (when `ELSE RAW`) or `min`; likewise for `max`. `ELSE RAW` is the
   counter-reset escape hatch: when a delta briefly goes negative because a
   process restarted, the operator sees the new counter value, which is small,
   legitimate and interpretable, rather than a rendering artefact. Note that it
   undoes both `DELTA` and `NEGATE` — it is a *raw*-value escape, not a partial
   one.
7. **Rate normalisation.** With `PER SECOND`: `tr = (last - prev)/1000; if tr >
   0 { val /= tr }`. Uses raw timestamps, not window-relative ones, so the
   reported rate is the real inter-sample rate regardless of where the window
   boundary falls. It applies to whatever value stage 6 produced, including a
   raw-substituted one.
8. **Window boundary.** If `ts - windowStart > w` (strict `>`), flush the
   current window via `emitWindow`, then reset `windowStart = ts` and clear the
   window state. The boundary-crossing sample belongs to the **new** window.
9. **Window start init.** On the first accepted sample, `windowStart = ts`. No
   global grid alignment: windows are anchored to the first real data point of
   this series, which is what removes the reshuffle-on-zoom instability that
   wall-clock-aligned buckets suffer.
10. **Min/max accumulation.** Compare the transformed `val` and the raw `ts`
    against the running window min and max, with **strict** `<` and `>`, so the
    earliest sample holding an extreme value wins and the leading edge of a
    plateau is preserved.

After the loop: a tail flush of the final partial window, then the
singular-series check.

## 4. `emitWindow`

Receives the window's min point, max point, the null timestamps that fell in
the window, and the SSE setting; returns points in strict chronological order.

Null classification — each null falls into exactly one of three slots:

- **before** — earlier than both extrema;
- **mid** — between the two extrema;
- **after** — later than both.

At most one null per slot survives per window. This is deliberate information
loss: at a seven-day zoom, dozens of outages collapse into a handful of
visually distinct connect-breaks, and zooming in recovers each individually.

Emission order: the chronologically **earlier** extremum first. This is not
cosmetic. A line chart interpolates between successive points; emitting
min-then-max when the max occurred first draws a downward slope that never
existed. Real timestamp order keeps the rendered line a truthful caricature —
peaks up, troughs down, inflections in their real temporal position.

A window whose min and max share a timestamp (one accepted sample) emits one
point.

SSE padding is applied inside `emitWindow` when the window emits exactly one
real point adjacent to nulls (before-only, after-only, between two nulls, or
adjacent to a mid null), so the lone real value renders as a drawable segment
between the connect-breaks rather than a zero-length mark.

## 5. Singular-series extension

A series that reduces to one real point is invisible on a line chart. `SSE`
declares how to synthesise its neighbours at ±500 ms:

| Setting | Result |
| --- | --- |
| numeric constant (default `0`) | segment from the constant, through the real point, back to the constant |
| `REPEAT` | horizontal segment at the real value |
| `OFF` | no padding; the single mark stands alone |

±500 ms is a rendering choice: long enough to be visible at any realistic zoom,
short enough not to mislead about when the event occurred. In-window padding
clamps the offset into the space actually available, because gap detection
injects its null at `ts − 1` and a flat −500 ms point would land on the far
side of it and break C1 ([12-implementation.md §6.5](12-implementation.md)). The padding points
are synthetic by construction (a declared constant or a repeat), never mistaken
for independent measurements.

Invariant P4, all the way from raw record to rendered pixel: *if a series
produced any value in the rendered range, the operator sees it.*

## 6. Reference pseudocode

```text
function renderSeries(series, spec, rangeMs, maxPoints, intervalMs):
    w = spec.everyMs or min(rangeMs / maxPoints, intervalMs) * 2
    sort(series, by ts)

    out=[]; last=-1; lastValue=0; first=true
    windowStart=0; wMin=[]; wMax=[]; nulls=[]

    for s in series:
        raw = s.value; ts = s.ts

        if last != -1 and spec.gapMs != 0 and ts - last > spec.gapMs:      # 1
            nulls.append(ts - 1)

        prev = last; last = ts                                             # 2
        if prev == ts: continue

        val = raw                                                          # 3

        if spec.delta:                                                     # 4
            if first: first = false; lastValue = raw; continue
            val = raw - lastValue; lastValue = raw

        if spec.negate: val = -val                                         # 5

        if spec.clamp:                                                     # 6
            if spec.clamp.min != nil and val < spec.clamp.min:
                val = spec.clamp.elseRaw ? raw : spec.clamp.min
            if spec.clamp.max != nil and val > spec.clamp.max:
                val = spec.clamp.elseRaw ? raw : spec.clamp.max

        if spec.perSecond:                                                 # 7
            tr = (last - prev) / 1000
            if tr > 0: val = val / tr

        if windowStart == 0: windowStart = ts                              # 9
        if ts - windowStart > w:                                           # 8
            out += emitWindow(wMin, wMax, nulls, spec.sse)
            windowStart = ts; wMin = []; wMax = []; nulls = []

        if wMin is empty or val < wMin.value: wMin = [val, ts]             # 10
        if wMax is empty or val > wMax.value: wMax = [val, ts]

    if wMin is not empty:
        out += emitWindow(wMin, wMax, nulls, spec.sse)

    if len(out) == 1:
        pad = sse(spec.sse, out[0])
        if pad != nil: out = [pad[0], out[0], pad[1]]

    return out
```

(Stages 8 and 9 are written in the order the whitepaper's Appendix A uses:
`windowStart` bootstrap precedes the boundary test, so the first accepted
sample never triggers a spurious flush.)

## 7. Correctness contract

The eight properties from the whitepaper §6 are the acceptance criteria for the
implementation, each with a dedicated property test:

| | Property | Test |
| --- | --- | --- |
| C1 | Output timestamps are strictly increasing | property test over random series |
| C2 | No false continuity: any raw gap wider than `gapMs` yields a null at `ts-1`, including a *trailing* gap between the last sample and the end of the range, which yields a null at `last + gapMs` | property test with injected gaps |
| C3 | `ELSE RAW` substitutes the raw sample, and `PER SECOND` may still divide it | table test per flag combination |
| C4 | Both window extrema appear, at their original raw timestamps | property test comparing against a brute-force per-window min/max |
| C5 | Ties are won by the earliest sample | table test with plateaus |
| C6 | Any series with an accepted sample yields ≥ 3 points, unless `SSE OFF` | property test |
| C7 | Duplicate timestamps do not mask a later real gap | table test with duplicate runs |
| C8 | `w == 0` emits every accepted sample, in order | property test on short ranges |

Additionally: **determinism** — the same input always produces byte-identical
output, so two operators looking at the same dashboard at the same moment see
the same plot. Enforced by a golden-file suite over recorded real series.

## 8. Mapping onto Grafana data frames

The whitepaper's output contract is the SimpleJson `[value, ts]` list, where
`[null, ts]` means connect-break. The native plugin emits data frames instead,
so the mapping is stated explicitly:

- **One frame per series**, each with a `time` field and a nullable `float64`
  value field. Not a wide frame: windows are anchored per series, so two series
  in the same panel do not share a time axis, and forcing them into one wide
  frame would require re-interpolation — exactly the falsification the pipeline
  exists to prevent.
- A null marker becomes a row whose value field is `nil`. The frame sets
  `fieldConfig.custom.spanNulls = false`, so Grafana renders the connect-break.
  A datasource-level toggle can flip that default, and doing so is described in
  the docs as "hide outages", because that is what it does.
- Field config carries the unit hint from the catalogue, and the frame's `meta`
  carries `executedQueryString` (the canonical MQL text) so the panel inspector
  shows exactly what ran.
- Series name goes into the value field's `DisplayNameFromDS`, so Grafana's
  legend, colour assignment and overrides behave natively.
- Frames are returned sorted by series name, so colour assignment is stable
  across reloads.

## 9. What this pipeline deliberately does not do

- **No cross-series arithmetic at render time.** Per-series window anchoring
  makes `a / b` ill-defined. If it is needed, compute it at ingest.
- **No user-supplied aggregators.** Min and max per window are the contract.
- **A trailing gap is a gap.** A null is injected when a later sample arrives,
  and also at `last + gapMs` when the range ends more than `gapMs` after the
  last sample, so a series that stops mid-range draws a connect-break rather
  than ending at its last point. `spec.EndMs` carries the range end; it is left
  at zero for alert evaluation, where no synthetic point may contribute.
- **No interpolation across gaps, ever.**
- **No smoothing of counter-reset recovery.** The jump from delta values to a
  raw counter reading is visually abrupt, and abrupt is correct: every smoothed
  alternative hid information an on-call operator needed.
- **No SIMD.** The walk is branchy, data-dependent, and already runs in the
  shadow of the LSM scan; a vectorised version would spend its nominal gain on
  mask management and would be harder to audit against §7.
