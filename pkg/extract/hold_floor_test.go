package extract

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// referenceHoldFloor is holdFloor written the obvious way: scan every
// entry for the smallest live mark, then scan every entry again for an
// emitted span that contains it. holdFloor is the same answer reached
// without walking the whole list, and the two must agree on every state
// a stream can reach.
func referenceHoldFloor(st *Stream) (int64, bool) {
	oldest, held := int64(0), false
	for _, b := range st.multiline {
		if !held || b.mark < oldest {
			oldest, held = b.mark, true
		}
	}
	for _, e := range st.holds {
		if st.live(e) && (!held || e.mark < oldest) {
			oldest, held = e.mark, true
		}
	}
	if !held {
		return 0, false
	}
	for i := len(st.holds) - 1; i >= 0; i-- {
		e := st.holds[i]
		if e.mark >= oldest || st.live(e) {
			continue
		}
		if e.endMark() >= oldest {
			oldest = e.mark
		}
	}
	return oldest, true
}

const holdFloorSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - {layout: epoch_s, regex: '^[0-9]+'}
      strip: true
    framing:
      multiline:
        - start_contains: 'BEGIN'
          continue_regex: '^ at '
          join:
            - {regex: '^ at (.*)$', capture: 1}
    labels: [op]
    fields:
      lat: {kind: gauge}
      n: {kind: gauge}
    patterns:
      - set: agg
        search: 'LAT'
        extract: [' LAT op=(?P<op>[a-z0-9]+) lat=(?P<lat>[0-9]+)']
        aggregate: {every: 5s, on: [op], field: lat, mode: max}
      - set: ml
        search: 'BEGIN'
        extract: [' BEGIN n=(?P<n>\d+)(?P<trace>.*)$']
`

// The floor decides where a checkpoint may land, so the fast path has to
// produce the same number as the slow one on every state a stream reaches:
// open windows, closed ones whose span still reaches the floor, multiline
// buffers that pull the floor back behind them, and idle flushes.
func TestHoldFloorMatchesReference(t *testing.T) {
	s, err := Parse([]byte(holdFloorSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 200; trial++ {
		st, err := s.NewStream(s.Profiles[0], StreamOptions{RefTime: time.Unix(0, 0)})
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		mark, sec := int64(0), int64(1000)
		for step := 0; step < 60; step++ {
			var line string
			switch rng.Intn(5) {
			case 0:
				line = fmt.Sprintf("%d BEGIN n=%d", sec, step)
			case 1:
				line = fmt.Sprintf("%d at frame-%d", sec, step)
			default:
				line = fmt.Sprintf("%d LAT op=k%d lat=%d", sec, rng.Intn(6), rng.Intn(100))
			}
			st.Mark(mark)
			_, _ = st.Process(line)
			mark += int64(len(line)) + 1
			sec += int64(rng.Intn(4))
			if rng.Intn(9) == 0 {
				_, _ = st.FlushIdle(time.Unix(sec+120, 0))
			}
			wantAt, wantHeld := referenceHoldFloor(st)
			gotAt, gotHeld := st.holdFloor()
			if gotHeld != wantHeld || gotAt != wantAt {
				t.Fatalf("trial %d step %d: holdFloor = (%d, %v), reference = (%d, %v); holds=%d aggs=%d",
					trial, step, gotAt, gotHeld, wantAt, wantHeld, len(st.holds), len(st.aggs))
			}
			// HeldFrom is what the drivers call, and it prunes as it goes.
			if at, held := st.HeldFrom(); held != wantHeld || (held && at != wantAt) {
				t.Fatalf("trial %d step %d: HeldFrom = (%d, %v), want (%d, %v)", trial, step, at, held, wantAt, wantHeld)
			}
		}
	}
}

// holdFloor runs once per record on every offset-checkpointing driver, so
// its cost may not grow with the number of open aggregation windows. It
// used to scan the whole hold list twice, with a map lookup per entry: at
// the maxOpenWindows cap that is milliseconds of scanning per line, which
// is a throughput cliff reached by an `aggregate.on` key with more values
// than its author expected -- the case the cap exists for.
func TestHoldFloorCostDoesNotGrowWithOpenWindows(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	s, err := Parse([]byte(holdFloorSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	cost := func(keys int) time.Duration {
		st, err := s.NewStream(s.Profiles[0], StreamOptions{RefTime: time.Unix(0, 0)})
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		mark := int64(0)
		for i := 0; i < keys; i++ {
			line := fmt.Sprintf("1000 LAT op=k%d lat=%d", i, i)
			st.Mark(mark)
			if _, err := st.Process(line); err != nil {
				t.Fatalf("process: %v", err)
			}
			mark += int64(len(line)) + 1
			st.HeldFrom()
		}
		if len(st.aggs) != keys {
			t.Fatalf("open windows = %d, want %d", len(st.aggs), keys)
		}
		hot := fmt.Sprintf("1000 LAT op=k%d lat=1", keys/2)
		const n = 300
		start := time.Now()
		for i := 0; i < n; i++ {
			st.Mark(mark)
			if _, err := st.Process(hot); err != nil {
				t.Fatalf("process: %v", err)
			}
			mark += int64(len(hot)) + 1
			st.HeldFrom()
		}
		return time.Since(start) / n
	}
	small, large := cost(100), cost(20_000)
	// Two hundred times the open windows must not cost meaningfully more
	// per record. The old shape was linear, so this ratio was ~100x; the
	// bound is loose enough to survive a noisy machine and tight enough
	// that a return to a per-record scan fails it.
	if large > 20*small+50*time.Microsecond {
		t.Fatalf("per-record floor cost grew with the open-window count: %v at 100 windows, %v at 20000", small, large)
	}
}
