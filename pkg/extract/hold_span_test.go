package extract

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
)

const holdSpanSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_ms
          regex: '^\d+'
      anchor: prefix
      strip: true
    framing:
      multiline:
        - start_contains: 'BEGIN'
          continue_regex: 'CONT'
          idle_timeout: 2s
          join:
            - regex: 'CONT (?P<c>.*)$'
              capture: 1
        - start_contains: 'OPEN'
          continue_regex: 'MORE'
          idle_timeout: 2s
          join:
            - regex: 'MORE (?P<c>.*)$'
              capture: 1
    labels: [k]
    patterns:
      - set: agg
        search: 'AGG'
        extract: ['AGG k=(?P<k>\S+) v=(?P<v>\d+)']
        aggregate: {every: 10s, on: [k], field: v, mode: sum}
      - set: plain
        search: 'PLAIN'
        extract: ['PLAIN n=(?P<n>\d+)']
`

func holdSpanStream(t *testing.T) (*Spec, *Stream) {
	t.Helper()
	sp, err := Parse([]byte(holdSpanSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := sp.NewStream(sp.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	return sp, st
}

// A buffered multiline record is processed under the mark of the line
// that *opened* it, so it can be folded into an aggregation window that a
// later record opened -- the window's span then has to reach back over it.
//
// Only the forward half of the span was recorded, so the window began
// after bytes part of it came from: HeldFrom reported a floor past the
// buffered record, the driver acknowledged it, and a replay from there met
// the continuation lines with no buffer open and rebuilt the window
// without that record's value. Nothing had delivered the window yet, so
// the contribution was simply lost, and the number the store ended up
// holding was one the source never reported.
func TestAWindowSpanReachesBackOverABufferedRecord(t *testing.T) {
	_, st := holdSpanStream(t)
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).UnixMilli()

	// The multiline record opens at offset 0 and is completed at 100; its
	// value only exists once the two lines are joined.
	st.Mark(0)
	if _, err := st.Process(fmt.Sprintf("%d BEGIN AGG k=a ", base)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	st.Mark(100)
	if _, err := st.Process(fmt.Sprintf("%d CONT v=5", base+1000)); err != nil {
		t.Fatalf("cont: %v", err)
	}
	// A plain aggregating record at 200 opens the window.
	st.Mark(200)
	if _, err := st.Process(fmt.Sprintf("%d AGG k=a v=100", base+2000)); err != nil {
		t.Fatalf("agg: %v", err)
	}
	if at, held := st.HeldFrom(); !held || at != 0 {
		t.Fatalf("HeldFrom = (%d, %v) while the multiline record is still open, want (0, true)", at, held)
	}
	// The next start marker flushes the buffered record, which folds its
	// value into the window opened at 200.
	st.Mark(300)
	if _, err := st.Process(fmt.Sprintf("%d BEGIN AGG k=a ", base+3000)); err != nil {
		t.Fatalf("flush: %v", err)
	}
	at, held := st.HeldFrom()
	if !held {
		t.Fatal("HeldFrom reports nothing held while a window built from offset 0 is open")
	}
	if at != 0 {
		t.Fatalf("HeldFrom = %d after a record at offset 0 was folded into the window; a replay from there cannot rebuild it", at)
	}
	// And the fold really happened: the window carries both values.
	out, _ := st.Flush()
	var total int64
	for _, r := range out {
		if r.Set != "agg" {
			continue
		}
		v, ok := r.Fields["v"]
		if !ok {
			t.Fatalf("aggregated row carries no v: %+v", r)
		}
		n, _ := v.AsInt()
		total += n
	}
	if total != 105 {
		t.Fatalf("aggregated total is %d, want 105 (100 from the plain record, 5 from the buffered one)", total)
	}
}

// The same span, reached through the idle flush rather than through the
// next start marker: a quiet stream flushes its buffered record on the
// timer, and the fold is identical.
func TestAnIdleFlushedRecordAlsoWidensTheWindowSpan(t *testing.T) {
	_, st := holdSpanStream(t)
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).UnixMilli()

	st.Mark(0)
	if _, err := st.Process(fmt.Sprintf("%d BEGIN AGG k=a ", base)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	st.Mark(100)
	if _, err := st.Process(fmt.Sprintf("%d CONT v=7", base+1000)); err != nil {
		t.Fatalf("cont: %v", err)
	}
	st.Mark(200)
	if _, err := st.Process(fmt.Sprintf("%d AGG k=a v=1", base+2000)); err != nil {
		t.Fatalf("agg: %v", err)
	}
	// Past the rule's own idle timeout, but well short of the window's
	// `every`, so the buffered record is flushed and the window is not.
	if _, errs := st.FlushIdle(time.Now().Add(3 * time.Second)); len(errs) > 0 {
		t.Fatalf("idle flush: %v", errs)
	}
	if at, held := st.HeldFrom(); !held || at != 0 {
		t.Fatalf("HeldFrom = (%d, %v) after the idle flush folded a record from offset 0, want (0, true)", at, held)
	}
}

// holdSpanKey is the content identity of one extracted sample: two runs
// that produce the same samples produce the same keys, and a window
// rebuilt from fewer records does not.
func holdSpanKey(r Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s|%d|", r.Set, r.TSMs)
	keys := make([]string, 0, len(r.Labels))
	for k := range r.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s,", k, r.Labels[k])
	}
	b.WriteByte('|')
	keys = keys[:0]
	for k := range r.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s,", k, r.Fields[k].String())
	}
	return b.String()
}

// replayFrom feeds every record at or past `from` through a fresh stream,
// exactly as a driver resuming from a checkpoint would, and reports the
// samples it produced.
func replayFrom(t *testing.T, sp *Spec, lines []string, marks []int64, from int64, idle map[int]bool) map[string]int {
	t.Helper()
	st, err := sp.NewStream(sp.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	out := map[string]int{}
	for i, line := range lines {
		if marks[i] < from {
			continue
		}
		st.Mark(marks[i])
		res, _ := st.Process(line)
		for _, r := range res {
			out[holdSpanKey(r)]++
		}
		if idle[i] {
			flushed, _ := st.FlushIdle(time.Now().Add(3 * time.Second))
			for _, r := range flushed {
				out[holdSpanKey(r)]++
			}
		}
		st.HeldFrom()
	}
	flushed, _ := st.Flush()
	for _, r := range flushed {
		out[holdSpanKey(r)]++
	}
	return out
}

// The floor HeldFrom reports is a promise: a driver that acknowledges it
// and then crashes must be able to rebuild everything it had not
// delivered by replaying from there. This walks randomised streams of
// multiline, aggregating and plain records, crashes after every one of
// them, and checks that promise against the uninterrupted run.
func TestHeldFloorSurvivesACrashAtEveryRecord(t *testing.T) {
	sp, err := Parse([]byte(holdSpanSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	rnd := rand.New(rand.NewSource(11))
	for iter := 0; iter < 60; iter++ {
		n := 5 + rnd.Intn(30)
		lines := make([]string, 0, n)
		marks := make([]int64, 0, n)
		idle := map[int]bool{}
		mark := int64(0)
		ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < n; i++ {
			ts = ts.Add(time.Duration(1+rnd.Intn(8)) * time.Second)
			var line string
			switch rnd.Intn(6) {
			case 0:
				line = fmt.Sprintf("%d AGG k=k%d v=%d", ts.UnixMilli(), rnd.Intn(3), rnd.Intn(100))
			case 1:
				line = fmt.Sprintf("%d PLAIN n=%d", ts.UnixMilli(), rnd.Intn(100))
			case 2:
				line = fmt.Sprintf("%d BEGIN AGG k=k%d ", ts.UnixMilli(), rnd.Intn(3))
			case 3:
				line = fmt.Sprintf("%d CONT v=%d", ts.UnixMilli(), rnd.Intn(100))
			case 4:
				line = fmt.Sprintf("%d OPEN AGG k=k%d ", ts.UnixMilli(), rnd.Intn(3))
			default:
				line = fmt.Sprintf("%d MORE v=%d", ts.UnixMilli(), rnd.Intn(100))
			}
			if rnd.Intn(8) == 0 {
				idle[i] = true
			}
			lines = append(lines, line)
			marks = append(marks, mark)
			mark += int64(len(line)) + 1
		}
		whole := replayFrom(t, sp, lines, marks, 0, idle)

		for crash := 0; crash < len(lines); crash++ {
			st, err := sp.NewStream(sp.Profiles[0], StreamOptions{})
			if err != nil {
				t.Fatalf("stream: %v", err)
			}
			// Everything delivered up to the crash is durable; the
			// checkpoint is whatever the floor said at that moment.
			durable := map[string]int{}
			var floor int64
			for i := 0; i <= crash; i++ {
				st.Mark(marks[i])
				res, _ := st.Process(lines[i])
				for _, r := range res {
					durable[holdSpanKey(r)]++
				}
				if idle[i] {
					flushed, _ := st.FlushIdle(time.Now().Add(3 * time.Second))
					for _, r := range flushed {
						durable[holdSpanKey(r)]++
					}
				}
				at, held := st.HeldFrom()
				if !held {
					at = marks[i] + int64(len(lines[i])) + 1
				}
				floor = at
			}
			replay := replayFrom(t, sp, lines, marks, floor, idle)
			for key := range whole {
				if durable[key] == 0 && replay[key] == 0 {
					t.Fatalf("iter %d, crash after record %d, floor %d: %q was produced by the uninterrupted run and by neither the durable set nor the replay",
						iter, crash, floor, key)
				}
			}
		}
	}
	_ = model.Int
}
