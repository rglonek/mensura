package extract

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"
)

const framingBase = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    framing:
%s
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`

// A framing bound that is declared and then silently ignored is worse
// than one that is rejected: every reader of max_record_bytes tests
// `n > 0`, so a negative value removed the cap the operator was trying to
// set, and a negative multiline idle_timeout flushed every buffered
// record on the next tick instead of joining anything.
func TestFramingBoundsAreValidated(t *testing.T) {
	cases := []struct {
		name    string
		framing string
		want    string
	}{
		{"negative record cap", "      max_record_bytes: -1\n", "max_record_bytes"},
		{
			"negative idle timeout",
			"      multiline:\n        - start_contains: 'BEGIN'\n          continue_regex: '^ '\n          idle_timeout: -5s\n",
			"idle_timeout",
		},
		{
			"zero idle timeout",
			"      multiline:\n        - start_contains: 'BEGIN'\n          continue_regex: '^ '\n          idle_timeout: 0s\n",
			"idle_timeout",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(strings.Replace(framingBase, "%s", c.framing, 1)))
			if err == nil {
				t.Fatalf("the spec compiled; a declared bound that does nothing must be refused")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not name %s", err, c.want)
			}
		})
	}
}

// The ordinary shapes still compile: unset means "take the default".
func TestFramingBoundsAcceptTheOrdinaryShapes(t *testing.T) {
	for _, framing := range []string{
		"      record: line\n",
		"      max_record_bytes: 4096\n",
		"      multiline:\n        - start_contains: 'BEGIN'\n          continue_regex: '^ '\n",
		"      multiline:\n        - start_contains: 'BEGIN'\n          continue_regex: '^ '\n          idle_timeout: 5s\n",
	} {
		if _, err := Parse([]byte(strings.Replace(framingBase, "%s", framing, 1))); err != nil {
			t.Errorf("framing %q was refused: %v", framing, err)
		}
	}
}

const replaySpec = `
version: 1
profiles:
  - name: p
    select: {}
    labels: [op]
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    patterns:
      - set: ops
        search: 'op='
        extract: ['op=(?P<op>\w+)']
        aggregate: {every: 10s, on: [op], field: hits, mode: increment}
      - set: raw
        search: 'v='
        extract: ['v=(?P<v>\d+)']
`

func resultKey(r Result) string {
	ls := make([]string, 0, len(r.Labels))
	for k, v := range r.Labels {
		ls = append(ls, k+"="+v)
	}
	sort.Strings(ls)
	fs := make([]string, 0, len(r.Fields))
	for k, v := range r.Fields {
		fs = append(fs, k+"="+v.String())
	}
	sort.Strings(fs)
	return fmt.Sprintf("%s@%d %v %v", r.Set, r.TSMs, ls, fs)
}

// replayHarness runs a line stream through a stream, marking each record
// with its index, and reports what came out.
type replayHarness struct {
	t     *testing.T
	spec  *Spec
	lines []string
}

func (h *replayHarness) mark(i int) int64 { return int64(i) * 100 }

func (h *replayHarness) run(from, to int, flush bool) ([]Result, int64, bool) {
	h.t.Helper()
	st, err := h.spec.NewStream(h.spec.Profiles[0], StreamOptions{})
	if err != nil {
		h.t.Fatalf("stream: %v", err)
	}
	var out []Result
	for i := from; i < to; i++ {
		st.Mark(h.mark(i))
		r, _ := st.Process(h.lines[i])
		out = append(out, r...)
	}
	at, held := st.HeldFrom()
	if flush {
		flushed, _ := st.Flush()
		out = append(out, flushed...)
	}
	return out, at, held
}

// crashAt simulates losing the process after line k-1: everything up to
// there was delivered, and the next start re-reads from the offset
// HeldFrom named. The union has to be exactly what an uninterrupted run
// produced -- a missing result is data loss, and an extra one is a row
// nobody measured.
func (h *replayHarness) crashAt(k int, want map[string]bool) (missing, extra []string) {
	h.t.Helper()
	emitted, at, held := h.run(0, k, false)
	resume := k
	if held {
		resume = int(at / 100)
	}
	replayed, _, _ := h.run(resume, len(h.lines), true)

	got := map[string]bool{}
	for _, r := range emitted {
		got[resultKey(r)] = true
	}
	for _, r := range replayed {
		got[resultKey(r)] = true
	}
	for key := range want {
		if !got[key] {
			missing = append(missing, key)
		}
	}
	for key := range got {
		if !want[key] {
			extra = append(extra, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}

// Aggregation windows for different keys interleave, and one closing
// while another is open used to let the hold floor rise into the middle
// of the closed window's records. A restart from there did not rebuild
// that window: those records opened a new one at the wrong start
// timestamp, so the store gained a partial row nobody measured and lost
// the record that should have opened the next real window.
func TestAggregationReplayFromHeldFromIsLossless(t *testing.T) {
	spec, err := Parse([]byte(replaySpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	h := &replayHarness{t: t, spec: spec, lines: []string{
		fmt.Sprintf("%d op=a", base+0),     // opens a's window [0s, 10s)
		fmt.Sprintf("%d op=a", base+1000),  //
		fmt.Sprintf("%d op=b", base+2000),  // opens b's window [2s, 12s)
		fmt.Sprintf("%d op=a", base+3000),  // still a's first window
		fmt.Sprintf("%d op=a", base+11000), // closes a's, opens the next
		fmt.Sprintf("%d op=b", base+13000), // closes b's, opens the next
		fmt.Sprintf("%d op=a", base+21000), //
	}}
	full, _, _ := h.run(0, len(h.lines), true)
	want := map[string]bool{}
	for _, r := range full {
		want[resultKey(r)] = true
	}
	if len(want) != 5 {
		t.Fatalf("the fixture produced %d windows, want 5: %v", len(want), want)
	}
	for k := 1; k <= len(h.lines); k++ {
		missing, extra := h.crashAt(k, want)
		if len(missing) > 0 || len(extra) > 0 {
			t.Errorf("crash after line %d:\n  missing %v\n  extra   %v", k-1, missing, extra)
		}
	}
}

// The same property over randomised streams, where windows for several
// keys open, close and overlap in every order.
func TestAggregationReplayIsLosslessOverRandomStreams(t *testing.T) {
	spec, err := Parse([]byte(replaySpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	ops := []string{"a", "b", "c"}
	for seed := int64(0); seed < 60; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			h := &replayHarness{t: t, spec: spec}
			ts := base
			for i := 0; i < 24; i++ {
				ts += int64(rng.Intn(9)) * 1000
				if rng.Intn(6) == 0 {
					h.lines = append(h.lines, fmt.Sprintf("%d v=%d", ts, i))
					continue
				}
				h.lines = append(h.lines, fmt.Sprintf("%d op=%s", ts, ops[rng.Intn(len(ops))]))
			}
			full, _, _ := h.run(0, len(h.lines), true)
			want := map[string]bool{}
			for _, r := range full {
				want[resultKey(r)] = true
			}
			for k := 1; k <= len(h.lines); k++ {
				missing, extra := h.crashAt(k, want)
				if len(missing) > 0 || len(extra) > 0 {
					t.Fatalf("crash after line %d of %v:\n  missing %v\n  extra   %v", k-1, h.lines, missing, extra)
				}
			}
		})
	}
}

// Holding an emitted window's span must not turn into holding every
// window a long-lived stream ever opened.
func TestHoldListStaysBoundedWithInterleavedKeys(t *testing.T) {
	spec, err := Parse([]byte(replaySpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := spec.NewStream(spec.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	ops := []string{"a", "b", "c", "d"}
	for i := 0; i < 4000; i++ {
		st.Mark(int64(i) * 100)
		if _, err := st.Process(fmt.Sprintf("%d op=%s", base+int64(i)*3000, ops[i%len(ops)])); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		st.HeldFrom()
	}
	if len(st.holds) > 2*len(st.aggs)+16 {
		t.Fatalf("the hold list holds %d entries for %d open window(s)", len(st.holds), len(st.aggs))
	}
}

const replayMultilineSpec = `
version: 1
profiles:
  - name: p
    select: {}
    labels: [op]
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    framing:
      multiline:
        - start_contains: 'BEGIN'
          continue_regex: '^\s*\+'
          join:
            - regex: '^\s*\+(.*)$'
              capture: 1
          idle_timeout: 1h
    patterns:
      - set: blocks
        search: 'BEGIN'
        extract: ['BEGIN t=(?P<t>\d+)']
      - set: ops
        search: 'op='
        extract: ['op=(?P<op>\w+)']
        aggregate: {every: 10s, on: [op], field: hits, mode: increment}
`

// A multiline record that has been assembled and emitted spans several
// lines, and an aggregation window opened between them used to let the
// hold floor settle inside that span. A replay from there met each
// continuation line with no buffer open, so instead of belonging to the
// record it was judged on its own -- and a continuation that a pattern
// happens to match becomes a whole sample the source never reported.
func TestMultilineReplayFromHeldFromIsLossless(t *testing.T) {
	spec, err := Parse([]byte(replayMultilineSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	h := &replayHarness{t: t, spec: spec, lines: []string{
		fmt.Sprintf("%d BEGIN t=1", base+0),    // opens the buffered record
		fmt.Sprintf("%d +one", base+1000),      // continuation
		fmt.Sprintf("%d op=b", base+2000),      // opens b's window, which stays open
		fmt.Sprintf("%d +op=c", base+3000),     // continuation a pattern would match
		fmt.Sprintf("%d BEGIN t=2", base+4000), // flushes the record spanning 0..3
		fmt.Sprintf("%d +three", base+5000),    //
	}}
	full, _, _ := h.run(0, len(h.lines), true)
	want := map[string]bool{}
	for _, r := range full {
		want[resultKey(r)] = true
	}
	for _, key := range []string{"blocks@1787918400000 [] [t=1]", "ops@1787918402000 [op=b] [hits=1]"} {
		if !want[key] {
			t.Fatalf("the fixture did not produce %s; it produced %v", key, want)
		}
	}
	for k := 1; k <= len(h.lines); k++ {
		missing, extra := h.crashAt(k, want)
		if len(missing) > 0 || len(extra) > 0 {
			t.Errorf("crash after line %d:\n  missing %v\n  extra   %v", k-1, missing, extra)
		}
	}
}

// The same property over randomised streams that mix multiline records,
// aggregation across several keys and plain records.
func TestMultilineReplayIsLosslessOverRandomStreams(t *testing.T) {
	spec, err := Parse([]byte(replayMultilineSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	ops := []string{"a", "b", "c"}
	for seed := int64(0); seed < 60; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			h := &replayHarness{t: t, spec: spec}
			ts := base
			for i := 0; i < 24; i++ {
				ts += int64(rng.Intn(9)) * 1000
				switch rng.Intn(4) {
				case 0:
					h.lines = append(h.lines, fmt.Sprintf("%d BEGIN t=%d", ts, i))
				case 1:
					// A continuation that also matches a pattern on its own.
					h.lines = append(h.lines, fmt.Sprintf("%d +op=%s", ts, ops[rng.Intn(len(ops))]))
				case 2:
					h.lines = append(h.lines, fmt.Sprintf("%d +plain%d", ts, i))
				default:
					h.lines = append(h.lines, fmt.Sprintf("%d op=%s", ts, ops[rng.Intn(len(ops))]))
				}
			}
			full, _, _ := h.run(0, len(h.lines), true)
			want := map[string]bool{}
			for _, r := range full {
				want[resultKey(r)] = true
			}
			for k := 1; k <= len(h.lines); k++ {
				missing, extra := h.crashAt(k, want)
				if len(missing) > 0 || len(extra) > 0 {
					t.Fatalf("crash after line %d of %v:\n  missing %v\n  extra   %v", k-1, h.lines, missing, extra)
				}
			}
		})
	}
}

// Holding an emitted record's span must not stall the checkpoint. The
// floor lags by the span of the oldest window whose records still reach
// it -- the same order as the window width the design already promises --
// and not by the whole stream.
func TestHoldFloorStillAdvances(t *testing.T) {
	spec, err := Parse([]byte(replayMultilineSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := spec.NewStream(spec.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	// Five keys sharing a 10s window at a record every 250ms, so every
	// window holds many records and their spans overlap heavily.
	ops := []string{"a", "b", "c", "d", "e"}
	const records = 3000
	worst := int64(0)
	for i := 0; i < records; i++ {
		mark := int64(i) * 100
		st.Mark(mark)
		if _, err := st.Process(fmt.Sprintf("%d op=%s", base+int64(i)*250, ops[i%len(ops)])); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		at, held := st.HeldFrom()
		if !held {
			continue
		}
		if lag := mark - at; lag > worst {
			worst = lag
		}
	}
	// One window is 40 records wide per key here; a few of those is the
	// expected shape, the whole stream is a stall.
	if worst > 100*400 {
		t.Errorf("the hold floor lagged %d records behind the read head; it is not advancing", worst/100)
	}
}
