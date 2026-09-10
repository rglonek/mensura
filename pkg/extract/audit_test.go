package extract

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
)

// Every name a pattern can put on a row is a column the store validates
// per sample, so a name it refuses is a spec fault that costs one
// rejection per record for the life of the process -- with nothing
// anywhere pointing back at the spec. The regex group names, the field an
// aggregate synthesises and the keys of default_values were the last
// names in a spec that nothing checked.
func TestPatternColumnNamesAreValidated(t *testing.T) {
	const base = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    labels: [op]
    patterns:
      - set: app
%s
`
	cases := []struct {
		name    string
		pattern string
		want    string
	}{
		{
			"capture named timestamp",
			"        search: 'x='\n        extract: ['x=(?P<timestamp>\\d+)']\n",
			"timestamp",
		},
		{
			"aggregate field the store refuses",
			"        search: 'op='\n        extract: ['op=(?P<op>\\w+)']\n        aggregate: {every: 10s, on: [op], field: timestamp, mode: increment}\n",
			"timestamp",
		},
		{
			"default_values key the store refuses",
			"        search: 'x='\n        extract: ['x=(?P<x>\\d+)']\n        default_values: {'bad name': '1'}\n",
			"bad name",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(strings.Replace(base, "%s", c.pattern, 1)))
			if err == nil {
				t.Fatalf("the spec compiled; a column name the store refuses must be a spec error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not name %q", err, c.want)
			}
		})
	}
}

// A valid spec still compiles: the check must not refuse the ordinary
// shapes, including a bucket set's payload groups and a field name that
// starts with a digit.
func TestPatternColumnNamesAcceptTheOrdinaryShapes(t *testing.T) {
	const spec = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    labels: [op]
    bucket_sets:
      - name: h
        parse: paren_pairs
        buckets: ['00','01']
        edges: pow2
        total_field: total
    patterns:
      - set: app
        search: 'hist'
        bucket_set: h
        extract: ['hist (?P<op>\w+) \((?P<total>\d+) total\) (?P<buckets>.*)']
`
	if _, err := Parse([]byte(spec)); err != nil {
		t.Fatalf("compile: %v", err)
	}
}

const auditAggSpec = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    labels: [k]
    patterns:
      - set: app
        search: 'k='
        extract: ['k=(?P<k>\S+)']
        aggregate: {every: 1h, on: [k], field: hits, mode: increment}
`

func auditAggStream(t *testing.T) *Stream {
	t.Helper()
	s, err := Parse([]byte(auditAggSpec))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	st, err := s.NewStream(s.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	return st
}

// A window is one accumulator plus a copy of the opening record's labels
// and fields, and there is one per distinct `on` tuple. Nothing bounded
// that set, so a key whose cardinality the spec author misjudged grew the
// ingester's memory for as long as `every` lasted -- with no cap, no
// counter and no diagnostic. Past the cap the oldest-ending windows are
// emitted early, handed to the caller and counted.
func TestOpenAggregationWindowsAreBounded(t *testing.T) {
	st := auditAggStream(t)
	base := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC).UnixMilli()

	emitted := 0
	// One more record than the cap, each with its own key, all inside one
	// `every` so none can expire on its own.
	for i := 0; i <= maxOpenWindows; i++ {
		out, err := st.Process(fmt.Sprintf("%013d k=key%d", base+int64(i), i))
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		emitted += len(out)
	}
	if got := len(st.aggs); got > maxOpenWindows {
		t.Fatalf("held %d open windows, which is past the cap of %d", got, maxOpenWindows)
	}
	if st.Stats.WindowsForcedClosed == 0 {
		t.Fatal("the cap was reached and nothing counted it")
	}
	if emitted == 0 {
		t.Fatal("a window closed early must be handed to the caller, not dropped")
	}
	if int64(emitted) != st.Stats.WindowsForcedClosed {
		t.Fatalf("emitted %d early-closed window(s) but counted %d", emitted, st.Stats.WindowsForcedClosed)
	}
	// The one that went is the oldest-ending, so key0 is gone and the
	// newest key is still open.
	if _, live := st.aggs[strings.Join([]string{"app", "hits", "increment", "k=key0"}, "\x00")]; live {
		t.Fatal("the oldest window survived the shed")
	}
}

// 03-extraction.md section 9 says aggregation is lossy on purpose and
// that `check` reports how lossy. Nothing measured it.
func TestAggregationReductionIsMeasured(t *testing.T) {
	st := auditAggStream(t)
	base := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < 10; i++ {
		if _, err := st.Process(fmt.Sprintf("%013d k=one", base+int64(i)*1000)); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	if len(st.Stats.Aggregates) != 1 {
		t.Fatalf("expected one aggregating pattern, got %d", len(st.Stats.Aggregates))
	}
	if got := st.Stats.Aggregates[0].Records; got != 10 {
		t.Fatalf("absorbed records: got %d, want 10", got)
	}
	if got := st.Stats.Aggregates[0].Windows; got != 0 {
		t.Fatalf("nothing has closed yet, so no rows: got %d", got)
	}
	out, _ := st.Flush()
	if len(out) != 1 {
		t.Fatalf("expected one row, got %d", len(out))
	}
	a := st.Stats.Aggregates[0]
	if a.Windows != 1 || a.Records != 10 || a.Set != "app" || a.Field != "hits" || a.Mode != "increment" {
		t.Fatalf("reduction: %+v", a)
	}
}

const flushVerdictSpec = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    framing:
      multiline:
        - start_contains: 'BEGIN'
          continue_regex: '^\d{13} \+'
          idle_timeout: 1ms
          join:
            - regex: '^\d{13} \+(.*)$'
              capture: 1
    patterns:
      - set: app
        search: 'MATCHES'
        extract: ['MATCHES (?P<n>\d+)']
`

// A record flushed by a rotation, a retirement, an idle tick or a
// shutdown is judged nowhere else, and the drivers turn a verdict into
// the pipeline's counters. Process was given this back for the flush a
// start marker performs; the other paths went on discarding it, which on
// a low-traffic multiline source is most of them.
func TestFlushReportsTheVerdictOfWhatItFlushes(t *testing.T) {
	s, err := Parse([]byte(flushVerdictSpec))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	base := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC).UnixMilli()

	t.Run("Flush", func(t *testing.T) {
		st, _ := s.NewStream(s.Profiles[0], StreamOptions{})
		if _, err := st.Process(fmt.Sprintf("%013d BEGIN nothing claims this", base)); err != nil {
			t.Fatalf("start marker: %v", err)
		}
		out, verdicts := st.Flush()
		if len(out) != 0 {
			t.Fatalf("the record matches no pattern, so it yields nothing: %v", out)
		}
		if len(verdicts) != 1 || verdicts[0] != ErrNoMatch {
			t.Fatalf("verdicts: %v", verdicts)
		}
	})

	t.Run("FlushIdle", func(t *testing.T) {
		st, _ := s.NewStream(s.Profiles[0], StreamOptions{})
		if _, err := st.Process(fmt.Sprintf("%013d BEGIN nothing claims this", base)); err != nil {
			t.Fatalf("start marker: %v", err)
		}
		_, verdicts := st.FlushIdle(time.Now().Add(time.Hour))
		if len(verdicts) != 1 || verdicts[0] != ErrNoMatch {
			t.Fatalf("verdicts: %v", verdicts)
		}
	})
}

// Flush walks the multiline rules in declared order rather than in map
// order: the samples it produces are keyed by a flush sequence number
// plus their position in the batch, so an order that varies run to run
// keys the same record differently under `key: offset`.
func TestFlushIsOrderedByRule(t *testing.T) {
	const spec = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    framing:
      multiline:
        - start_contains: 'AAA'
          continue_regex: '^\d{13} \+'
          join: [{regex: '^\d{13} \+(.*)$', capture: 1}]
        - start_contains: 'BBB'
          continue_regex: '^\d{13} \+'
          join: [{regex: '^\d{13} \+(.*)$', capture: 1}]
        - start_contains: 'CCC'
          continue_regex: '^\d{13} \+'
          join: [{regex: '^\d{13} \+(.*)$', capture: 1}]
    patterns:
      - set: app
        search: 'v='
        extract: ['v=(?P<v>\w+)']
`
	s, err := Parse([]byte(spec))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	base := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < 50; i++ {
		st, _ := s.NewStream(s.Profiles[0], StreamOptions{})
		for _, marker := range []string{"AAA", "BBB", "CCC"} {
			if _, err := st.Process(fmt.Sprintf("%013d %s v=%s", base, marker, marker)); err != nil {
				t.Fatalf("%s: %v", marker, err)
			}
		}
		out, _ := st.Flush()
		var got []string
		for _, r := range out {
			got = append(got, r.Fields["v"].String())
		}
		if want := []string{"AAA", "BBB", "CCC"}; !equalStrings(got, want) {
			t.Fatalf("flush order %v, want %v", got, want)
		}
	}
	_ = model.KindGauge
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
