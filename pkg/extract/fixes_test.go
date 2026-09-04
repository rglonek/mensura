package extract

import (
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
)

// A nanosecond value declared as epoch_s must not be multiplied on trust.
//
// n*1000 overflows int64 and wraps to a timestamp that is not the one in
// the record -- sometimes past model.MaxTSMs, which is caught, and
// sometimes back inside it, which is not. The realistic source is a spec
// declaring the wrong unit, so it is reported as a timestamp failure:
// counted and dropped, rather than stored at a fabricated time.
func TestEpochSecondsRefusesAValueThatCannotBeSeconds(t *testing.T) {
	// A plausible epoch-nanosecond reading. Held in a variable, not a
	// constant: the multiplication below is the overflow under test, and
	// the compiler refuses to fold it.
	nanos := int64(1756382400000000000)
	if _, err := epochMillis("epoch_s", nanos); err == nil {
		t.Fatalf("epoch_s accepted %d, which cannot be a second count", nanos)
	}
	// The wrap is real, not theoretical: this is what would have been
	// stored, and it is not the timestamp in the record.
	if wrapped := nanos * 1000; wrapped == nanos*1000 {
		t.Logf("unchecked, the record would land at %d (MaxTSMs is %d)", wrapped, model.MaxTSMs)
	}
}

func TestEpochUnitsConvert(t *testing.T) {
	cases := []struct {
		layout string
		in     int64
		want   int64
	}{
		{"epoch_s", 1756382400, 1756382400000},
		{"epoch_ms", 1756382400123, 1756382400123},
		{"epoch_us", 1756382400123456, 1756382400123},
		{"epoch_ns", 1756382400123456789, 1756382400123},
		// Pre-epoch values floor rather than truncate towards zero, so a
		// timestamp lands in the millisecond it belongs to.
		{"epoch_us", -1500, -2},
		{"epoch_ns", -1_500_000, -2},
	}
	for _, c := range cases {
		got, err := epochMillis(c.layout, c.in)
		if err != nil {
			t.Fatalf("%s %d: %v", c.layout, c.in, err)
		}
		if got != c.want {
			t.Fatalf("%s %d: got %d, want %d", c.layout, c.in, got, c.want)
		}
	}
}

const heldSpec = `
version: 1
profiles:
  - name: held
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
    framing:
      multiline:
        - start_contains: 'BEGIN'
          continue_regex: '^CONT'
          join: [{regex: '^CONT (.*)$', capture: 1}]
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`

// HeldFrom names the oldest record the stream is still holding, so a
// driver that checkpoints byte offsets can stop short of it. Before it
// existed such a record was indistinguishable from one that had been
// dealt with, and its bytes were acknowledged while its sample lived only
// in this struct.
func TestHeldFromNamesTheOldestBufferedRecord(t *testing.T) {
	spec, err := Parse([]byte(heldSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := spec.NewStream(spec.Profile("held"), StreamOptions{RefTime: time.Now()})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	// A plain record is dealt with on the spot.
	st.Mark(10)
	if out, err := st.Process("1756382400000 n=1"); err != nil || len(out) != 1 {
		t.Fatalf("plain record: %v %v", out, err)
	}
	if _, held := st.HeldFrom(); held {
		t.Fatal("a record that produced its sample is still reported as held")
	}

	// A record that only opens a block is held, at its own mark.
	st.Mark(20)
	if out, err := st.Process("1756382401000 BEGIN n=2"); err != nil || len(out) != 0 {
		t.Fatalf("block opener: %v %v", out, err)
	}
	at, held := st.HeldFrom()
	if !held || at != 20 {
		t.Fatalf("HeldFrom = (%d, %v), want (20, true)", at, held)
	}

	// A later record that produces its own sample does not release the
	// block: the block's data is still only in memory.
	st.Mark(30)
	if out, err := st.Process("1756382402000 n=3"); err != nil || len(out) != 1 {
		t.Fatalf("record after the block: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 20 {
		t.Fatalf("HeldFrom = (%d, %v) after an unrelated record, want (20, true)", at, held)
	}

	// A second opener closes the first block and starts its own, so the
	// hold-back moves forward rather than pinning at the first mark
	// forever.
	st.Mark(40)
	if out, err := st.Process("1756382403000 BEGIN n=4"); err != nil || len(out) != 1 {
		t.Fatalf("second opener: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 40 {
		t.Fatalf("HeldFrom = (%d, %v) after the block closed, want (40, true)", at, held)
	}

	// Flushing releases everything.
	st.Flush()
	if _, held := st.HeldFrom(); held {
		t.Fatal("the stream still reports held state after Flush")
	}
}

const aggHeldSpec = `
version: 1
profiles:
  - name: agg
    select: {}
    labels: [code]
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
    patterns:
      - set: hits
        search: 'code='
        extract: ['code=(?P<code>\d+)']
        aggregate: {every: 1m, on: [code], field: hits, mode: increment}
`

// An aggregation window holds the records it has absorbed, and releases
// them when it closes.
func TestHeldFromCoversAnOpenAggregationWindow(t *testing.T) {
	spec, err := Parse([]byte(aggHeldSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := spec.NewStream(spec.Profile("agg"), StreamOptions{RefTime: time.Now()})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()

	st.Mark(100)
	if out, err := st.Process(itoa(base) + " code=200"); err != nil || len(out) != 0 {
		t.Fatalf("first record: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 100 {
		t.Fatalf("HeldFrom = (%d, %v), want (100, true)", at, held)
	}
	// A second record folds into the same window; the hold-back stays at
	// the record that opened it.
	st.Mark(200)
	if out, err := st.Process(itoa(base+1000) + " code=200"); err != nil || len(out) != 0 {
		t.Fatalf("second record: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 100 {
		t.Fatalf("HeldFrom = (%d, %v) inside the window, want (100, true)", at, held)
	}
	// A record past the window's end closes it and opens the next one.
	st.Mark(300)
	out, err := st.Process(itoa(base+120_000) + " code=200")
	if err != nil || len(out) != 1 {
		t.Fatalf("window close: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 300 {
		t.Fatalf("HeldFrom = (%d, %v) after the window closed, want (300, true)", at, held)
	}
}
