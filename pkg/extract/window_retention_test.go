package extract

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

const retentionSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^[0-9]+'
    labels: [op]
    patterns:
      - set: s
        search: "HIT"
        extract:
          - 'HIT (?P<op>\S+)'
        aggregate:
          every: 1s
          on: [op]
          field: hits
          mode: increment
`

// A heap that only reslices on Pop leaves the popped entry in the backing
// array, and the collector traces the array rather than the length -- so
// every aggregation window a stream ever emitted stayed reachable through
// its *aggregator, which holds copies of the opening record's label and
// field maps and the whole record line. The queue's high-water mark is
// maxOpenWindows and one stream can live for the life of the process, so
// the retention was the peak footprint rather than the live one.
func TestClosedWindowsAreNotRetainedByTheExpiryQueue(t *testing.T) {
	sp, err := Parse([]byte(retentionSpec))
	if err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	st, err := sp.NewStream(sp.Profile("p"), StreamOptions{})
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	const n = 200
	base := time.Now().Unix()
	for i := 0; i < n; i++ {
		if _, err := st.Process(fmt.Sprintf("%d HIT op%d", base, i)); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	// One record far enough ahead closes every window that is open.
	if _, err := st.Process(fmt.Sprintf("%d HIT last", base+10)); err != nil {
		t.Fatalf("closing record: %v", err)
	}
	if len(st.aggQ) != 1 {
		t.Fatalf("%d window(s) still open, want 1", len(st.aggQ))
	}
	q := st.aggQ[:cap(st.aggQ)]
	for i := len(st.aggQ); i < len(q); i++ {
		if q[i].a != nil || q[i].key != "" {
			t.Fatalf("slot %d past the queue's length still holds window %q; a closed window stays reachable for the life of the stream", i, q[i].key)
		}
	}
}

// pruneHolds compacts the hold list in place, which shortens the slice and
// leaves the dropped entries -- each naming an aggregation key and the
// window it belonged to -- in the backing array. Pruning that frees
// nothing is not pruning.
func TestPrunedHoldsAreNotRetained(t *testing.T) {
	sp, err := Parse([]byte(retentionSpec))
	if err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	st, err := sp.NewStream(sp.Profile("p"), StreamOptions{})
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	base := time.Now().Unix()
	for i := 0; i < 200; i++ {
		st.Mark(int64(i) * 64)
		if _, err := st.Process(fmt.Sprintf("%d HIT op%d", base+int64(i), i)); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	st.Mark(200 * 64)
	if _, err := st.Process(fmt.Sprintf("%d HIT last", base+10_000)); err != nil {
		t.Fatalf("closing record: %v", err)
	}
	if _, _ = st.HeldFrom(); len(st.holds) == 0 {
		t.Skip("nothing was pruned in this arrangement")
	}
	h := st.holds[:cap(st.holds)]
	for i := len(st.holds); i < len(h); i++ {
		if h[i].a != nil || h[i].key != "" {
			t.Fatalf("slot %d past the hold list's length still names window %q", i, h[i].key)
		}
	}
}

// The store converts a declared retention or shard width with
// `time.Duration(ms) * time.Millisecond`, which overflows past ~292 years
// and then drops the declaration in silence. It is decidable here, which
// is where every other spec duration is decided.
func TestSetDurationBeyondADurationIsRefused(t *testing.T) {
	for _, key := range []string{"retention", "shard"} {
		doc := fmt.Sprintf(`
version: 1
sets:
  app:
    %s: 100000000d
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^[0-9]+'
    patterns:
      - set: app
        search: "x"
        extract:
          - 'v=(?P<v>[0-9]+)'
`, key)
		_, err := Parse([]byte(doc))
		if err == nil {
			t.Fatalf("sets.app.%s: a duration the store cannot hold compiled", key)
		}
		if !strings.Contains(err.Error(), "beyond") {
			t.Fatalf("sets.app.%s: unexpected error %v", key, err)
		}
	}
}
