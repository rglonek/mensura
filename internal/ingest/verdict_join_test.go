package ingest

import (
	"context"
	"testing"

	"github.com/rglonek/mensura/pkg/extract"
)

// A continuation line whose timestamp moved backwards refuses that line
// *and* flushes the record it was meant to join, so extract.Stream has
// two verdicts to report from one call. Collapsing them into a single
// ExtractError lost whichever one the flushed record deserved -- a
// profile whose joined records match no pattern under-counted
// unmatched_lines by one every time an interleaved writer produced such a
// line, which is the counter an operator reads to find out that the spec
// is wrong for the file.
func TestBothVerdictsOfOneRecordAreCounted(t *testing.T) {
	spec, err := extract.Parse([]byte(`
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      strip: true
    framing:
      multiline:
        - start_contains: "BEGIN"
          continue_regex: "^ more"
          join:
            - regex: "^ more (.*)$"
              capture: 1
    patterns:
      - set: app
        search: MATCHES
        extract: ['MATCHES v=(?P<v>\d+)']
`))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := spec.NewStream(spec.Profile("p"), extract.StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	sink := NewSink(nil, DefaultSinkConfig(), testLogger{t})
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// A record that opens a multiline buffer and matches no pattern.
	if _, perr := st.Process("1756382400000 BEGIN nothing matches this"); perr != nil {
		ing.recordOutcome(perr)
	}
	// A continuation line carrying an earlier timestamp: it is refused,
	// and it flushes the buffered record, which is judged for the first
	// and only time here.
	_, perr := st.Process("1756382300000 more tail")
	if perr == nil {
		t.Fatal("a backwards continuation timestamp was accepted")
	}
	ing.recordOutcome(perr)

	snap := ing.Progress().Snapshot()
	if snap.UnmatchedLines != 1 {
		t.Errorf("unmatched_lines = %d, want 1: the flushed record's verdict was dropped", snap.UnmatchedLines)
	}
	if snap.ExtractErrors != 1 {
		t.Errorf("extract_errors = %d, want 1: the backwards line's own verdict was dropped", snap.ExtractErrors)
	}
}
