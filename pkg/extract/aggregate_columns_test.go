package extract

import (
	"strings"
	"testing"
)

// An aggregation window used to be built with a copy of the opening
// record's whole field map, so every row it emitted carried that one
// record's other measurements as if they belonged to the window.
func TestAggregateWindowCarriesOnlyWhatItMeasured(t *testing.T) {
	const doc = `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^\d+'
    labels: [status]
    patterns:
      - set: s
        search: 'REQ'
        extract:
          - 'REQ status=(?P<status>\w+) bytes=(?P<bytes>\d+)'
        aggregate: {every: 10s, on: [status], field: hits, mode: increment}
`
	spec, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	st, err := spec.NewStream(spec.Profile("p"), StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"1700000000 REQ status=ok bytes=100",
		"1700000001 REQ status=ok bytes=999999",
		"1700000002 REQ status=ok bytes=7",
	} {
		if _, err := st.Process(line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	out, errs := st.Flush()
	if len(errs) > 0 {
		t.Fatalf("flush: %v", errs)
	}
	if len(out) != 1 {
		t.Fatalf("want one window, got %d", len(out))
	}
	if v, ok := out[0].Fields["bytes"]; ok {
		t.Errorf("window carries bytes=%s, which only the record that opened it reported", v.String())
	}
	hits, ok := out[0].Fields["hits"]
	if !ok {
		t.Fatal("window carries no hits column")
	}
	if got, _ := hits.AsInt(); got != 3 {
		t.Errorf("hits = %d, want 3", got)
	}
	if len(out[0].Fields) != 1 {
		t.Errorf("window carries %d columns, want only the accumulated one: %v", len(out[0].Fields), out[0].Fields)
	}
	if out[0].Labels["status"] != "ok" {
		t.Errorf("window lost its key label: %v", out[0].Labels)
	}
}

// A window with no reading under `max` still writes no row, which is the
// behaviour the single-column emit has to preserve.
func TestAggregateWindowWithNoReadingWritesNoRow(t *testing.T) {
	const doc = `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^\d+'
    labels: [op]
    patterns:
      - set: s
        search: 'OP'
        extract:
          - 'OP op=(?P<op>\w+)(?: lat=(?P<lat>\d+))?'
        aggregate: {every: 10s, on: [op], field: lat, mode: max}
`
	spec, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	st, err := spec.NewStream(spec.Profile("p"), StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Process("1700000000 OP op=read"); err != nil {
		t.Fatal(err)
	}
	out, _ := st.Flush()
	if len(out) != 0 {
		t.Fatalf("a window that measured nothing wrote %d row(s): %v", len(out), out)
	}
	if st.Stats.WindowsEmpty != 1 {
		t.Errorf("WindowsEmpty = %d, want 1", st.Stats.WindowsEmpty)
	}
}

func TestLintNamesTheCapturesAWindowCannotCarry(t *testing.T) {
	const doc = `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^\d+'
    labels: [status, pool]
    fields:
      bytes: {kind: gauge}
      hits: {kind: delta}
    patterns:
      - set: s
        search: 'REQ'
        extract:
          - 'REQ status=(?P<status>\w+) pool=(?P<pool>\w+) bytes=(?P<bytes>\d+)'
        aggregate: {every: 10s, on: [status], field: hits, mode: increment}
`
	spec, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	var dropped, carried bool
	for _, l := range spec.Lint() {
		if l.Code != "L007" {
			continue
		}
		if l.Fatal() {
			t.Errorf("L007 must stay advisory: %s", l)
		}
		switch {
		case strings.Contains(l.Msg, `"bytes"`) && strings.Contains(l.Msg, "discarded"):
			dropped = true
		case strings.Contains(l.Msg, `"pool"`):
			carried = true
		}
	}
	if !dropped {
		t.Error("no L007 naming the discarded field")
	}
	if !carried {
		t.Error("no L007 naming the label outside `on`")
	}
}

// A pattern that captures only its key and aggregates into a column of
// its own has nothing to report.
func TestLintQuietForAWellFormedAggregate(t *testing.T) {
	const doc = `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^\d+'
    labels: [error_class]
    fields:
      errors: {kind: delta}
    patterns:
      - set: s
        search: 'ERROR '
        extract:
          - 'ERROR (?P<error_class>[A-Za-z.]+): '
        aggregate: {every: 10s, on: [error_class], field: errors, mode: increment}
`
	spec, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range spec.Lint() {
		if l.Code == "L007" {
			t.Errorf("unexpected L007: %s", l)
		}
	}
}
