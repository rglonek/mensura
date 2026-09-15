package extract

import (
	"strings"
	"testing"
)

// A `default_values` entry for a name the profile declares as a label has
// to fill the *label* map. It used to fill the fields unconditionally,
// which produced a sample carrying one name twice -- once as a label,
// once as a field -- and that is the one shape store.rowFor refuses by
// name, so every record the pattern produced was rejected for the life of
// the process with nothing pointing back at the spec.
func TestDefaultValueForADeclaredLabelIsALabel(t *testing.T) {
	spec, err := Parse([]byte(`
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^\d+'
    labels: [status]
    patterns:
      - set: app
        search: 'req'
        extract:
          - 'req(?: status=(?P<status>[a-z]+))? ms=(?P<ms>\d+)'
        default_values:
          status: unknown
`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	st, err := spec.NewStream(spec.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// The capture participated: the label carries it and the default must
	// not also write a column of the same name.
	got, err := st.Process("1700000000 req status=ok ms=5")
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("%d result(s), want 1", len(got))
	}
	if got[0].Labels["status"] != "ok" {
		t.Fatalf("labels %v, want status=ok", got[0].Labels)
	}
	if _, dup := got[0].Fields["status"]; dup {
		t.Fatalf("%q is both a label and a field (%v / %v); the store refuses every such sample", "status", got[0].Labels, got[0].Fields)
	}

	// The capture did not participate: the default fills the label, not a
	// column, so the row still falls in its own BY group.
	got, err = st.Process("1700000001 req ms=7")
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("%d result(s), want 1", len(got))
	}
	if got[0].Labels["status"] != "unknown" {
		t.Fatalf("labels %v, want the default status=unknown", got[0].Labels)
	}
	if _, dup := got[0].Fields["status"]; dup {
		t.Fatalf("the default landed as a field as well: %v", got[0].Fields)
	}
}

// A default for a name that is not a declared label is still a field, and
// still coerced.
func TestDefaultValueForAFieldIsStillAField(t *testing.T) {
	spec, err := Parse([]byte(`
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^\d+'
    patterns:
      - set: app
        search: 'req'
        extract:
          - 'req(?: bytes=(?P<bytes_sent>\d+))? ms=(?P<ms>\d+)'
        default_values:
          bytes_sent: 0
`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	st, _ := spec.NewStream(spec.Profiles[0], StreamOptions{})
	got, err := st.Process("1700000000 req ms=7")
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	v, ok := got[0].Fields["bytes_sent"]
	if !ok {
		t.Fatalf("the default did not fill the field: %v", got[0].Fields)
	}
	if n, ok := v.AsInt(); !ok || n != 0 {
		t.Fatalf("bytes_sent is %v, want the coerced integer 0", v)
	}
}

// The accumulator synthesises `aggregate.field` as a column, so declaring
// that name as a label is a classification nothing downstream can honour.
func TestAggregateFieldDeclaredAsALabelIsRefused(t *testing.T) {
	_, err := Parse([]byte(`
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^\d+'
    labels: [op, hits]
    patterns:
      - set: app
        search: 'req'
        extract:
          - 'req op=(?P<op>\w+)'
        aggregate:
          every: 1s
          on: [op]
          field: hits
          mode: increment
`))
	if err == nil {
		t.Fatal("a spec whose aggregate field is also a declared label must not compile")
	}
	if !strings.Contains(err.Error(), "hits") {
		t.Fatalf("the error must name the field: %v", err)
	}
}

// L005: a capture the profile does not classify as a label, whose name is
// one the acquisition layer attaches as a stream label anyway. Every
// sample such a pattern produces carries the name as a label and as a
// field, and the store refuses all of them.
func TestLintReportsACaptureThatCollidesWithAStreamLabel(t *testing.T) {
	spec, err := Parse([]byte(`
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^\d+'
    fields:
      host: {kind: string}
    patterns:
      - set: app
        search: 'req'
        extract:
          - 'req host=(?P<host>\S+) ms=(?P<ms>\d+)'
`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	var found *Lint
	for i, l := range spec.Lint() {
		if l.Code == "L005" {
			found = &spec.Lint()[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("no L005 for a field capture named after a stream label; findings: %v", spec.Lint())
	}
	if !strings.Contains(found.Msg, `"host"`) {
		t.Fatalf("L005 must name the capture: %s", found.Msg)
	}
	if found.Fatal() {
		t.Fatal("L005 is advisory: an identity rule scoped by match_path does not apply to every stream")
	}
	// Declaring it as a label is the fix, and it silences the finding.
	fixed, err := Parse([]byte(`
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^\d+'
    labels: [host]
    patterns:
      - set: app
        search: 'req'
        extract:
          - 'req host=(?P<host>\S+) ms=(?P<ms>\d+)'
`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	for _, l := range fixed.Lint() {
		if l.Code == "L005" {
			t.Fatalf("a declared label still reports L005: %s", l)
		}
	}
}
