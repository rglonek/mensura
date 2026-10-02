package extract

import (
	"strings"
	"testing"
)

func lintCodes(t *testing.T, doc string) []Lint {
	t.Helper()
	s, err := Parse([]byte(doc))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return s.Lint()
}

func hasCode(lints []Lint, code string) *Lint {
	for i := range lints {
		if lints[i].Code == code {
			return &lints[i]
		}
	}
	return nil
}

// Two patterns write one set and disagree about whether a capture is a
// label. Labels and fields share one column namespace, so the store
// refuses whichever classification arrives second -- for every record,
// for the life of the process.
func TestLintColumnNamespaceAcrossPatterns(t *testing.T) {
	lints := lintCodes(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: app
        search: "A "
        labels: [status]
        extract: ['A status=(?P<status>\w+)']
      - set: app
        search: "B "
        extract: ['B status=(?P<status>\d+)']
`)
	l := hasCode(lints, "L008")
	if l == nil {
		t.Fatalf("no L008 for a set written with one name as a label and as a field: %v", lints)
	}
	if !strings.Contains(l.Msg, `"status"`) || !strings.Contains(l.Msg, `"app"`) {
		t.Fatalf("L008 does not name the column and the set: %s", l.Msg)
	}
	if !Fatal(lints) {
		t.Fatal("L008 must fail `check`: one of the two patterns can never store a row")
	}
}

// Across profiles too: the two halves of the collision need not sit in
// the same profile.
func TestLintColumnNamespaceAcrossProfiles(t *testing.T) {
	lints := lintCodes(t, `
version: 1
profiles:
  - name: a
    select: {path_glob: ['*.a']}
    labels: [zone]
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: shared
        search: "A "
        extract: ['A zone=(?P<zone>\w+) v=(?P<v>\d+)']
  - name: b
    select: {path_glob: ['*.b']}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: shared
        search: "B "
        extract: ['B zone=(?P<zone>\d+)']
`)
	if hasCode(lints, "L008") == nil {
		t.Fatalf("no L008 across profiles writing one set: %v", lints)
	}
}

// Two patterns writing *different* sets may classify a name however they
// like: the columns never meet.
func TestLintColumnNamespaceDifferentSetsIsFine(t *testing.T) {
	lints := lintCodes(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: one
        search: "A "
        labels: [status]
        extract: ['A status=(?P<status>\w+)']
      - set: two
        search: "B "
        extract: ['B status=(?P<status>\d+)']
`)
	if l := hasCode(lints, "L008"); l != nil {
		t.Fatalf("L008 reported for two different destination sets: %s", l.Msg)
	}
}

// Declaring metadata in `fields:` for a name the profile also lists under
// `labels:` is harmless: nothing writes it as a field, and the store
// treats a declaration as metadata rather than as evidence.
func TestLintColumnNamespaceIgnoresFieldDeclarations(t *testing.T) {
	lints := lintCodes(t, `
version: 1
profiles:
  - name: p
    labels: [status]
    fields:
      status: {unit: state, description: the request status}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: app
        search: "A "
        extract: ['A status=(?P<status>\w+) v=(?P<v>\d+)']
`)
	if l := hasCode(lints, "L008"); l != nil {
		t.Fatalf("L008 reported for a fields: declaration on a label: %s", l.Msg)
	}
}
