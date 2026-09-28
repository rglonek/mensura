package extract

import "testing"

// TestCompileIsIdempotent pins Compile against being called twice.
//
// It is an exported method, and every compiled artefact it builds is
// replaced rather than extended -- p.labelSet, p.buckets, pat.labelSet,
// bs.edges and the per-regex fields assigned in place -- except pat.extract,
// which was appended to. A second Compile therefore left every `extract:`
// regex in the list twice: patternExtractedNames reported each capture
// twice, Declarations() and Lint() walked them twice, and process() tried
// each one again on every record none of them matched.
func TestCompileIsIdempotent(t *testing.T) {
	const doc = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    labels: [op]
    fields:
      n: {kind: counter}
    patterns:
      - set: app
        search: 'n='
        extract: ['op=(?P<op>\w+) n=(?P<n>\d+)', 'n=(?P<n>\d+)']
`
	s, err := Parse([]byte(doc))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	p := s.Profile("p")
	if p == nil {
		t.Fatal("profile p is missing")
	}
	first := len(p.Patterns[0].extract)
	if first != 2 {
		t.Fatalf("first compile built %d extract regexes, want 2", first)
	}
	firstNames := len(patternExtractedNames(p.Patterns[0]))
	firstLints := len(s.Lint())

	if err := s.Compile(); err != nil {
		t.Fatalf("second compile: %v", err)
	}
	if got := len(p.Patterns[0].extract); got != first {
		t.Fatalf("second compile left %d extract regexes, want %d", got, first)
	}
	if got := len(patternExtractedNames(p.Patterns[0])); got != firstNames {
		t.Fatalf("second compile left %d extracted names, want %d", got, firstNames)
	}
	if got := len(s.Lint()); got != firstLints {
		t.Fatalf("second compile changed the lint count from %d to %d", firstLints, got)
	}
}
