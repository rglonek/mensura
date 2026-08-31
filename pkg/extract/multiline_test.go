package extract

import (
	"strings"
	"testing"
)

// An empty start_contains matches every line, so the multiline block
// silently never joins anything and holds each record back until the next
// one arrives. That is a spec bug worth naming at compile time.
func TestMultilineRequiresStartContains(t *testing.T) {
	const spec = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    framing:
      multiline:
        - continue_regex: '^\s+at '
          join: [{regex: '(.*)', capture: 1}]
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`
	_, err := Parse([]byte(spec))
	if err == nil || !strings.Contains(err.Error(), "start_contains") {
		t.Fatalf("expected a start_contains error, got %v", err)
	}
}

// A bucket name becomes a field name on the row, so a spec that cannot
// produce a valid one should fail where the spec is compiled.
func TestBucketNamesValidatedAtCompile(t *testing.T) {
	const spec = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    bucket_sets:
      - name: lat
        buckets: ["ok", "not a field"]
    patterns:
      - set: lines
        search: 'h='
        bucket_set: lat
        extract: ['h=(?P<buckets>.*)']
`
	_, err := Parse([]byte(spec))
	if err == nil || !strings.Contains(err.Error(), "not a field") {
		t.Fatalf("expected an invalid bucket name error, got %v", err)
	}
}
