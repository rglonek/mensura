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

// continue_regex is the only test Process applies to a candidate
// continuation line, so a multiline rule without one joins nothing at all
// -- while still opening a buffer on every start marker. The record that
// opened it is then held until the next start marker or the idle timeout,
// and on a followed file HeldFrom pins the checkpoint to its offset for
// just as long. A spec that reads as "assemble these lines" silently
// becoming "delay every one of them" is a compile-time fault.
func TestMultilineRequiresContinueRegex(t *testing.T) {
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
        - start_contains: 'BEGIN'
          join: [{regex: '(.*)', capture: 1}]
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`
	_, err := Parse([]byte(spec))
	if err == nil || !strings.Contains(err.Error(), "continue_regex") {
		t.Fatalf("expected a continue_regex error, got %v", err)
	}
}
