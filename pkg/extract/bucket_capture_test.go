package extract

import (
	"strings"
	"testing"
)

// capturingBucketSpec builds a spec whose pattern captures `name` beside
// the histogram payload, against a bucket set declared with `bucketSet`.
func capturingBucketSpec(name, bucketSet string) string {
	return `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_s, regex: '^\d+'}]
      strip: true
    patterns:
      - set: s
        search: 'h '
        bucket_set: bs
        extract: ['h \((?P<total>\d+)\) (?P<` + name + `>\d+) (?P<buckets>.*)']
    bucket_sets:
` + bucketSet
}

// expand() writes the histogram into the same field map the captures were
// collected into, and it runs afterwards, so any column the bucket set
// produces silently overwrites a capture of the same name: the count the
// source reported is replaced by a number derived from somewhere else, on
// a row that is well formed and a heatmap that draws.
//
// Only the `tail` collision used to be refused, and it is the least likely
// of the three -- a bucket set naming its buckets `fast`/`slow`, or
// deriving `<bucket>plus` columns from names like those, collides with an
// ordinary capture far more readily than the literal word "tail".
func TestBucketColumnCollidingWithACaptureIsRefused(t *testing.T) {
	const declared = `      - name: bs
        parse: key_value
        buckets: ['fast','slow']
        edges: 'explicit:[0,1]'
        total_field: total
        cumulative: true
        tail: true
`
	for _, name := range []string{"fast", "slow", "fastplus", "slowplus", "tail"} {
		_, err := Parse([]byte(capturingBucketSpec(name, declared)))
		if err == nil {
			t.Fatalf("a pattern capturing %q beside bucket set bs compiled; the histogram overwrites it", name)
		}
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("the error for %q must name the clashing column, got %v", name, err)
		}
	}
}

// `total_field` is the one name a bucket set reads rather than writes, so
// a pattern must still be able to capture it -- that is the only way it is
// ever populated.
func TestCapturingTheTotalFieldIsStillAllowed(t *testing.T) {
	const declared = `      - name: bs
        parse: key_value
        buckets: ['fast','slow']
        edges: 'explicit:[0,1]'
        total_field: total
        cumulative: true
        tail: true
`
	// The helper already captures `total`; a second, unrelated capture
	// must compile alongside it.
	if _, err := Parse([]byte(capturingBucketSpec("latency", declared))); err != nil {
		t.Fatalf("a capture that collides with nothing must compile: %v", err)
	}
}
