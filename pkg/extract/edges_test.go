package extract

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
)

// A bucket-set edge that is not a finite number is refused at compile
// time.
//
// strconv.ParseFloat reads "inf", "+Inf" and "NaN" without complaint, and
// the ascending-edge test cannot catch a NaN because every comparison
// against one is false -- so `edges: explicit:[1,inf]` compiled, passed
// `check`, and then travelled as wire.FieldMeta.BucketEdge. encoding/json
// refuses to marshal it, so the whole write request carrying the
// declaration is undeliverable: the client classifies that as fatal, the
// sink drops the batch, reports it to the delivery observers as a hole,
// and every followed file's checkpoint freezes behind it. `fields:
// limits:` is held to this rule already; this is the other number a spec
// can put on the wire.
func TestNonFiniteBucketEdgesAreRefused(t *testing.T) {
	cases := []struct {
		edges   string
		buckets []string
	}{
		{"explicit:[1,inf]", []string{"a", "b"}},
		{"explicit:[nan]", []string{"a"}},
		{"explicit:[+Inf]", []string{"a"}},
		{"linear:inf", []string{"a", "b"}},
		{"linear:nan", []string{"a", "b"}},
		// A finite step still overflows once it is multiplied by the
		// bucket position, which no per-literal check can see.
		{"linear:1e308", []string{"a", "b", "c"}},
	}
	for _, c := range cases {
		bs := &BucketSet{Name: "h", Buckets: c.buckets, Edges: c.edges}
		err := bs.compile()
		if err == nil {
			t.Errorf("edges %q compiled to %v; it must be refused", c.edges, bs.edges)
			continue
		}
		if !strings.Contains(err.Error(), "finite") {
			t.Errorf("edges %q: %v does not say why", c.edges, err)
		}
	}
}

// The layouts that are meant to work still do, and every edge they
// produce can be encoded.
func TestFiniteBucketEdgesStillCompile(t *testing.T) {
	cases := []struct {
		edges   string
		buckets []string
	}{
		{"", []string{"00", "01", "02", "03"}},
		{"pow2", []string{"00", "01", "02", "03"}},
		{"linear:5", []string{"a", "b", "c"}},
		{"explicit:[0,10,100]", []string{"a", "b", "c"}},
	}
	for _, c := range cases {
		bs := &BucketSet{Name: "h", Buckets: c.buckets, Edges: c.edges}
		if err := bs.compile(); err != nil {
			t.Fatalf("edges %q: %v", c.edges, err)
		}
		for i, e := range bs.edges {
			if !model.IsFinite(e) {
				t.Fatalf("edges %q: edge %d is %v", c.edges, i, e)
			}
			if _, err := json.Marshal(e); err != nil {
				t.Fatalf("edges %q: edge %d cannot be encoded: %v", c.edges, i, err)
			}
		}
	}
}

// And the same refusal from the spec surface, which is where an operator
// meets it.
func TestASpecWithANonFiniteEdgeDoesNotCompile(t *testing.T) {
	_, err := Parse([]byte(`
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    bucket_sets:
      - name: hdr
        buckets: [a, b]
        edges: 'explicit:[1,inf]'
    patterns:
      - set: hist
        search: 'histogram'
        bucket_set: hdr
        extract: ['histogram (?P<buckets>.*)']
`))
	if err == nil {
		t.Fatal("a spec declaring a non-finite bucket edge compiled")
	}
	if !strings.Contains(err.Error(), "finite") {
		t.Fatalf("%v does not say why", err)
	}
}
