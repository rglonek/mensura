package extract

import "testing"

// bucketSpec builds a one-pattern spec around a bucket-set declaration.
func bucketColumnsSpec(bucketSet string) string {
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
        extract: ['h \((?P<total>\d+)\) (?P<buckets>.*)']
    bucket_sets:
` + bucketSet
}

// Every column a bucket set writes lands on one row, so they all have to
// be distinct names.
//
// A repeated bucket is not a harmless duplicate declaration. expand() adds
// each occurrence of a bucket into the running sum it derives the tail
// from, so a bucket listed twice made the tail short by that bucket's own
// count -- often clamped to zero -- and every cumulative "<bucket>plus"
// column below it wrong with it. The row was well formed and the heatmap
// drew, so nothing anywhere said so.
func TestDuplicateBucketColumnIsRefused(t *testing.T) {
	_, err := Parse([]byte(bucketColumnsSpec(`      - name: bs
        parse: paren_pairs
        buckets: ['00','00','01']
        edges: pow2
        total_field: total
        cumulative: true
        tail: true
`)))
	if err == nil {
		t.Fatal("a bucket set listing the same bucket twice compiled")
	}
	t.Log(err)
}

// The mirror image: a bucket whose name is the derived column of another
// one. The declared bucket's measured count is overwritten by a number
// nothing counted.
func TestBucketCollidingWithACumulativeColumnIsRefused(t *testing.T) {
	_, err := Parse([]byte(bucketColumnsSpec(`      - name: bs
        parse: key_value
        buckets: ['00','00plus']
        edges: 'explicit:[0,1]'
        total_field: total
        cumulative: true
`)))
	if err == nil {
		t.Fatal("a bucket named after another bucket's cumulative column compiled")
	}
	t.Log(err)
}

// `tail: true` writes a column literally called "tail", which a bucket may
// not also be called. The same clash is already refused where a pattern
// *captures* "tail"; a bucket list is the other way in.
func TestBucketCollidingWithTheTailColumnIsRefused(t *testing.T) {
	_, err := Parse([]byte(bucketColumnsSpec(`      - name: bs
        parse: key_value
        buckets: ['00','tail']
        edges: 'explicit:[0,1]'
        total_field: total
        tail: true
`)))
	if err == nil {
		t.Fatal("a bucket called \"tail\" compiled alongside tail: true")
	}
	t.Log(err)
}

// total_field is read off the same row the buckets are written to, and
// expand() writes the buckets first -- so a total_field naming one of them
// reads a bucket count as the total and derives the tail from it.
func TestTotalFieldCollidingWithABucketIsRefused(t *testing.T) {
	_, err := Parse([]byte(bucketColumnsSpec(`      - name: bs
        parse: key_value
        buckets: ['00','01']
        edges: 'explicit:[0,1]'
        total_field: '01'
        tail: true
`)))
	if err == nil {
		t.Fatal("a total_field naming one of the buckets compiled")
	}
	t.Log(err)
}

// The ordinary shape still compiles and still expands correctly: buckets,
// their cumulative columns, the tail and the total are four distinct
// families of name.
func TestDistinctBucketColumnsStillCompile(t *testing.T) {
	s, err := Parse([]byte(bucketColumnsSpec(`      - name: bs
        parse: paren_pairs
        buckets: ['00','01','02']
        edges: pow2
        total_field: total
        cumulative: true
        tail: true
`)))
	if err != nil {
		t.Fatalf("a well-formed bucket set was refused: %v", err)
	}
	st, err := s.NewStream(s.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := st.Process("1700000000 h (100) (00: 10) (01: 20) (02: 30)")
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	f := res[0].Fields
	// 100 total, 60 in the declared buckets: the tail is the rest, and
	// each cumulative column is the count at or above its bucket.
	for name, want := range map[string]int64{
		"00": 10, "01": 20, "02": 30, "tail": 40,
		"02plus": 70, "01plus": 90, "00plus": 100,
	} {
		got, ok := f[name].AsInt()
		if !ok || got != want {
			t.Errorf("%s = %v, expected %d", name, f[name], want)
		}
	}
}
