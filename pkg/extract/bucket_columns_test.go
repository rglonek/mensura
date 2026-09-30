package extract

import (
	"strings"
	"testing"
)

// Two bucket sets that write the same column into the same destination set
// land on one row, and nothing said so.
//
// compile already refuses a collision *within* one bucket set. Across two
// of them aimed at one `set:` the consequences were all silent: both
// patterns write one column with two different meanings, so the stored
// series interleaves two measurements -- and Sink.DeclareFields sends one
// FieldMeta per (set, column) and marks it sent, so the second bucket
// set's shared columns are never declared at all. applyFieldMeta then
// grows that bucket set's column list to the indices that did arrive and
// leaves the rest empty, runHeatmap skips an empty column, and the panel
// draws with its shared buckets simply missing.
//
// The conventional HDR bucket names are "00".."23", so a read histogram
// and a write histogram aimed at one set collide on every one of them.
func TestTwoBucketSetsMayNotShareAColumnOnOneSet(t *testing.T) {
	clashing := `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    bucket_sets:
      - name: reads
        buckets: ["00", "01"]
      - name: writes
        buckets: ["00", "02"]
    patterns:
      - set: lat
        search: READ
        bucket_set: reads
        extract: ['READ (?P<buckets>.*)$']
      - set: lat
        search: WRITE
        bucket_set: writes
        extract: ['WRITE (?P<buckets>.*)$']
`
	_, err := Parse([]byte(clashing))
	if err == nil {
		t.Fatal("two bucket sets writing the column \"00\" into set \"lat\" compiled cleanly")
	}
	for _, want := range []string{"reads", "writes", `"00"`, "lat"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %s: %v", want, err)
		}
	}

	// A derived column collides just as a declared bucket does: a
	// cumulative "01plus" against a literal bucket named "01plus".
	derived := strings.Replace(clashing,
		`      - name: writes
        buckets: ["00", "02"]`,
		`      - name: writes
        cumulative: true
        buckets: ["01", "02"]`, 1)
	derived = strings.Replace(derived, `buckets: ["00", "01"]`, `buckets: ["00", "01plus"]`, 1)
	if _, err := Parse([]byte(derived)); err == nil {
		t.Error("a literal bucket colliding with another set's cumulative column compiled cleanly")
	}

	// The same two bucket sets writing *different* destination sets is
	// ordinary: they never share a row, so the names may repeat.
	apart := strings.Replace(clashing, `      - set: lat
        search: WRITE`, `      - set: lat_write
        search: WRITE`, 1)
	if _, err := Parse([]byte(apart)); err != nil {
		t.Fatalf("two bucket sets on different sets were refused: %v", err)
	}

	// And one bucket set reached through two branches of one pattern is
	// one bucket set, not a collision with itself.
	shared := `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    bucket_sets:
      - name: reads
        buckets: ["00", "01"]
    patterns:
      - set: lat
        search: OP
        bucket_set: reads
        extract: ['READ (?P<buckets>.*)$']
        route:
          - regex: 'WRITE (?P<buckets>.*)$'
            set: lat
`
	if _, err := Parse([]byte(shared)); err != nil {
		t.Fatalf("one bucket set reached through two branches was refused: %v", err)
	}
}

// The bucket-set name is what the store keys its per-set bucket-set map
// on, so it is held to the same rule here as it is there: a name the
// store refuses is refused on every declaration, and a refused
// declaration is a 400 -- which the write client classifies as fatal, so
// the first batch carrying it is dropped and reported as a hole.
func TestABucketSetNameIsValidatedAtCompileTime(t *testing.T) {
	tmpl := `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    bucket_sets:
      - name: %q
        buckets: ["00"]
    patterns:
      - set: lat
        search: OP
        bucket_set: %q
        extract: ['OP (?P<buckets>.*)$']
`
	for _, name := range []string{"has space", "at@sign", strings.Repeat("x", 200)} {
		src := strings.ReplaceAll(tmpl, "%q", `"`+name+`"`)
		if _, err := Parse([]byte(src)); err == nil {
			t.Errorf("bucket set name %q compiled cleanly", name)
		}
	}
	src := strings.ReplaceAll(tmpl, "%q", `"hdr24"`)
	if _, err := Parse([]byte(src)); err != nil {
		t.Fatalf("an ordinary bucket set name was refused: %v", err)
	}
}
