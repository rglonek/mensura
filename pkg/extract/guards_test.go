package extract

import (
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
)

// specWith wraps a profile body in the smallest document that compiles.
func specWith(body string) []byte {
	return []byte(`
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^[0-9]+'
` + body)
}

const joinPatterns = `
    patterns:
      - set: s
        search: "START"
        extract:
          - 'START (?P<v>[0-9]+)'
`

// join.capture is a slice index into the submatch list, and Process tests
// `len(g) > j.Capture` before reading g[j.Capture] -- true for every
// negative value. `capture: -1` therefore compiled cleanly and then
// panicked with an index out of range on the first continuation line that
// matched, on whichever goroutine happened to be extracting: a batch
// worker, the follow poll loop, or a receive connection. Any of them
// takes the whole ingester down.
func TestNegativeJoinCaptureIsRefused(t *testing.T) {
	_, err := Parse(specWith(`    framing:
      multiline:
        - start_contains: "START"
          continue_regex: "CONT"
          join:
            - regex: "CONT (.*)"
              capture: -1
` + joinPatterns))
	if err == nil {
		t.Fatal("compiled a negative join capture")
	}
	if !strings.Contains(err.Error(), "capture -1") {
		t.Fatalf("%v: want the capture to be named", err)
	}
}

// The other end fails open rather than loudly: the same test is false for
// every line, so the rule joins nothing while still opening a buffer on
// every start marker -- which is the failure an absent continue_regex is
// already refused for.
func TestOutOfRangeJoinCaptureIsRefused(t *testing.T) {
	_, err := Parse(specWith(`    framing:
      multiline:
        - start_contains: "START"
          continue_regex: "CONT"
          join:
            - regex: "CONT (.*)"
              capture: 3
` + joinPatterns))
	if err == nil {
		t.Fatal("compiled a join capture past the last group")
	}
	if !strings.Contains(err.Error(), "outside 0..1") {
		t.Fatalf("%v: want the valid range to be named", err)
	}
}

func TestValidJoinCaptureStillCompiles(t *testing.T) {
	for _, c := range []string{"0", "1"} {
		if _, err := Parse(specWith(`    framing:
      multiline:
        - start_contains: "START"
          continue_regex: "CONT"
          join:
            - regex: "CONT (.*)"
              capture: ` + c + `
` + joinPatterns)); err != nil {
			t.Fatalf("capture %s: %v", c, err)
		}
	}
}

// filepath.Match reports a malformed pattern as an error alongside "did
// not match", and matches() discards it -- so a spec whose glob does not
// compile matched no file at all, passed `check`, and surfaced at import
// time as "no profile matched", which points at the file rather than at
// the pattern.
func TestMalformedPathGlobIsRefused(t *testing.T) {
	_, err := Parse(specWith(`    select:
      path_glob: ['app[.log']
` + joinPatterns))
	if err == nil || !strings.Contains(err.Error(), "path_glob") {
		t.Fatalf("got %v, want a path_glob error", err)
	}
	if _, err := Parse(specWith(`    select:
      path_glob: ['*app*.log', '*app*.log.*']
` + joinPatterns)); err != nil {
		t.Fatalf("refused a well-formed glob: %v", err)
	}
}

func bucketSpec(edges string) []byte {
	return specWith(`    bucket_sets:
      - name: b
        parse: csv
        buckets: ['00','01','02']
        edges: '` + edges + `'
    patterns:
      - set: s
        search: "X"
        extract:
          - 'X (?P<buckets>.*)'
        bucket_set: b
`)
}

// fmt.Sscanf with %g stops at the first byte it cannot use and reports no
// error for the rest, so a typo became a different histogram axis in
// silence -- the same failure the config file's duration parser was moved
// off Sscanf to avoid. A step that does not advance, or explicit edges
// that do not ascend, are not an axis at all: heatmapSeriesName names a
// series after its edge, so the panel gets several series with one name.
func TestMalformedBucketEdgesAreRefused(t *testing.T) {
	for _, edges := range []string{
		"linear:5x", "linear:0", "linear:-1", "linear:",
		"explicit:[3zzz, 1, 2]", "explicit:[0, 0, 1]", "explicit:[0, 2, 1]",
	} {
		if _, err := Parse(bucketSpec(edges)); err == nil {
			t.Errorf("edges %q: compiled", edges)
		}
	}
	for _, edges := range []string{"pow2", "linear:5", "explicit:[0, 1, 4]", "explicit:[-2, 0, 2.5]"} {
		if _, err := Parse(bucketSpec(edges)); err != nil {
			t.Errorf("edges %q: %v", edges, err)
		}
	}
}

func TestLinearEdgesAreExact(t *testing.T) {
	s, err := Parse(bucketSpec("linear:2.5"))
	if err != nil {
		t.Fatal(err)
	}
	got := s.Profiles[0].BucketSets[0].EdgeValues()
	want := []float64{0, 2.5, 5}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("edges %v, want %v", got, want)
		}
	}
}

// The aggregation key joined its components with a separator that a label
// value may itself contain, so {a: "x", b: "y"} and {a: "x\x00b=y"} built
// the same key and two windows measuring different things folded into one
// accumulator. Every component is length-prefixed now, exactly as
// model.PrimaryKey and the store's seriesKey are.
func TestAggregationKeySeparatesEmbeddedDelimiters(t *testing.T) {
	spec, err := Parse(specWith(`    labels: [a, b]
    patterns:
      - set: s
        search: "EV"
        aggregate: {every: 1h, on: [a, b], field: hits, mode: increment}
        extract:
          - 'EV a=(?P<a>\S+) b=(?P<b>\S+)'
`))
	if err != nil {
		t.Fatal(err)
	}
	st, err := spec.NewStream(spec.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Two tuples that a separator-joined key cannot tell apart.
	if _, err := st.Process("1700000000 EV a=x\x00b=y b=z"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := st.Process("1700000001 EV a=x b=y\x00b=z"); err != nil {
		t.Fatalf("second: %v", err)
	}
	if n := len(st.aggs); n != 2 {
		t.Fatalf("%d open window(s), want 2: the two tuples share an accumulator", n)
	}
	out, _ := st.Flush()
	if len(out) != 2 {
		t.Fatalf("%d row(s), want 2", len(out))
	}
	for _, r := range out {
		if v, ok := r.Fields["hits"]; !ok || v != model.Int(1) {
			t.Fatalf("row %v counted more than its own record", r.Labels)
		}
	}
}

// The timestamp defaults are resolved per stream -- once per file on a
// batch import, once per connection on a receiver, and on every poll of a
// followed path -- so a typo in either compiled cleanly, passed `check`,
// and then failed forever from a spec the tool had called good.
func TestTimestampDefaultsAreValidatedAtCompileTime(t *testing.T) {
	body := `    labels: []
    patterns:
      - set: s
        search: "X"
        extract:
          - 'X (?P<v>[0-9]+)'
`
	withDefaults := func(defaults string) []byte {
		return []byte("version: 1\ndefaults:\n  timestamp:\n" + defaults + `
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_s
          regex: '^[0-9]+'
` + body)
	}
	for _, tc := range []struct{ name, defaults, want string }{
		{"timezone", "    timezone: Not/AZone\n", "timezone"},
		{"assume_year word", "    assume_year: last\n", "assume_year"},
		{"assume_year range", "    assume_year: \"-5\"\n", "1970..9999"},
	} {
		_, err := Parse(withDefaults(tc.defaults))
		if err == nil {
			t.Errorf("%s: compiled", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want it to mention %q", tc.name, err, tc.want)
		}
	}
	for _, ok := range []string{"    assume_year: now\n", "    assume_year: \"2026\"\n", "    timezone: UTC\n", "    assume_year: file-mtime\n"} {
		if _, err := Parse(withDefaults(ok)); err != nil {
			t.Errorf("refused a valid declaration %q: %v", ok, err)
		}
	}
}
