package extract

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
)

func mustSpec(t *testing.T, body string) *Spec {
	t.Helper()
	s, err := Parse([]byte(body))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return s
}

func specError(t *testing.T, body string) string {
	t.Helper()
	if _, err := Parse([]byte(body)); err != nil {
		return err.Error()
	}
	t.Fatal("expected the spec to be refused")
	return ""
}

const aggSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    labels: [class]
    patterns:
      - set: errors
        search: "ERR"
        extract: ['ERR (?P<class>\w+)']
        aggregate: {every: 10s, on: [class], field: n, mode: %s}
`

// An unrecognised aggregation mode used to compile. The accumulator's
// update switch has no default, so the window kept whichever value the
// first record carried and ignored every later one: `mode: avg` drew a
// flat, entirely plausible series and reported nothing.
func TestUnknownAggregateModeIsRefused(t *testing.T) {
	for _, mode := range []string{"increment", "sum", "max", "last"} {
		mustSpec(t, strings.Replace(aggSpec, "%s", mode, 1))
	}
	msg := specError(t, strings.Replace(aggSpec, "%s", "avg", 1))
	if !strings.Contains(msg, "avg") || !strings.Contains(msg, "increment") {
		t.Errorf("refusal does not name the mode or the alternatives: %s", msg)
	}
}

const framingSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
      %s
    framing:
      record: %s
    patterns:
      - set: s
        search: "x"
        extract: ['x (?P<n>\d+)']
`

func framingBody(onErr, record string) string {
	s := strings.Replace(framingSpec, "%s", onErr, 1)
	return strings.Replace(s, "%s", record, 1)
}

// A declaration that does nothing is worse than one that is rejected --
// the principle this repository already applies to store_stream_label
// and listen.*.tls.client_ca. `record: json` was accepted and every
// record still framed by line; `on_parse_error: fail` was accepted and
// the parse error still merely counted.
func TestUnimplementedFramingOptionsAreRefused(t *testing.T) {
	mustSpec(t, framingBody("", "line"))
	mustSpec(t, framingBody("on_parse_error: count", "line"))

	if msg := specError(t, framingBody("", "json")); !strings.Contains(msg, "not implemented") {
		t.Errorf("record: json refusal should say it is unimplemented: %s", msg)
	}
	for _, v := range []string{"fail", "drop-stream"} {
		msg := specError(t, framingBody("on_parse_error: "+v, "line"))
		if !strings.Contains(msg, "not implemented") {
			t.Errorf("on_parse_error: %s refusal should say it is unimplemented: %s", v, msg)
		}
	}
	if msg := specError(t, framingBody("anchor: sideways", "line")); !strings.Contains(msg, "sideways") {
		t.Errorf("an unknown anchor should be named: %s", msg)
	}
}

const multilineJoinSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    framing:
      multiline:
        - start_contains: "BEGIN"
          continue_regex: '^\d+ CONT'
          join:
            - {regex: 'CONT (?P<x>keep\S*)', capture: 1}
    patterns:
      - set: s
        search: "BEGIN"
        extract: ['BEGIN (?P<n>\d+)']
`

// A continuation line that no join rule captures contributed nothing to
// the buffered record and nothing of its own. It used to vanish from
// both the samples and the unmatched tally, which is the one outcome a
// spec author cannot debug.
func TestUnjoinedContinuationIsCounted(t *testing.T) {
	spec := mustSpec(t, multilineJoinSpec)
	st, err := spec.NewStream(spec.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if _, err := st.Process("1700000000000 BEGIN 7"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := st.Process("1700000000001 CONT keepthis"); err != nil {
		t.Fatalf("joined continuation: %v", err)
	}
	if st.Stats.Unjoined != 0 {
		t.Fatalf("a joined continuation was counted as unjoined")
	}
	_, err = st.Process("1700000000002 CONT nothingmatches")
	if err != ErrNoJoin {
		t.Fatalf("unjoined continuation returned %v, expected ErrNoJoin", err)
	}
	if st.Stats.Unjoined != 1 {
		t.Fatalf("Unjoined = %d, expected 1", st.Stats.Unjoined)
	}
}

const aggErrSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    labels: [class]
    patterns:
      - set: errors
        search: "ERR"
        extract: ['ERR (?P<class>\w+)', 'ERR']
        aggregate: {every: 1s, on: [class], field: n, mode: increment}
`

// closeExpiredAggregators has already removed the windows it returns
// from the stream, so process() must hand them back even when the record
// that triggered the sweep then fails. Returning nil lost every window
// that happened to expire on the same record as a spec fault.
func TestExpiredWindowsSurviveAnAggregationError(t *testing.T) {
	spec := mustSpec(t, aggErrSpec)
	st, err := spec.NewStream(spec.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	if _, err := st.Process(itoa(base) + " ERR disk"); err != nil {
		t.Fatalf("open window: %v", err)
	}
	// Ten seconds later, a record whose second extract alternative
	// matches and captures no `class`, so aggregation cannot key it.
	out, err := st.Process(itoa(base+10_000) + " ERR")
	if err == nil {
		t.Fatal("expected the keyless record to report an error")
	}
	if len(out) != 1 {
		t.Fatalf("got %d window(s) back with the error, expected the one that expired", len(out))
	}
	if out[0].Set != "errors" {
		t.Fatalf("unexpected set %q", out[0].Set)
	}
}

// Cutting a record at a byte boundary must not leave a partial UTF-8
// sequence: the store rejects an invalid-UTF-8 label value by name, so
// an over-long record lost its whole sample rather than its tail.
func TestOversizeTruncationKeepsValidUTF8(t *testing.T) {
	// "aaé" is four bytes; a cap of three splits the final rune.
	if got := trimToRune("aa\xc3"); got != "aa" {
		t.Fatalf("trimToRune left a partial rune: %q", got)
	}
	if got := trimToRune("aaé"); got != "aaé" {
		t.Fatalf("trimToRune trimmed a whole rune: %q", got)
	}
	if got := trimToRune(""); got != "" {
		t.Fatalf("trimToRune on empty returned %q", got)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// ParseDuration accepts a leading sign so that Print -> Parse round-trips
// an unvalidated AST, so a spec's durations need a range check of their
// own. Without one, `retention: "-5s"` compiled cleanly, travelled to the
// store, and was refused there on every single write -- with a 500, which
// the client retries and then drops.
func TestNegativeSetDurationsAreASpecError(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"retention", `
version: 1
sets: {app: {retention: "-5s"}}
profiles:
  - name: p
    timestamp: {formats: [{layout: epoch_ms, regex: '^[0-9]+'}]}
    patterns: [{set: app, search: "x", extract: ['(?P<n>\d+)']}]
`, "retention"},
		{"shard", `
version: 1
sets: {app: {shard: "-1h"}}
profiles:
  - name: p
    timestamp: {formats: [{layout: epoch_ms, regex: '^[0-9]+'}]}
    patterns: [{set: app, search: "x", extract: ['(?P<n>\d+)']}]
`, "shard"},
		{"max_interval", `
version: 1
profiles:
  - name: p
    timestamp: {formats: [{layout: epoch_ms, regex: '^[0-9]+'}]}
    fields: {n: {kind: gauge, max_interval: "-30s"}}
    patterns: [{set: app, search: "x", extract: ['(?P<n>\d+)']}]
`, "max_interval"},
	} {
		if msg := specError(t, tc.body); !strings.Contains(msg, tc.want) {
			t.Errorf("%s: error %q does not name the offending setting", tc.name, msg)
		}
	}
}

// A sub-second cadence is a legitimate declaration and must survive
// compilation intact; the wire carries it in milliseconds.
func TestSubSecondMaxIntervalIsKept(t *testing.T) {
	s := mustSpec(t, `
version: 1
profiles:
  - name: p
    timestamp: {formats: [{layout: epoch_ms, regex: '^[0-9]+'}]}
    fields: {n: {kind: gauge, max_interval: "500ms"}}
    patterns: [{set: app, search: "x", extract: ['(?P<n>\d+)']}]
`)
	if got := s.Profiles[0].Fields["n"].MaxIntervalMs(); got != 500 {
		t.Fatalf("max_interval survived as %d ms, want 500", got)
	}
}

// A bucket the payload did not carry is absent, not zero. Writing Int(0)
// for it draws on a heatmap as a measured zero -- the same invention the
// "captured no buckets group" guard exists to prevent.
func TestPartialHistogramDoesNotInventZeroBuckets(t *testing.T) {
	bs := &BucketSet{Name: "lat", Parse: "key_value", Buckets: []string{"b0", "b1", "b2"}}
	fields := map[string]model.Value{}
	if err := bs.expand("b0=3 b2=4", fields); err != nil {
		t.Fatalf("expand: %v", err)
	}
	if _, invented := fields["b1"]; invented {
		t.Fatal("a bucket the payload never carried was recorded as a measured zero")
	}
	for _, name := range []string{"b0", "b2"} {
		if _, ok := fields[name]; !ok {
			t.Fatalf("bucket %s was captured but not recorded", name)
		}
	}
}

// The derived columns read every bucket, so with some missing they would
// be wrong -- and a wrong number is worse than an absent one.
func TestPartialHistogramSkipsDerivedColumns(t *testing.T) {
	bs := &BucketSet{Name: "lat", Parse: "key_value", Buckets: []string{"b0", "b1"}, Cumulative: true, Tail: true}
	partial := map[string]model.Value{}
	if err := bs.expand("b0=3", partial); err != nil {
		t.Fatalf("expand: %v", err)
	}
	for _, name := range []string{"tail", "b0plus", "b1plus"} {
		if _, ok := partial[name]; ok {
			t.Errorf("%s was derived from a payload that carried only some buckets", name)
		}
	}
	// A complete payload still derives them.
	full := map[string]model.Value{}
	if err := bs.expand("b0=3 b1=4", full); err != nil {
		t.Fatalf("expand: %v", err)
	}
	if _, ok := full["b0plus"]; !ok {
		t.Fatal("a complete payload no longer derives its cumulative columns")
	}
}

// A pattern may not write a reserved set, and neither may the `sets:`
// block. Without the check a spec declaring retention for one compiled,
// and the declaration then travelled with every write as metadata the
// store refuses outright -- one spec typo turning into a 400 on every
// batch the ingester produced.
//
// The ingest-progress set is the documented exemption, and it is the one
// name that has to be accepted here: applySetMeta exempts it by name
// precisely so a spec can age it out, and refusing it in the compiler made
// that exemption unreachable. The one set every ingester writes was then
// also the one set no spec could give a retention, so it was routed to the
// unsharded shard the sweep skips and kept forever.
func TestReservedSetNameIsRefusedInTheSetsBlock(t *testing.T) {
	body := `
version: 1
sets:
  %s: {retention: 1h}
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: app
        search: "x"
        extract: ['x (?P<n>\d+)']
`
	mustSpec(t, strings.Replace(body, "%s", "app", 1))
	spec := mustSpec(t, strings.Replace(body, "%s", model.IngestSet, 1))
	opt, ok := spec.Sets[model.IngestSet]
	if !ok {
		t.Fatalf("the ingest-progress set did not survive compilation")
	}
	if opt.RetentionMs() == nil || *opt.RetentionMs() != int64(time.Hour/time.Millisecond) {
		t.Errorf("retention for %s did not compile: %v", model.IngestSet, opt.RetentionMs())
	}
	for _, name := range []string{"_mensura_catalogue", "_mensura_x"} {
		msg := specError(t, strings.Replace(body, "%s", name, 1))
		if !strings.Contains(msg, "reserved") || !strings.Contains(msg, name) {
			t.Errorf("refusal for %q does not name the problem: %s", name, msg)
		}
	}
}

// `mode: increment` counts occurrences. Seeding the window with the
// captured value plus one meant a pattern that also extracted the field it
// counts started every window at that value, so the first window of each
// key reported a number nothing had counted.
func TestIncrementCountsOccurrencesNotTheCapturedValue(t *testing.T) {
	spec := mustSpec(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
      strip: true
    labels: [class]
    patterns:
      - set: errors
        search: "ERR"
        extract: [' ERR (?P<class>\w+) (?P<n>\d+)']
        aggregate: {every: 10s, on: [class], field: n, mode: increment}
`)
	st, err := spec.NewStream(spec.Profiles[0], StreamOptions{RefTime: time.Now()})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < 3; i++ {
		if _, err := st.Process(fmt.Sprintf("%d ERR disk 500", base+int64(i)*1000)); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	out := st.Flush()
	if len(out) != 1 {
		t.Fatalf("flush produced %d results, want 1", len(out))
	}
	got, _ := out[0].Fields["n"].AsInt()
	if got != 3 {
		t.Errorf("increment counted %d occurrences, want 3: the captured value seeded the window", got)
	}
}

// `parse: json_object` is listed in 03-extraction.md section 8 and in the
// Parse field's own comment, but expand() has no case for it. Left
// unchecked it compiled cleanly -- so `check --spec` reported the spec
// good -- and then failed on every single record at run time, losing the
// whole histogram.
func TestUnimplementedBucketParseModeIsRefusedAtCompileTime(t *testing.T) {
	body := `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: lat
        search: "hist"
        bucket_set: b
        extract: ['hist (?P<buckets>.*)']
    bucket_sets:
      - name: b
        parse: %s
        buckets: ['00','01']
`
	for _, mode := range []string{"paren_pairs", "csv", "key_value"} {
		mustSpec(t, strings.Replace(body, "%s", mode, 1))
	}
	for _, mode := range []string{"json_object", "yaml", "Paren_Pairs"} {
		msg := specError(t, strings.Replace(body, "%s", mode, 1))
		if !strings.Contains(msg, mode) {
			t.Fatalf("mode %q: expected the message to name it, got %q", mode, msg)
		}
	}
}

// process() refuses a bucket-set pattern that captured no histogram
// payload, per record, for the life of the process. It is decidable from
// the regexes alone, so it is decided at compile time instead.
func TestBucketSetPatternNeedsABucketsCapture(t *testing.T) {
	body := `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: lat
        search: "hist"
        bucket_set: b
        extract: ['hist (?P<op>\S+)']
    bucket_sets:
      - name: b
        buckets: ['00','01']
`
	if msg := specError(t, body); !strings.Contains(msg, "buckets") {
		t.Fatalf("expected the message to name the missing capture, got %q", msg)
	}
}

// The accumulator keys a window on labels[on]. An `on` key that no regex
// captures, or that is captured but never classified as a label, makes
// every record fail with "aggregation key is not a declared label" and
// the pattern produce nothing. Both halves are decidable from the spec.
func TestAggregationKeyMustBeCapturedAndDeclaredALabel(t *testing.T) {
	const body = `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    labels: [%s]
    patterns:
      - set: errors
        search: "ERR"
        extract: ['ERR (?P<class>\w+)']
        aggregate: {every: 10s, on: [%s], field: n, mode: increment}
`
	mustSpec(t, strings.NewReplacer("%s", "class").Replace(body))
	// Captured, but not declared as a label: it lands in fields, so the
	// lookup never finds it.
	if msg := specError(t, strings.Replace(strings.Replace(body, "%s", "other", 1), "%s", "class", 1)); !strings.Contains(msg, "class") {
		t.Fatalf("expected the undeclared label to be named, got %q", msg)
	}
	// Declared, but nothing captures it.
	if msg := specError(t, strings.Replace(strings.Replace(body, "%s", "nope", 1), "%s", "nope", 1)); !strings.Contains(msg, "nope") {
		t.Fatalf("expected the uncaptured key to be named, got %q", msg)
	}
}

// `tail: true` writes a field literally called "tail", so a pattern that
// also captures something by that name had one silently overwrite the
// other -- expand() runs after the captures are collected, so the
// histogram tail always won.
func TestTailFieldCollisionIsRefused(t *testing.T) {
	body := `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: lat
        search: "hist"
        bucket_set: b
        extract: ['hist (?P<tail>\d+) (?P<buckets>.*)']
    bucket_sets:
      - name: b
        buckets: ['00','01']
        tail: %s
`
	mustSpec(t, strings.Replace(body, "%s", "false", 1))
	if msg := specError(t, strings.Replace(body, "%s", "true", 1)); !strings.Contains(msg, "tail") {
		t.Fatalf("expected the clash to be named, got %q", msg)
	}
}

// A joined multiline record is bounded like a single one. max_record_bytes
// capped each input line and the buffer a join appends into was capped by
// nothing, so a stream of continuation lines grew one string for as long
// as the idle timeout allowed -- per rule, and on the receive path per
// peer.
func TestMultilineJoinIsBoundedByMaxRecordBytes(t *testing.T) {
	s := mustSpec(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    framing:
      max_record_bytes: 64
      multiline:
        - start_contains: "BEGIN"
          continue_regex: '^\d+ more '
          join: [{regex: 'more (.*)$', capture: 1}]
    patterns:
      - set: s
        search: "BEGIN"
        extract: ['BEGIN (?P<v>\d+)']
`)
	st, err := s.NewStream(s.Profile("p"), StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if _, err := st.Process("1000 BEGIN 7"); err != nil {
		t.Fatalf("start line: %v", err)
	}
	for i := 0; i < 200; i++ {
		if _, err := st.Process(fmt.Sprintf("%d more %s", 1001+i, strings.Repeat("x", 100))); err != nil {
			t.Fatalf("continuation %d: %v", i, err)
		}
	}
	out := st.Flush()
	if len(out) != 1 {
		t.Fatalf("expected one joined record, got %d", len(out))
	}
	if n := len(out[0].Line); n > 64 {
		t.Fatalf("joined record is %d bytes, past the 64-byte cap", n)
	}
	if st.Stats.Oversize == 0 {
		t.Fatal("truncation that nothing counts is indistinguishable from data that was never there")
	}
}

// An identity rule that declares both keys is one rule: match_path
// selects which files it applies to, and regex says what to pull out of
// their heads. Running the two halves independently attached a
// path-scoped rule's content labels to files its own match_path had just
// declined.
func TestIdentityMatchPathScopesTheContentRegex(t *testing.T) {
	s := mustSpec(t, `
version: 1
identity:
  - match_path: 'web/(?P<host>[^/]+)\.log$'
    regex: 'node-id (?P<node>[0-9a-f]+)'
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: s
        search: "x"
        extract: ['(?P<v>x)']
`)
	head := []byte("node-id deadbeef\n")
	in := s.DiscoverIdentity("web/w1.log", head)
	if in["host"] != "w1" || in["node"] != "deadbeef" {
		t.Fatalf("a matching path should yield both halves, got %v", in)
	}
	out := s.DiscoverIdentity("db/d1.log", head)
	if len(out) != 0 {
		t.Fatalf("a path the rule declined must yield nothing, got %v", out)
	}
}

// acMatcher.FirstIndex returns the lowest pattern index whose literal
// occurs, and process() uses that one pattern and no other. So a pattern
// whose search contains an earlier pattern's search is dead: its set is
// never written, and nothing at run time says so, because the line did
// match something. 03-extraction.md section 1 promises `check` reports it.
func TestLintReportsUnreachablePatterns(t *testing.T) {
	s := mustSpec(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    labels: [pool]
    fields:
      v: {kind: gauge}
    patterns:
      - set: general
        search: "stats: "
        extract: ['stats: (?P<v>\d+)']
      - set: specific
        search: "stats: pool="
        extract: ['stats: pool=(?P<pool>\S+) (?P<v>\d+)']
`)
	lints := s.Lint()
	var found bool
	for _, l := range lints {
		if l.Code == "L001" && strings.Contains(l.Msg, "specific") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected the shadowed pattern to be reported, got %v", lints)
	}

	// An empty search shadows everything after it.
	s2 := mustSpec(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    fields:
      v: {kind: gauge}
    patterns:
      - set: catchall
        search: ""
        extract: ['(?P<v>\d+)']
      - set: never
        search: "ERROR"
        extract: ['ERROR (?P<v>\d+)']
`)
	if lints := s2.Lint(); len(lints) == 0 || lints[0].Code != "L001" {
		t.Fatalf("expected an empty search to shadow the rest, got %v", lints)
	}

	// The correct ordering reports nothing.
	s3 := mustSpec(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    labels: [pool]
    fields:
      v: {kind: gauge}
    patterns:
      - set: specific
        search: "stats: pool="
        extract: ['stats: pool=(?P<pool>\S+) (?P<v>\d+)']
      - set: general
        search: "stats: "
        extract: ['stats: (?P<v>\d+)']
`)
	for _, l := range s3.Lint() {
		if l.Code == "L001" {
			t.Fatalf("the specific-first ordering is reachable: %v", l)
		}
	}
}

// The other half of what 03-extraction.md section 1 promises: captures
// and declarations that resolve to nothing.
func TestLintReportsUnresolvedCapturesAndDeclarations(t *testing.T) {
	s := mustSpec(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    labels: [pool]
    fields:
      declared_but_never_captured: {kind: gauge}
    patterns:
      - set: s
        search: "x"
        extract: ['x (?P<pool>\S+) (?P<undeclared>\d+)']
`)
	codes := map[string]string{}
	for _, l := range s.Lint() {
		codes[l.Code] = l.Msg
	}
	if msg, ok := codes["L002"]; !ok || !strings.Contains(msg, "undeclared") {
		t.Fatalf("expected the unresolved capture to be reported, got %v", codes)
	}
	if msg, ok := codes["L003"]; !ok || !strings.Contains(msg, "declared_but_never_captured") {
		t.Fatalf("expected the orphaned field declaration to be reported, got %v", codes)
	}
}

// A negative scan_lines is a typo, and it used to survive compilation and
// then panic inside DiscoverIdentity: SplitN with a non-positive n returns
// nothing to slice and the reslice ran with a negative bound. That panic
// is unrecovered on the batch worker goroutines and on the follow poll
// goroutine, so one bad key took the whole ingester down.
func TestNegativeScanLinesIsRefused(t *testing.T) {
	msg := specError(t, `
version: 1
identity:
  - regex: '(?P<host>\w+)'
    scan_lines: -1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    patterns:
      - set: s
        search: 'x'
        extract: ['x=(?P<v>\d+)']
`)
	if !strings.Contains(msg, "scan_lines") {
		t.Fatalf("error does not name the key: %s", msg)
	}
}

// The bound is resolved at use as well as at compile time: it is a slice
// bound, and Spec values built by hand never went through Compile.
func TestDiscoverIdentitySurvivesAnUncompiledScanLines(t *testing.T) {
	s := &Spec{Version: 1, Identity: []IdentityRule{{ScanLines: -1, Regex: `(?P<host>\w+)`}}}
	if err := s.Compile(); err == nil {
		t.Fatal("Compile accepted a negative scan_lines")
	}
	// Compile refused it, so the rule is still uncompiled -- force the
	// regex in the way a hand-built Spec would and confirm the walk is
	// bounded rather than panicking.
	s.Identity[0].regex = regexp.MustCompile(`(?P<host>\w+)`)
	got := s.DiscoverIdentity("/var/log/a.log", []byte("web1 hello\n"))
	if got["host"] != "web1" {
		t.Fatalf("identity discovery returned %v, want host=web1", got)
	}
}

// Only a pattern the matcher can never select is fatal. The advisory
// findings describe a spec that works and declares more than it uses --
// which is exactly what `identity:` and --label produce -- and failing on
// them meant a correct spec could not pass `check`.
func TestOnlyUnreachablePatternsAreFatal(t *testing.T) {
	s := mustSpec(t, `
version: 1
defaults:
  labels: [dc]
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    fields:
      v: {kind: gauge}
    patterns:
      - set: s
        search: 'x='
        extract: ['x=(?P<v>\d+)']
`)
	lints := s.Lint()
	if len(lints) == 0 {
		t.Fatal("expected the undeclared operator label to be reported at all")
	}
	for _, l := range lints {
		if l.Fatal() {
			t.Fatalf("advisory finding %s is fatal, so a working spec fails `check`: %s", l.Code, l.Msg)
		}
	}
	if Fatal(lints) {
		t.Fatal("Fatal reported a fatal finding where there is none")
	}
}

// A shadowed pattern is a broken spec: its destination set is never
// written and nothing at run time says so.
func TestUnreachablePatternIsFatal(t *testing.T) {
	s := mustSpec(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    fields:
      v: {kind: gauge}
      w: {kind: gauge}
    patterns:
      - set: a
        search: 'x'
        extract: ['x=(?P<v>\d+)']
      - set: b
        search: 'xy'
        extract: ['xy=(?P<w>\d+)']
`)
	if !Fatal(s.Lint()) {
		t.Fatal("a pattern the matcher can never select was not reported as fatal")
	}
}

// A label the identity rules produce arrives on the stream, so declaring
// it is not declaring something unused. The two hard-coded names that
// used to stand in for this missed every other capture an identity regex
// can make.
func TestIdentityLabelsAreNotReportedUnused(t *testing.T) {
	s := mustSpec(t, `
version: 1
identity:
  - match_path: '(?P<cluster>[^/]+)/[^/]+\.log$'
profiles:
  - name: p
    labels: [cluster]
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    fields:
      v: {kind: gauge}
    patterns:
      - set: s
        search: 'x='
        extract: ['x=(?P<v>\d+)']
`)
	for _, l := range s.Lint() {
		if l.Code == "L004" {
			t.Fatalf("a label `identity:` supplies was reported unused: %s", l.Msg)
		}
	}
}

// A `route:` target is a destination set exactly as `set:` is, and it was
// the one that nothing checked. A spec routing to a name carrying the '@'
// that separates a set from its shard suffix, or to the store's reserved
// prefix, compiled cleanly and then failed at run time twice over: the
// field metadata Declarations() derives for that set comes back 400, which
// the write client classifies as fatal, so the first batch carrying it is
// dropped outright -- reported as a hole, which freezes every followed
// file's checkpoint -- and every sample the route produces is rejected by
// the store for the life of the process, with nothing pointing at the spec.
func TestRouteTargetSetNamesAreValidated(t *testing.T) {
	body := `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: app
        search: "x"
        route:
          - regex: 'x (?P<n>\d+)'
            set: %s
`
	// A plain name still compiles, and a route with no set of its own
	// still inherits the pattern's -- which is already validated.
	mustSpec(t, strings.Replace(body, "%s", "app_errors", 1))
	inherited := mustSpec(t, strings.Replace(body, "%s", `""`, 1))
	if got := inherited.Profiles[0].Patterns[0].Route[0].Set; got != "app" {
		t.Errorf("a route with no set of its own resolved to %q, expected the pattern's", got)
	}
	for _, name := range []string{"_mensura_ingest", "_mensura_x", `"has@at"`, `"has space"`} {
		msg := specError(t, strings.Replace(body, "%s", name, 1))
		if !strings.Contains(msg, "route") {
			t.Errorf("refusal for route target %s does not say it is a route: %s", name, msg)
		}
	}
}
