package extract

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
)

// A nanosecond value declared as epoch_s must not be multiplied on trust.
//
// n*1000 overflows int64 and wraps to a timestamp that is not the one in
// the record -- sometimes past model.MaxTSMs, which is caught, and
// sometimes back inside it, which is not. The realistic source is a spec
// declaring the wrong unit, so it is reported as a timestamp failure:
// counted and dropped, rather than stored at a fabricated time.
func TestEpochSecondsRefusesAValueThatCannotBeSeconds(t *testing.T) {
	// A plausible epoch-nanosecond reading. Held in a variable, not a
	// constant: the multiplication below is the overflow under test, and
	// the compiler refuses to fold it.
	nanos := int64(1756382400000000000)
	if _, err := epochMillis("epoch_s", nanos); err == nil {
		t.Fatalf("epoch_s accepted %d, which cannot be a second count", nanos)
	}
	// The wrap is real, not theoretical: this is what would have been
	// stored, and it is not the timestamp in the record.
	if wrapped := nanos * 1000; wrapped == nanos*1000 {
		t.Logf("unchecked, the record would land at %d (MaxTSMs is %d)", wrapped, model.MaxTSMs)
	}
}

func TestEpochUnitsConvert(t *testing.T) {
	cases := []struct {
		layout string
		in     int64
		want   int64
	}{
		{"epoch_s", 1756382400, 1756382400000},
		{"epoch_ms", 1756382400123, 1756382400123},
		{"epoch_us", 1756382400123456, 1756382400123},
		{"epoch_ns", 1756382400123456789, 1756382400123},
		// Pre-epoch values floor rather than truncate towards zero, so a
		// timestamp lands in the millisecond it belongs to.
		{"epoch_us", -1500, -2},
		{"epoch_ns", -1_500_000, -2},
	}
	for _, c := range cases {
		got, err := epochMillis(c.layout, c.in)
		if err != nil {
			t.Fatalf("%s %d: %v", c.layout, c.in, err)
		}
		if got != c.want {
			t.Fatalf("%s %d: got %d, want %d", c.layout, c.in, got, c.want)
		}
	}
}

const heldSpec = `
version: 1
profiles:
  - name: held
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
    framing:
      multiline:
        - start_contains: 'BEGIN'
          continue_regex: '^CONT'
          join: [{regex: '^CONT (.*)$', capture: 1}]
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`

// HeldFrom names the oldest record the stream is still holding, so a
// driver that checkpoints byte offsets can stop short of it. Before it
// existed such a record was indistinguishable from one that had been
// dealt with, and its bytes were acknowledged while its sample lived only
// in this struct.
func TestHeldFromNamesTheOldestBufferedRecord(t *testing.T) {
	spec, err := Parse([]byte(heldSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := spec.NewStream(spec.Profile("held"), StreamOptions{RefTime: time.Now()})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	// A plain record is dealt with on the spot.
	st.Mark(10)
	if out, err := st.Process("1756382400000 n=1"); err != nil || len(out) != 1 {
		t.Fatalf("plain record: %v %v", out, err)
	}
	if _, held := st.HeldFrom(); held {
		t.Fatal("a record that produced its sample is still reported as held")
	}

	// A record that only opens a block is held, at its own mark.
	st.Mark(20)
	if out, err := st.Process("1756382401000 BEGIN n=2"); err != nil || len(out) != 0 {
		t.Fatalf("block opener: %v %v", out, err)
	}
	at, held := st.HeldFrom()
	if !held || at != 20 {
		t.Fatalf("HeldFrom = (%d, %v), want (20, true)", at, held)
	}

	// A later record that produces its own sample does not release the
	// block: the block's data is still only in memory.
	st.Mark(30)
	if out, err := st.Process("1756382402000 n=3"); err != nil || len(out) != 1 {
		t.Fatalf("record after the block: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 20 {
		t.Fatalf("HeldFrom = (%d, %v) after an unrelated record, want (20, true)", at, held)
	}

	// A second opener closes the first block and starts its own, so the
	// hold-back moves forward rather than pinning at the first mark
	// forever.
	st.Mark(40)
	if out, err := st.Process("1756382403000 BEGIN n=4"); err != nil || len(out) != 1 {
		t.Fatalf("second opener: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 40 {
		t.Fatalf("HeldFrom = (%d, %v) after the block closed, want (40, true)", at, held)
	}

	// Flushing releases everything.
	st.Flush()
	if _, held := st.HeldFrom(); held {
		t.Fatal("the stream still reports held state after Flush")
	}
}

const aggHeldSpec = `
version: 1
profiles:
  - name: agg
    select: {}
    labels: [code]
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
    patterns:
      - set: hits
        search: 'code='
        extract: ['code=(?P<code>\d+)']
        aggregate: {every: 1m, on: [code], field: hits, mode: increment}
`

// An aggregation window holds the records it has absorbed, and releases
// them when it closes.
func TestHeldFromCoversAnOpenAggregationWindow(t *testing.T) {
	spec, err := Parse([]byte(aggHeldSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := spec.NewStream(spec.Profile("agg"), StreamOptions{RefTime: time.Now()})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()

	st.Mark(100)
	if out, err := st.Process(itoa(base) + " code=200"); err != nil || len(out) != 0 {
		t.Fatalf("first record: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 100 {
		t.Fatalf("HeldFrom = (%d, %v), want (100, true)", at, held)
	}
	// A second record folds into the same window; the hold-back stays at
	// the record that opened it.
	st.Mark(200)
	if out, err := st.Process(itoa(base+1000) + " code=200"); err != nil || len(out) != 0 {
		t.Fatalf("second record: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 100 {
		t.Fatalf("HeldFrom = (%d, %v) inside the window, want (100, true)", at, held)
	}
	// A record past the window's end closes it and opens the next one.
	st.Mark(300)
	out, err := st.Process(itoa(base+120_000) + " code=200")
	if err != nil || len(out) != 1 {
		t.Fatalf("window close: %v %v", out, err)
	}
	if at, held := st.HeldFrom(); !held || at != 300 {
		t.Fatalf("HeldFrom = (%d, %v) after the window closed, want (300, true)", at, held)
	}
}

// One junk value must cost its own record, not the whole window.
//
// model.Coerce turns the literal text "NaN", "Inf" or "+Infinity" in a
// log line into exactly that float -- strconv.ParseFloat accepts all
// three. The accumulator discarded AsFloat's verdict and folded it in, so
// `sum` stayed NaN for every later record and a window seeded with one
// never moved again (`incoming > NaN` is false). The window's sample is
// then refused outright by the sink as unencodable, so a single bad line
// silently erased every record that shared its window.
func TestAggregationSurvivesANonFiniteValue(t *testing.T) {
	const spec = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    labels: [op]
    patterns:
      - set: s
        search: 'lat'
        extract: ['lat op=(?P<op>\w+) v=(?P<v>\S+)']
        aggregate:
          every: 1h
          on: [op]
          field: v
          mode: MODE
`
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	for _, tc := range []struct {
		mode string
		want string
	}{
		{"sum", "6"},
		{"max", "3"},
		{"last", "3"},
	} {
		sp, err := Parse([]byte(strings.ReplaceAll(spec, "MODE", tc.mode)))
		if err != nil {
			t.Fatalf("%s: spec: %v", tc.mode, err)
		}
		st, err := sp.NewStream(sp.Profile("p"), StreamOptions{})
		if err != nil {
			t.Fatalf("%s: stream: %v", tc.mode, err)
		}
		junk := 0
		for i, v := range []string{"1", "2", "NaN", "not-a-number", "3"} {
			line := fmt.Sprintf("%d lat op=read v=%s", base+int64(i)*1000, v)
			if _, err := st.Process(line); err != nil {
				junk++
			}
		}
		if junk != 2 {
			t.Errorf("%s: %d record(s) reported as unusable, want 2", tc.mode, junk)
		}
		out := st.Flush()
		if len(out) != 1 {
			t.Fatalf("%s: %d window(s), want 1", tc.mode, len(out))
		}
		if got := out[0].Fields["v"].String(); got != tc.want {
			t.Errorf("%s: window value %s, want %s: a junk record poisoned the whole window", tc.mode, got, tc.want)
		}
	}
}

// increment counts occurrences, so a junk value in the field it names is
// irrelevant to it: the record still happened.
func TestIncrementAggregationIgnoresTheFieldValue(t *testing.T) {
	const spec = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    labels: [op]
    patterns:
      - set: s
        search: 'lat'
        extract: ['lat op=(?P<op>\w+) v=(?P<v>\S+)']
        aggregate:
          every: 1h
          on: [op]
          field: v
          mode: increment
`
	sp, err := Parse([]byte(spec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := sp.NewStream(sp.Profile("p"), StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	for i, v := range []string{"1", "NaN", "x"} {
		if _, err := st.Process(fmt.Sprintf("%d lat op=read v=%s", base+int64(i)*1000, v)); err != nil {
			t.Errorf("increment refused a record over its field value: %v", err)
		}
	}
	out := st.Flush()
	if len(out) != 1 || out[0].Fields["v"].String() != "3" {
		t.Fatalf("counted %v, want one window of 3", out)
	}
}

// maxSeedSpec aggregates a field that is not on every matching record, so
// a window can be opened by a record that carries no value.
const maxSeedSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    labels: [class]
    fields:
      n: {kind: gauge}
    patterns:
      - set: temps
        search: "T"
        extract:
          - 'T (?P<class>\w+) n=(?P<n>-?[0-9]+)'
          - 'T (?P<class>\w+) idle'
        aggregate: {every: 10s, on: [class], field: n, mode: max}
`

// `mode: max` must not treat the zero an empty window starts at as a
// reading.
//
// A window opened by a record that does not carry the field started at
// zero, and zero is a floor no negative value can beat, so a window of
// only negative readings reported 0 -- a number nothing measured, on
// exactly the metrics where negative values are the point (a temperature,
// a clock skew, a free-space delta).
func TestMaxAggregationDoesNotSeedAWindowAtZero(t *testing.T) {
	spec := mustSpec(t, maxSeedSpec)
	st, err := spec.NewStream(spec.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	// The window opens on a record with no n at all, then sees only
	// negative readings.
	for _, line := range []string{
		"1000 T cpu idle",
		"2000 T cpu n=-9",
		"3000 T cpu n=-4",
	} {
		if _, err := st.Process(line); err != nil {
			t.Fatalf("process %q: %v", line, err)
		}
	}
	out := st.Flush()
	if len(out) != 1 {
		t.Fatalf("expected one window, got %d", len(out))
	}
	got, ok := out[0].Fields["n"].AsFloat()
	if !ok {
		t.Fatalf("window value is not numeric: %+v", out[0].Fields["n"])
	}
	if got != -4 {
		t.Fatalf("max over {-9, -4} reported %v; the empty window's zero was treated as a reading", got)
	}
}

// A document with include: cannot be compiled from bytes.
//
// There is no file to resolve the paths against, so the key was decoded
// and dropped: the spec compiled without every profile, identity rule and
// set option the base contributed, and then reported "no profile matched"
// for files the same spec loaded from disk handles.
func TestParseRefusesASpecThatDeclaresIncludes(t *testing.T) {
	body := "version: 1\ninclude: [base.yaml]\nprofiles: []\n"
	msg := specError(t, body)
	if !strings.Contains(msg, "include") || !strings.Contains(msg, "base.yaml") {
		t.Fatalf("refusal does not name the includes: %s", msg)
	}
}

// A field kind the rest of the system does not act on must be refused at
// compile time.
//
// It used to compile: a kind is a plain string all the way from `fields:`
// into the catalogue, and mql.Validate compares it against the four it
// knows and ignores anything else. So `kind: couter` shipped, and
// silently withdrew every behaviour it was written to switch on -- no
// W102 "counter is plotted raw; consider RATE", and for a mistyped
// `string` no E005 and no string column under FORMAT table.
func TestCompileRefusesAnUnknownFieldKind(t *testing.T) {
	body := `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d+'}]
    fields:
      n: {kind: couter}
    patterns:
      - set: s
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`
	msg := specError(t, body)
	if !strings.Contains(msg, "couter") || !strings.Contains(msg, "counter, gauge, delta or string") {
		t.Fatalf("refusal does not name the kind and the choices: %s", msg)
	}
	// The four documented spellings still compile.
	for _, kind := range []string{"counter", "gauge", "delta", "string"} {
		ok := strings.Replace(body, "kind: couter", "kind: "+kind, 1)
		if _, err := Parse([]byte(ok)); err != nil {
			t.Fatalf("kind %q was refused: %v", kind, err)
		}
	}
}

// A declared label that the store cannot accept must be refused here,
// where the spec author can see it.
//
// It used to compile, and every sample the profile produced was then
// rejected by the store one at a time, for the life of the process, with
// nothing anywhere pointing back at the spec. "timestamp" is the worst
// case: it names the indexed column.
func TestCompileRefusesAnInvalidDeclaredLabel(t *testing.T) {
	tmpl := `
version: 1
defaults:
  labels: [DEFAULTS]
profiles:
  - name: p
    select: {}
    labels: [PROFILE]
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d+'}]
    patterns:
      - set: s
        search: 'n='
        labels: [PATTERN]
        extract: ['n=(?P<n>\d+)']
`
	spec := func(defaults, profile, pattern string) string {
		body := strings.Replace(tmpl, "DEFAULTS", defaults, 1)
		body = strings.Replace(body, "PROFILE", profile, 1)
		return strings.Replace(body, "PATTERN", pattern, 1)
	}
	if _, err := Parse([]byte(spec("dc", "env", "host"))); err != nil {
		t.Fatalf("a spec whose labels are all valid was refused: %v", err)
	}
	for _, tc := range []struct{ where, defaults, profile, pattern string }{
		{"defaults.labels", "timestamp", "env", "host"},
		{"labels", "dc", "1st", "host"},
		{"pattern for set", "dc", "env", "timestamp"},
	} {
		msg := specError(t, spec(tc.defaults, tc.profile, tc.pattern))
		if !strings.Contains(msg, tc.where) {
			t.Fatalf("refusal does not say the label came from %s: %s", tc.where, msg)
		}
	}
}

// A bucket set's total_field is an ordinary capture, so model.Coerce may
// turn it into a float far outside the int64 range. AsInt used to convert
// it anyway -- a conversion the Go spec leaves undefined -- so `tail` and
// every `<bucket>plus` column were derived from a total nothing measured.
// A total this function cannot represent leaves the declared sum in
// place, which is the honest fallback the code already has for a missing
// total.
func TestAnUnrepresentableTotalDoesNotFabricateATail(t *testing.T) {
	bs := &BucketSet{
		Name: "h", Parse: "paren_pairs", Buckets: []string{"00", "01"},
		Edges: "pow2", TotalField: "total", Tail: true, Cumulative: true,
	}
	if err := bs.compile(); err != nil {
		t.Fatalf("compile: %v", err)
	}
	fields := map[string]model.Value{"total": model.Coerce("1e300")}
	if err := bs.expand("(00: 3) (01: 4)", fields); err != nil {
		t.Fatalf("expand: %v", err)
	}
	tail, ok := fields[tailField].AsInt()
	if !ok {
		t.Fatal("no tail column was written")
	}
	if tail != 0 {
		t.Fatalf("tail is %d; an unrepresentable total must not invent a count", tail)
	}
	// The cumulative columns are derived from the same tail.
	if v, _ := fields["01plus"].AsInt(); v != 4 {
		t.Fatalf("01plus is %d, want 4", v)
	}
	if v, _ := fields["00plus"].AsInt(); v != 7 {
		t.Fatalf("00plus is %d, want 7", v)
	}
}
