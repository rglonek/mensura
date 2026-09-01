package extract

import (
	"strings"
	"testing"
	"time"
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
