package extract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const appSpec = `
version: 1
defaults:
  timestamp:
    timezone: UTC
    assume_year: "2026"
identity:
  - match_path: '(?P<host>[^/]+)/[^/]+\.log$'
profiles:
  - name: appserver
    select: {path_glob: ['*/app*.log']}
    timestamp:
      formats:
        - {layout: '2006-01-02 15:04:05.000', regex: '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}'}
      anchor: prefix
      strip: true
    labels: [pool, error_class]
    fields:
      requests_total: {kind: counter, max_interval: 20s}
      inflight: {kind: gauge}
      errors: {kind: delta}
    patterns:
      - set: app
        search: 'stats: '
        extract:
          - 'stats: pool=(?P<pool>\S+) reqs=(?P<requests_total>\d+) inflight=(?P<inflight>\d+)'
      - set: app_errors
        search: 'ERROR '
        extract:
          - 'ERROR (?P<error_class>[A-Za-z.]+): '
        aggregate: {every: 10s, on: [error_class], field: errors, mode: increment}
`

func loadSpec(t *testing.T, src string) *Spec {
	t.Helper()
	s, err := Parse([]byte(src))
	if err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	return s
}

func newStream(t *testing.T, s *Spec, path string) *Stream {
	t.Helper()
	p := s.SelectProfile(path, nil, nil, "")
	if p == nil {
		t.Fatalf("no profile selected for %s", path)
	}
	st, err := s.NewStream(p, StreamOptions{})
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	return st
}

func TestExtractBasicPattern(t *testing.T) {
	s := loadSpec(t, appSpec)
	st := newStream(t, s, "web1/app.log")
	res, err := st.Process(`2026-08-28 10:41:02.113 stats: pool=default reqs=91823 inflight=7`)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected one sample, got %d", len(res))
	}
	r := res[0]
	if r.Set != "app" {
		t.Fatalf("set: %s", r.Set)
	}
	if r.Labels["pool"] != "default" {
		t.Fatalf("pool label not promoted: %+v", r.Labels)
	}
	if v, ok := r.Fields["requests_total"]; !ok || v.I != 91823 {
		t.Fatalf("requests_total: %+v", r.Fields)
	}
	if _, isLabelledAsField := r.Fields["pool"]; isLabelledAsField {
		t.Fatal("a declared label must not also become a field")
	}
	want := time.Date(2026, 8, 28, 10, 41, 2, 113_000_000, time.UTC).UnixMilli()
	if r.TSMs != want {
		t.Fatalf("timestamp: got %d want %d", r.TSMs, want)
	}
}

func TestUnmatchedLinesAreCountedNotSilent(t *testing.T) {
	s := loadSpec(t, appSpec)
	st := newStream(t, s, "web1/app.log")
	if _, err := st.Process(`2026-08-28 10:41:02.113 nothing interesting here`); err != ErrNoMatch {
		t.Fatalf("expected ErrNoMatch, got %v", err)
	}
	if st.Stats.Unmatched != 1 || len(st.Stats.FirstUnmatched) != 1 {
		t.Fatalf("unmatched lines must be counted and sampled: %+v", st.Stats)
	}
	if _, err := st.Process(`no timestamp at all`); err != ErrNoTimestamp {
		t.Fatalf("expected ErrNoTimestamp, got %v", err)
	}
	if st.Stats.TSParseErrors != 1 {
		t.Fatalf("timestamp failures must be counted: %+v", st.Stats)
	}
}

func TestAggregationWindow(t *testing.T) {
	s := loadSpec(t, appSpec)
	st := newStream(t, s, "web1/app.log")
	for i := 0; i < 3; i++ {
		if _, err := st.Process(`2026-08-28 10:41:0` + string(rune('0'+i)) + `.000 ERROR Timeout: upstream gone`); err != nil {
			t.Fatalf("process: %v", err)
		}
	}
	// Nothing is emitted until the window closes.
	out, _ := st.Flush()
	if len(out) != 1 {
		t.Fatalf("expected one aggregated sample, got %d", len(out))
	}
	if v, _ := out[0].Fields["errors"].AsFloat(); v != 3 {
		t.Fatalf("expected 3 counted errors, got %v", v)
	}
	if out[0].Labels["error_class"] != "Timeout" {
		t.Fatalf("aggregation must keep its key labels: %+v", out[0].Labels)
	}
}

func TestAggregationClosesOnWindowEnd(t *testing.T) {
	s := loadSpec(t, appSpec)
	st := newStream(t, s, "web1/app.log")
	_, _ = st.Process(`2026-08-28 10:41:00.000 ERROR Timeout: x`)
	// 11 s later: past the 10 s window, so the previous window is emitted.
	out, _ := st.Process(`2026-08-28 10:41:11.000 ERROR Timeout: x`)
	if len(out) != 1 {
		t.Fatalf("expected the closed window to be emitted, got %d", len(out))
	}
	if v, _ := out[0].Fields["errors"].AsFloat(); v != 1 {
		t.Fatalf("expected 1, got %v", v)
	}
}

const histSpec = `
version: 1
profiles:
  - name: hist
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    labels: [op]
    patterns:
      - set: latency
        search: 'histogram'
        bucket_set: h4
        extract:
          - 'histogram (?P<op>\S+) \((?P<total>\d+) total\) (?P<buckets>.*)'
    bucket_sets:
      - name: h4
        parse: paren_pairs
        buckets: ['00','01','02','03']
        edges: pow2
        total_field: total
        cumulative: true
        tail: true
`

func TestHistogramExpansion(t *testing.T) {
	s := loadSpec(t, histSpec)
	st := newStream(t, s, "x.log")
	res, err := st.Process(`1756400000000 histogram read (100 total) (00: 60) (01: 20) (02: 10) (03: 5)`)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	f := res[0].Fields
	for name, want := range map[string]int64{"00": 60, "01": 20, "02": 10, "03": 5, "tail": 5} {
		if v, ok := f[name]; !ok || v.I != want {
			t.Fatalf("bucket %s: got %+v want %d", name, f[name], want)
		}
	}
	// Cumulative counts everything at or above a bucket, tail included.
	for name, want := range map[string]int64{"03plus": 10, "02plus": 20, "01plus": 40, "00plus": 100} {
		if v, ok := f[name]; !ok || v.I != want {
			t.Fatalf("cumulative %s: got %+v want %d", name, f[name], want)
		}
	}
	bs := st.Profile().buckets["h4"]
	if got := bs.EdgeValues(); got[0] != 0 || got[1] != 1 || got[2] != 2 || got[3] != 4 {
		t.Fatalf("pow2 edges: %+v", got)
	}
}

const mlSpec = `
version: 1
profiles:
  - name: ml
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    framing:
      multiline:
        - start_contains: 'dump start'
          continue_regex: '^ cont '
          join:
            - {regex: '^ cont(.*)', capture: 1}
    patterns:
      - set: dumps
        search: 'dump start'
        extract:
          - 'dump start (?P<a>\d+) (?P<b>\d+)'
`

func TestMultilineJoin(t *testing.T) {
	s := loadSpec(t, mlSpec)
	st := newStream(t, s, "x.log")
	if out, err := st.Process(`1756400000000 dump start 1`); err != nil || len(out) != 0 {
		t.Fatalf("start line should buffer: %v %v", out, err)
	}
	if out, err := st.Process(`1756400000001 cont 2`); err != nil || len(out) != 0 {
		t.Fatalf("continuation should buffer: %v %v", out, err)
	}
	out, _ := st.Flush()
	if len(out) != 1 {
		t.Fatalf("expected the joined record on flush, got %d", len(out))
	}
	if v, ok := out[0].Fields["b"]; !ok || v.I != 2 {
		t.Fatalf("join did not append the continuation: %+v", out[0].Fields)
	}
}

func TestRouteFansOutToSets(t *testing.T) {
	s := loadSpec(t, `
version: 1
profiles:
  - name: r
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    patterns:
      - set: base
        search: 'hist'
        route:
          - {regex: 'hist (?P<v>\d+) msec', set: hist_ms}
          - {regex: 'hist (?P<v>\d+) usec', set: hist_us}
`)
	st := newStream(t, s, "x.log")
	out, err := st.Process(`1756400000000 hist 5 usec`)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if out[0].Set != "hist_us" {
		t.Fatalf("expected the usec route, got %s", out[0].Set)
	}
}

func TestIdentityDiscovery(t *testing.T) {
	s := loadSpec(t, appSpec)
	got := s.DiscoverIdentity("/var/log/web7/app.log", nil)
	if got["host"] != "web7" {
		t.Fatalf("expected host=web7, got %+v", got)
	}
}

func TestCompileRejectsBadSpecs(t *testing.T) {
	cases := map[string]string{
		"unknown field":  "version: 1\nwat: true\n",
		"bad version":    "version: 9\n",
		"reserved set":   "version: 1\nprofiles:\n  - name: p\n    timestamp:\n      formats: [{layout: epoch_ms, regex: 'x'}]\n    patterns:\n      - set: _mensura_x\n        search: a\n        extract: ['(?P<v>x)']\n",
		"no extract":     "version: 1\nprofiles:\n  - name: p\n    timestamp:\n      formats: [{layout: epoch_ms, regex: 'x'}]\n    patterns:\n      - set: s\n        search: a\n",
		"bad regex":      "version: 1\nprofiles:\n  - name: p\n    timestamp:\n      formats: [{layout: epoch_ms, regex: 'x'}]\n    patterns:\n      - set: s\n        search: a\n        extract: ['(?P<v>']\n",
		"unknown bucket": "version: 1\nprofiles:\n  - name: p\n    timestamp:\n      formats: [{layout: epoch_ms, regex: 'x'}]\n    patterns:\n      - set: s\n        search: a\n        extract: ['(?P<v>x)']\n        bucket_set: nope\n",
		"no ts formats":  "version: 1\nprofiles:\n  - name: p\n    patterns:\n      - set: s\n        search: a\n        extract: ['(?P<v>x)']\n",
	}
	for name, src := range cases {
		if _, err := Parse([]byte(src)); err == nil {
			t.Fatalf("%s: expected a compile error", name)
		}
	}
}

func TestACMatcherFirstMatchWins(t *testing.T) {
	m := newACMatcher([]string{"beta", "alpha", "al"})
	if got := m.FirstIndex("xx alpha yy"); got != 1 {
		t.Fatalf("expected the lowest matching index 1, got %d", got)
	}
	if got := m.FirstIndex("only al here"); got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}
	if got := m.FirstIndex("nothing"); got != -1 {
		t.Fatalf("expected -1, got %d", got)
	}
	if got := m.FirstIndex("beta and alpha"); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestProfileSelection(t *testing.T) {
	s := loadSpec(t, appSpec+`
  - name: fallback
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    patterns:
      - set: other
        search: x
        extract: ['(?P<v>x)']
`)
	if p := s.SelectProfile("web1/app.log", nil, nil, ""); p == nil || p.Name != "appserver" {
		t.Fatalf("expected the appserver profile, got %+v", p)
	}
	if p := s.SelectProfile("web1/other.log", nil, nil, ""); p == nil || p.Name != "fallback" {
		t.Fatalf("expected the fallback profile, got %+v", p)
	}
}

func TestOversizeRecordTruncated(t *testing.T) {
	s := loadSpec(t, strings.Replace(appSpec, "    labels: [pool, error_class]",
		"    framing: {max_record_bytes: 40}\n    labels: [pool, error_class]", 1))
	st := newStream(t, s, "web1/app.log")
	_, _ = st.Process(`2026-08-28 10:41:02.113 stats: pool=default reqs=91823 inflight=7`)
	if st.Stats.Oversize != 1 {
		t.Fatalf("expected the oversize counter to move: %+v", st.Stats)
	}
}

// An included spec's defaults used to be parsed and then dropped, so a
// base file holding the timezone, the year assumption or the shared label
// list was silently ignored and everything fell back to UTC.
func TestIncludeCarriesDefaults(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.yaml")
	if err := os.WriteFile(base, []byte(`
version: 1
defaults:
  timestamp:
    timezone: Europe/Warsaw
    assume_year: "2021"
  labels: [tier]
`), 0o644); err != nil {
		t.Fatal(err)
	}
	main := filepath.Join(dir, "main.yaml")
	if err := os.WriteFile(main, []byte(`
version: 1
include: [base.yaml]
defaults:
  labels: [pool]
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    patterns:
      - set: s
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`), 0o644); err != nil {
		t.Fatal(err)
	}
	spec, err := Load(main)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if spec.Defaults.Timestamp.Timezone != "Europe/Warsaw" {
		t.Fatalf("timezone %q was not inherited from the include", spec.Defaults.Timestamp.Timezone)
	}
	if spec.Defaults.Timestamp.AssumeYear != "2021" {
		t.Fatalf("assume_year %q was not inherited", spec.Defaults.Timestamp.AssumeYear)
	}
	p := spec.Profile("p")
	for _, want := range []string{"pool", "tier"} {
		if _, ok := p.labelSet[want]; !ok {
			t.Fatalf("default label %q is missing; the include's labels were dropped", want)
		}
	}
}

// A continuation line carrying an earlier timestamp used to delete the
// buffered multiline record, so the whole record vanished and only a
// counter recorded it.
func TestBackwardsMultilineEmitsTheBufferedRecord(t *testing.T) {
	spec, err := Parse([]byte(`
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
        - start_contains: 'ERROR'
          continue_regex: '^\s+at '
          join:
            - {regex: 'at (?P<frame>\S+)', capture: 1}
    patterns:
      - set: s
        search: 'ERROR'
        extract: ['ERROR (?P<msg>\w+)']
`))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	st, err := spec.NewStream(spec.Profile("p"), StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if _, err := st.Process("1756382400000 ERROR boom"); err != nil {
		t.Fatalf("open: %v", err)
	}
	// A continuation stamped earlier than the record it belongs to.
	out, perr := st.Process("1756382300000     at frame.one")
	if perr == nil {
		t.Fatal("expected the backwards timestamp to be reported")
	}
	if len(out) != 1 {
		t.Fatalf("the buffered record was discarded rather than emitted: %d result(s)", len(out))
	}
	if out[0].Fields["msg"].S != "boom" {
		t.Fatalf("wrong record emitted: %+v", out[0].Fields)
	}
}
