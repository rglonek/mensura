package extract

import (
	"strings"
	"testing"
	"time"
)

const syslogSpec = `
version: 1
defaults:
  timestamp:
    timezone: UTC
profiles:
  - name: p
    timestamp:
      formats: [{layout: 'Jan _2 15:04:05', regex: '^[A-Z][a-z]{2} [ \d]\d \d{2}:\d{2}:\d{2}'}]
      anchor: prefix
      strip: true
    patterns:
      - set: s
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`

func syslogStream(t *testing.T, ref time.Time) *Stream {
	t.Helper()
	s, err := Parse([]byte(syslogSpec))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	st, err := s.NewStream(s.Profiles[0], StreamOptions{RefTime: ref})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	return st
}

func mustTS(t *testing.T, st *Stream, line string) time.Time {
	t.Helper()
	r, err := st.Process(line)
	if err != nil {
		t.Fatalf("process %q: %v", line, err)
	}
	if len(r) != 1 {
		t.Fatalf("process %q: got %d samples", line, len(r))
	}
	return time.UnixMilli(r[0].TSMs).UTC()
}

// A follow or receive that stays open across New Year used to date the
// January records as last January: assumeYear is locked at stream open,
// "Jan  1" after "Dec 31" lands in the past, and the wrap only ever
// decremented.
func TestYearlessFollowsNewYear(t *testing.T) {
	ref := time.Date(2024, 12, 31, 23, 0, 0, 0, time.UTC)
	st := syslogStream(t, ref)
	dec := mustTS(t, st, "Dec 31 23:59:59 n=1")
	jan := mustTS(t, st, "Jan  1 00:00:01 n=2")
	if got, want := dec, time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("Dec 31: %s, want %s", got, want)
	}
	if got, want := jan, time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("Jan 1 after Dec 31: %s, want %s", got, want)
	}
}

// A batch whose mtime is in January and whose first line is in December
// used to date that December as *next* December: there was no previous
// record to wrap against, so the assumed year stood.
func TestYearlessBatchStartsInDecember(t *testing.T) {
	ref := time.Date(2025, 1, 2, 10, 0, 0, 0, time.UTC)
	st := syslogStream(t, ref)
	dec := mustTS(t, st, "Dec 31 23:59:59 n=1")
	jan := mustTS(t, st, "Jan  1 00:00:01 n=2")
	if got, want := dec, time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("Dec 31 with January mtime: %s, want %s", got, want)
	}
	if got, want := jan, time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("Jan 1 after wrapped Dec 31: %s, want %s", got, want)
	}
}

// The documented wrap still fires: a December line after a January one,
// with the assumed year already the new year, is last December.
func TestYearlessDecrementAfterJanuary(t *testing.T) {
	ref := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	st := syslogStream(t, ref)
	jan := mustTS(t, st, "Jan 15 00:00:00 n=1")
	dec := mustTS(t, st, "Dec 31 23:59:59 n=2")
	if got, want := jan, time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("Jan 15: %s, want %s", got, want)
	}
	if got, want := dec, time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("Dec 31 after Jan 15: %s, want %s", got, want)
	}
}

// A gap of a few days is not a wrap in either direction.
func TestYearlessGapIsNotAWrap(t *testing.T) {
	ref := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	st := syslogStream(t, ref)
	a := mustTS(t, st, "Jun  1 00:00:00 n=1")
	b := mustTS(t, st, "Jun  8 00:00:00 n=2")
	if got, want := a, time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("Jun 1: %s, want %s", got, want)
	}
	if got, want := b, time.Date(2024, 6, 8, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("Jun 8 after a week: %s, want %s", got, want)
	}
}

// strip: true removes only the matched timestamp, so a timestamp in the
// middle of the line does not take the prefix with it.
func TestStripAnywhereKeepsPrefix(t *testing.T) {
	s, err := Parse([]byte(`
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: '2006-01-02T15:04:05Z07:00', regex: '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z'}]
      anchor: anywhere
      strip: true
    patterns:
      - set: s
        search: 'app['
        extract: ['app\[(?P<pid>\d+)\]: .*n=(?P<n>\d+)']
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	st, err := s.NewStream(s.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	r, err := st.Process("web1 app[42]: 2024-01-01T00:00:00Z n=5")
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(r) != 1 {
		t.Fatalf("got %d samples, unmatched %q", len(r), st.Stats.FirstUnmatched)
	}
	if r[0].Fields["pid"].String() != "42" || r[0].Fields["n"].String() != "5" {
		t.Fatalf("fields: %+v", r[0].Fields)
	}
}

// continue_regex is the only test a continuation line has to pass, so a
// stack frame with no timestamp of its own still joins.
func TestMultilineContinuationNeedsNoTimestamp(t *testing.T) {
	s, err := Parse([]byte(`
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
      strip: true
    framing:
      multiline:
        - start_contains: 'Exception'
          continue_regex: '^\s+at '
          join: [{regex: '^\s+at (\S+)', capture: 1}]
    patterns:
      - set: s
        search: 'Exception'
        extract: ['(?P<cls>\w+Exception)(?P<frames>.*)']
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	st, err := s.NewStream(s.Profiles[0], StreamOptions{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	for _, l := range []string{
		"1700000000000 java.lang.IllegalStateException",
		"\tat com.foo.Bar(Bar.java:1)",
		"\tat com.foo.Baz(Bar.java:2)",
	} {
		if _, err := st.Process(l); err != nil {
			t.Fatalf("process %q: %v", l, err)
		}
	}
	r, errs := st.Flush()
	if len(errs) != 0 {
		t.Fatalf("flush errors: %v", errs)
	}
	if len(r) != 1 {
		t.Fatalf("got %d samples", len(r))
	}
	frames := r[0].Fields["frames"].String()
	if !strings.Contains(frames, "com.foo.Bar") || !strings.Contains(frames, "com.foo.Baz") {
		t.Fatalf("joined frames %q, want both stack frames", frames)
	}
	if st.Stats.TSParseErrors != 0 {
		t.Fatalf("continuation lines counted as timestamp failures: %d", st.Stats.TSParseErrors)
	}
}
