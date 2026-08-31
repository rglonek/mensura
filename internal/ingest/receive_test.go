package ingest

import (
	"testing"
	"time"
)

func TestParseLineProtocol(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	set, s, err := ParseLineProtocol(`http host=web1,dc=eu1 requests_total=91823,latency=1.5,note="a, b" 1756400000000`, now)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if set != "http" {
		t.Fatalf("set: %q", set)
	}
	if s.TSMs != 1756400000000 {
		t.Fatalf("timestamp: %d", s.TSMs)
	}
	if s.Labels["host"] != "web1" || s.Labels["dc"] != "eu1" {
		t.Fatalf("labels: %+v", s.Labels)
	}
	if v := s.Fields["requests_total"]; v.I != 91823 {
		t.Fatalf("integer field: %+v", v)
	}
	if v := s.Fields["latency"]; v.F != 1.5 {
		t.Fatalf("float field: %+v", v)
	}
	// A quoted value keeps its comma: the separator only splits outside
	// quotes.
	if v := s.Fields["note"]; v.S != "a, b" {
		t.Fatalf("quoted field: %+v", v)
	}
}

func TestParseLineProtocolStampsArrivalTime(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	_, s, err := ParseLineProtocol(`http host=web1 v=1`, now)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.TSMs != now.UnixMilli() {
		t.Fatalf("expected arrival time, got %d", s.TSMs)
	}
	// The substitution is marked, so a dashboard can tell the difference
	// between a source timestamp and an arrival timestamp.
	if s.Labels["ts_source"] != "receiver" {
		t.Fatalf("expected ts_source=receiver, got %+v", s.Labels)
	}
}

func TestParseLineProtocolRejectsMalformed(t *testing.T) {
	now := time.Now()
	for _, line := range []string{
		``,
		`http`,
		`http host=web1`,
		`http host=web1 novalue`,
		`_mensura_catalogue host=a v=1`,
		`http host=web1 v=1 notatimestamp`,
	} {
		if _, _, err := ParseLineProtocol(line, now); err == nil {
			t.Fatalf("expected %q to be rejected", line)
		}
	}
}

func TestParseLineProtocolEscaping(t *testing.T) {
	now := time.Now()
	_, s, err := ParseLineProtocol(`http path=/a\,b\=c v=1`, now)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.Labels["path"] != "/a,b=c" {
		t.Fatalf("escaping: %+v", s.Labels)
	}
}

// The label section is positional, so a sample carrying no labels of its
// own needs a way to say so: an empty section split into one malformed
// pair and the whole line was refused.
func TestParseLineProtocolAcceptsNoLabels(t *testing.T) {
	for _, line := range []string{"cpu - used=3 1756382400000", "cpu  used=3 1756382400000"} {
		set, s, err := ParseLineProtocol(line, time.Now())
		if err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		if set != "cpu" || len(s.Labels) != 0 {
			t.Fatalf("%q: set %q labels %v", line, set, s.Labels)
		}
		if v, ok := s.Fields["used"]; !ok || v.I != 3 {
			t.Fatalf("%q: fields %v", line, s.Fields)
		}
	}
}
