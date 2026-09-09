package ingest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
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

// A line-protocol record carries label keys, field names and a timestamp
// that ParseLineProtocol does not look at. Nothing else did either, so a
// sample the store will refuse -- a label key outside the charset, a
// timestamp in the wrong unit -- was reported to an HTTP sender as
// accepted, and passed in silence on TCP and UDP. The only trace of the
// loss was the store's own log on the far side of the sink.
func TestMetricsListenerRefusesASampleTheStoreWould(t *testing.T) {
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	ing := newMetricsIngest(t, spec, time.Time{})
	r := &receiver{ing: ing, opts: ReceiveOptions{Mode: "metrics"}, streams: map[string]*peerStream{}, allowed: map[string]struct{}{}}
	for _, line := range []string{
		// A label key the store's charset refuses.
		`http 1bad=x v=1 1756400000000`,
		// Epoch nanoseconds declared as milliseconds.
		`http host=a v=1 1756400000000000000`,
	} {
		if err := r.handleRecord(context.Background(), "1.2.3.4", line); err == nil {
			t.Fatalf("%q was accepted; the store will refuse it and the sender will never hear", line)
		}
	}
	// A well-formed one still lands.
	if err := r.handleRecord(context.Background(), "1.2.3.4", `http host=a v=1 1756400000000`); err != nil {
		t.Fatalf("a valid line was refused: %v", err)
	}
}

// --from and --to are honoured on every acquisition path that goes
// through the extractor. The line-protocol path does not, so the two
// flags were read, parsed, and then ignored by exactly the mode whose
// records nothing else filters.
func TestMetricsListenerHonoursTheTimeWindow(t *testing.T) {
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	from := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	ing := newMetricsIngest(t, spec, from)
	r := &receiver{ing: ing, opts: ReceiveOptions{Mode: "metrics"}, streams: map[string]*peerStream{}, allowed: map[string]struct{}{}}

	before := fmt.Sprintf("http host=a v=1 %d", from.Add(-time.Hour).UnixMilli())
	unusable, err := r.handleRecordOutcome(context.Background(), "1.2.3.4", before)
	if err != nil {
		t.Fatalf("a record outside the window is not an error: %v", err)
	}
	if !unusable {
		t.Fatal("a record before --from was stored anyway")
	}
	if n := ing.Progress().Snapshot().Samples; n != 0 {
		t.Fatalf("%d sample(s) were counted for a record outside the window", n)
	}

	after := fmt.Sprintf("http host=a v=1 %d", from.Add(time.Hour).UnixMilli())
	if unusable, err := r.handleRecordOutcome(context.Background(), "1.2.3.4", after); err != nil || unusable {
		t.Fatalf("a record inside the window was dropped: unusable=%v err=%v", unusable, err)
	}
}

// newMetricsIngest wires an ingest at a store that accepts everything, so
// the sink's flush loop has somewhere to go and can be closed when the
// test ends rather than logging into a finished one.
func newMetricsIngest(t *testing.T, spec *extract.Spec, from time.Time) *Ingest {
	t.Helper()
	rs := newRecordingStore()
	t.Cleanup(rs.srv.Close)
	client := wire.NewClient(rs.srv.URL, "")
	client.Compress = false
	sink := NewSink(client, DefaultSinkConfig(), testLogger{t})
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}, From: from})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return ing
}
