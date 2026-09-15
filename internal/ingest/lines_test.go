package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
	"github.com/rglonek/mensura/pkg/wire"
)

// linesReceiver starts a receive HTTP listener against the given store and
// returns its address.
func linesReceiver(t *testing.T, storeURL string, batch int) (string, *Ingest, *Sink) {
	t.Helper()
	spec, err := extract.Parse([]byte(followSpec))
	if err != nil {
		t.Fatalf("spec: %v", err)
	}
	client := wire.NewClient(storeURL, "")
	client.Compress = false
	// No client-side retries: the test is about what the listener does
	// with a delivery failure, not about how long it waits for one.
	client.MaxRetries = 0
	cfg := DefaultSinkConfig()
	cfg.BatchSize = batch
	cfg.FlushEvery = time.Hour // flushing is driven by the batch size here
	sink := NewSink(client, cfg, testLogger{t})
	ing, err := New(Config{Spec: spec, Sink: sink, Log: testLogger{t}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = ing.Receive(ctx, ReceiveOptions{HTTPAddr: addr, Listener: "test"}) }()
	if !waitFor(t, 5*time.Second, func() bool {
		c, derr := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if derr != nil {
			return false
		}
		_ = c.Close()
		return true
	}) {
		t.Fatal("listener never came up")
	}
	return addr, ing, sink
}

// A body that ends mid-record is not extracted from.
//
// The trailing record of an HTTP body legitimately carries no newline --
// the body ended, so nothing more is coming for it -- but a client that
// died mid-request leaves a *fragment*, and half a line handed to the
// extractor does not fail cleanly: a prefix-anchored pattern matches it
// and invents a sample from a number that was cut in two. The listener
// used to extract that fragment, deliver it, and only then answer 400 --
// so a dropped connection wrote a sample the sender never sent, and the
// sender, told its request had failed, sent the whole body again.
// handleConn and serveUDP have refused a cut-short record for this exact
// reason; this is the third acquisition path.
func TestABodyCutShortIsNotExtractedFrom(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	addr, ing, sink := linesReceiver(t, rs.srv.URL, 1)
	defer func() { _ = sink.Close(context.Background()) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	// One whole record, then a fragment of a second one. The declared
	// length promises more than is sent, so the server reports the body
	// as cut short rather than ended.
	sent := "1756382400000 n=1\n1756382400000 n=99"
	req := fmt.Sprintf("POST /ingest/v1/lines HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s",
		len(sent)+20, sent)
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Half-close so the server sees the body end early rather than
	// waiting for its read timeout.
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Errorf("a body that ended mid-record was answered 200")
		}
	}
	// The fragment must never have reached the extractor at all --
	// extract.Stream is stateful, so feeding it half a line also feeds
	// multiline buffers and aggregation windows that have no way back.
	// Progress counts a record the moment it is handed over.
	if got := ing.Progress().Snapshot().Records; got != 1 {
		t.Fatalf("%d record(s) were handed to the extractor, want 1: the fragment was judged as a record of its own", got)
	}
	if got := ing.Progress().Snapshot().Samples; got != 1 {
		t.Fatalf("%d sample(s) were produced, want 1: a sample was invented from a number cut in two", got)
	}
	if got := rs.counts()[99]; got != 0 {
		t.Fatalf("a record cut in half was extracted and delivered %d time(s): %v", got, rs.counts())
	}
}

// A trailing record with no newline is still a record when the body
// simply ended, which is the case this endpoint is normally used in.
func TestATrailingRecordWithNoNewlineIsStillARecord(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	addr, _, sink := linesReceiver(t, rs.srv.URL, 1)
	defer func() { _ = sink.Close(context.Background()) }()

	resp, err := http.Post("http://"+addr+"/ingest/v1/lines", "text/plain",
		strings.NewReader("1756382400000 n=1\n1756382400000 n=2"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, _ := body["accepted"].(float64); got != 2 {
		t.Fatalf("accepted = %v, want 2 (%v)", body["accepted"], body)
	}
}

// A store that will not take the batch is not a rejection of the record.
//
// Sink.Add buffers the sample and only then flushes, so its error is the
// verdict of that flush and says nothing about the line in hand. Counting
// it as a refusal answered 200 with "refused: 1" and a reason describing
// the store's health, for a record the sink had in fact accepted -- so a
// sender could not tell a spec that does not match its lines from a store
// that is down. 503 with Retry-After is the signal the store itself uses
// for the same condition.
func TestADeliveryFailureIsNotARefusal(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(wire.APIError{Error: "down"})
	}))
	defer down.Close()
	addr, _, sink := linesReceiver(t, down.URL, 1)
	defer func() { _ = sink.Close(context.Background()) }()

	resp, err := http.Post("http://"+addr+"/ingest/v1/lines", "text/plain",
		strings.NewReader("1756382400000 n=1\n"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("a shed request carries no Retry-After, so the sender has to invent an interval")
	}
}
