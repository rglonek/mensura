package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// oversizeLinesBody is a body one byte past maxHTTPBodyBytes, framed as a
// handful of long records rather than millions of short ones: the test is
// about the byte count, and a million records through the extractor is a
// minute and a half of it.
func oversizeLinesBody() string {
	const line = 900 << 10
	var b strings.Builder
	for b.Len() <= maxHTTPBodyBytes {
		b.WriteString(strings.Repeat("x", line))
		b.WriteByte('\n')
	}
	return b.String()
}

// An oversize body is refused before any of it is applied, where the
// sender declared its length.
//
// This endpoint is a stream: records reach the sink as they are framed,
// so the running byte count can only notice the overflow after part of
// the body has already been delivered. The sender is then told its
// request failed while some of its lines really were stored -- and
// because a received record's key hint is the listener's own arrival
// sequence rather than a byte offset, re-sending the body writes those
// lines a second time under `key: offset`. Content-Length is what almost
// every sender supplies, so deciding on it turns the common case into a
// refusal that applies nothing at all.
func TestAnOversizeLinesBodyIsRefusedBeforeItIsApplied(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	addr, ing, sink := linesReceiver(t, rs.srv.URL, 1)
	defer func() { _ = sink.Close(context.Background()) }()

	body := oversizeLinesBody()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/ingest/v1/lines", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", resp.StatusCode)
	}
	if got := ing.Progress().Snapshot().Records; got != 0 {
		t.Fatalf("%d record(s) of a refused body were handed to the extractor; a resend would store them twice", got)
	}
	if got := len(rs.counts()); got != 0 {
		t.Fatalf("%d record(s) of a refused body reached the store", got)
	}
}

// A body that overran the cap without declaring a length still reports
// what it applied, so a sender can tell a partial application from a
// clean refusal.
func TestALengthlessOversizeBodyReportsWhatItApplied(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	addr, _, sink := linesReceiver(t, rs.srv.URL, 1)
	defer func() { _ = sink.Close(context.Background()) }()

	// A chunked request: Content-Length is unset, so only the running
	// count can notice.
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/ingest/v1/lines", strings.NewReader(oversizeLinesBody()))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.ContentLength = -1
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("the refusal is not readable JSON: %v", err)
	}
	if _, ok := out["accepted"]; !ok {
		t.Errorf("the refusal does not say how much of the body was applied: %v", out)
	}
	if _, ok := out["error"]; !ok {
		t.Errorf("the refusal carries no reason: %v", out)
	}
}
