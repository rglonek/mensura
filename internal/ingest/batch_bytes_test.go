package ingest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// sampleBytes is the only thing standing between a buffered backlog and a
// body the store answers 413 to, and a 413 is fatal: the sink drops the
// whole batch, reports it to the delivery observers as a hole, and every
// followed file's checkpoint freezes behind it. So the estimate has to be
// an upper bound on the encoded size, not an approximation of it.
//
// It used to measure text raw. encoding/json escapes a quote, a
// backslash or a tab to two bytes and a control byte -- or one of '<',
// '>', '&' -- to six, and a `kind: string` field is a whole log line, so
// a 4 KiB message of quotes was charged 4 KiB and encoded to 8 KiB.
func TestSampleBytesIsAnUpperBound(t *testing.T) {
	bodies := []string{
		"",
		"plain text with no escapes at all",
		strings.Repeat(`"`, 512),
		strings.Repeat(`\`, 512),
		strings.Repeat("\n\t\r", 200),
		strings.Repeat("\x00\x01\x1f", 200),
		strings.Repeat("<a href=\"x\">&amp;</a>", 100),
		`GET /a?b=1&c=2 HTTP/1.1" 200 1234 "-" "curl/8.0"`,
	}
	for _, body := range bodies {
		s := model.Sample{
			TSMs:    1_700_000_000_000,
			KeyHint: "stream\x0012345",
			Labels:  map[string]string{"host": body, "source": "app.log"},
			Fields: map[string]model.Value{
				"message": model.String(body),
				"status":  model.Int(200),
				"latency": model.Float(1.5),
			},
		}
		enc, err := json.Marshal(&s)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := sampleBytes(&s), len(enc); got < want {
			t.Errorf("sampleBytes under-counts %q: estimated %d, encodes to %d", trunc(body), got, want)
		}
	}
}

// And the bound has to hold for a whole take, which is what BatchBytes
// actually caps: the request body the write client presents.
func TestTakeStaysUnderBatchBytes(t *testing.T) {
	const budget = 64 << 10
	s := NewSink(nil, SinkConfig{BatchSize: 1 << 20, BatchBytes: budget, MaxBufferedSamples: 1 << 20}, testLogger{t})
	defer func() { s.stopOnce.Do(func() { close(s.stopCh) }) }()
	for i := 0; i < 500; i++ {
		s.buffers["app"] = append(s.buffers["app"], model.Sample{
			TSMs:   1_700_000_000_000 + int64(i),
			Labels: map[string]string{"host": "web1"},
			Fields: map[string]model.Value{"msg": model.String(strings.Repeat(`"`, 300))},
		})
		s.pending++
	}
	s.mu.Lock()
	batches, taken, _ := s.takeLocked(s.cfg.BatchBytes)
	s.mu.Unlock()
	if taken == 0 {
		t.Fatal("took nothing")
	}
	body, err := json.Marshal(&wire.WriteRequest{Batches: batches})
	if err != nil {
		t.Fatal(err)
	}
	// One sample's worth of slack for the request envelope itself, which
	// the per-sample estimate does not model.
	if len(body) > budget+sampleBytes(&batches[0].Samples[0]) {
		t.Fatalf("a take of %d sample(s) encodes to %d bytes, past the %d-byte budget", taken, len(body), budget)
	}
}

func trunc(s string) string {
	if len(s) > 32 {
		return s[:32] + "..."
	}
	return s
}
