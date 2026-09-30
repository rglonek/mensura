package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// A body the store found too large is not a verdict on the samples inside
// it: the same samples in a smaller request are a request the store would
// take.
//
// wire.Client classifies 413 as fatal, which is right for the body and
// wrong for the batch: the sink dropped it, counted it, and reported it to
// the delivery observers as a hole -- which freezes every followed file's
// checkpoint. Nothing in the protocol lets an ingester discover the
// store's max_request_bytes, so an operator whose --batch-bytes sat above
// it lost data at full rate with no way to find out except by reading both
// configurations side by side.
func TestAnOversizeBodyShrinksTheBudgetInsteadOfLosingTheBatch(t *testing.T) {
	const limit = 1 << 16 // what this store will accept
	var (
		mu       sync.Mutex
		refusals int
		seen     = map[int64]int{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAllLimited(r)
		if len(body) > limit {
			mu.Lock()
			refusals++
			mu.Unlock()
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			_ = json.NewEncoder(w).Encode(wire.APIError{Error: "request body is larger than the configured limit"})
			return
		}
		var req wire.WriteRequest
		if err := json.Unmarshal(body, &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n := 0
		mu.Lock()
		for _, b := range req.Batches {
			for _, sm := range b.Samples {
				if v, ok := sm.Fields["n"]; ok {
					iv, _ := v.AsInt()
					seen[iv]++
					n++
				}
			}
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(wire.WriteResponse{Accepted: n})
	}))
	defer srv.Close()

	client := wire.NewClient(srv.URL, "")
	client.Compress = false
	client.MaxRetries = 0
	cfg := DefaultSinkConfig()
	cfg.BatchSize = 1 << 20    // flushing is driven by the test
	cfg.BatchBytes = 4 << 20   // far above what the store will take
	cfg.FlushEvery = time.Hour //
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()

	var obsMu sync.Mutex
	began, ended, drops := 0, 0, 0
	sink.Observe(countingObserver{mu: &obsMu, began: &began, ended: &ended, drops: &drops})

	const samples = 400
	for i := 1; i <= samples; i++ {
		if err := sink.AddSample(context.Background(), "lines", model.Sample{
			TSMs:   int64(1756382400000 + i),
			Fields: map[string]model.Value{"n": model.Int(int64(i)), "pad": model.String(strings.Repeat("p", 500))},
		}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	// Flush until the buffer drains or it stops making progress: each 413
	// halves the budget, so this converges rather than looping.
	for i := 0; i < 64 && sink.buffered() > 0; i++ {
		_ = sink.Flush(context.Background())
	}

	mu.Lock()
	gotRefusals, delivered := refusals, len(seen)
	mu.Unlock()
	if gotRefusals == 0 {
		t.Fatal("the store never refused a body, so this test is watching nothing")
	}
	if delivered != samples {
		t.Errorf("%d of %d samples reached the store after %d refusal(s); a 413 must not cost the batch",
			delivered, samples, gotRefusals)
	}
	if got := sink.Snapshot().FatalDrop; got != 0 {
		t.Errorf("%d batch(es) were dropped as unretryable", got)
	}
	obsMu.Lock()
	holes := drops
	obsMu.Unlock()
	if holes != 0 {
		t.Errorf("%d delivery hole(s) were reported; a hole freezes every followed file's checkpoint", holes)
	}
	if got := int(sink.batchBytes.Load()); got >= cfg.BatchBytes {
		t.Errorf("the body budget is still %d bytes; it should have been shrunk below %d", got, cfg.BatchBytes)
	}
}

// readAllLimited reads a write request body, honouring gzip the way the
// store does. This test's client sends plain bodies.
func readAllLimited(r *http.Request) ([]byte, error) {
	var sb strings.Builder
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			return []byte(sb.String()), nil
		}
	}
}

// Halving has a floor, so a store whose limit is smaller than a single
// record can be does not make this loop for ever with every checkpoint
// frozen behind it. Past the floor a 413 is the unretryable refusal it
// always was: the batch is dropped, counted and reported.
func TestShrinkingTheBodyBudgetStopsAtTheFloor(t *testing.T) {
	var refusals int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		refusals++
		mu.Unlock()
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_ = json.NewEncoder(w).Encode(wire.APIError{Error: "always too large"})
	}))
	defer srv.Close()

	client := wire.NewClient(srv.URL, "")
	client.Compress = false
	client.MaxRetries = 0
	cfg := DefaultSinkConfig()
	cfg.BatchSize = 1 << 20
	cfg.BatchBytes = minBatchBytes * 4
	cfg.FlushEvery = time.Hour
	sink := NewSink(client, cfg, testLogger{t})
	defer func() { _ = sink.Close(context.Background()) }()

	if err := sink.AddSample(context.Background(), "lines", model.Sample{
		TSMs: 1756382400000, Fields: map[string]model.Value{"n": model.Int(1)},
	}); err != nil {
		t.Fatalf("add: %v", err)
	}
	for i := 0; i < 32 && sink.buffered() > 0; i++ {
		_ = sink.Flush(context.Background())
	}
	if sink.buffered() != 0 {
		t.Fatalf("%d sample(s) are still buffered: halving never bottomed out", sink.buffered())
	}
	if got := sink.Snapshot().FatalDrop; got != 1 {
		t.Errorf("fatal drops = %d, want 1: past the floor a 413 is unretryable", got)
	}
	if got := int(sink.batchBytes.Load()); got != minBatchBytes {
		t.Errorf("the body budget is %d bytes, want the %d-byte floor", got, minBatchBytes)
	}
	mu.Lock()
	n := refusals
	mu.Unlock()
	// Two halvings to reach the floor, then one refusal that is fatal.
	if n > 8 {
		t.Errorf("%d requests were spent converging on the floor", n)
	}
}
