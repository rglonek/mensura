package ingest

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// --batch-bytes bounds the whole request body, and the declarations
// travel in it beside the samples.
//
// A spec declaring a wide bucket set produces one FieldMeta per bucket
// column -- up to the 4096 a bucket set may hold -- and they all go out
// with the first write, on top of a full batch. Bounding only the batches
// let that body pass the store's max_request_bytes, which answers 413;
// wire.Client classifies that as fatal, so the sink drops the batch,
// reports it to the delivery observers as a hole, and every followed
// file's checkpoint freezes behind it.
//
// The declarations themselves are the one thing the budget cannot shrink:
// they are applied before any batch, so when they alone exceed it the
// honest answer is to send them with as little else as possible, which is
// what TestOversizeDeclarationsStillMakeProgress covers.
func TestBatchBytesCountsTheDeclarations(t *testing.T) {
	const budget = 32 << 10
	s := &Sink{
		cfg:      SinkConfig{BatchSize: 1 << 30, BatchBytes: budget},
		buffers:  map[string][]model.Sample{},
		metaSent: map[string]struct{}{},
	}
	for i := 0; i < 200; i++ {
		s.metaQ = append(s.metaQ, wire.FieldMeta{
			Set: "app", Field: fmt.Sprintf("bucket_%03d", i), Kind: model.KindGauge,
			Unit: "milliseconds", BucketSet: "hdr", BucketIndex: i, BucketEdge: float64(i),
		})
	}
	s.setQ = append(s.setQ, wire.SetMeta{Set: "app", KeyScheme: model.KeyOffset})
	for i := 0; i < 200; i++ {
		s.buffers["app"] = append(s.buffers["app"], model.Sample{
			TSMs:   1_700_000_000_000 + int64(i),
			Labels: map[string]string{"host": "h1", "source": "app.log"},
			Fields: map[string]model.Value{"v": model.Int(int64(i))},
		})
		s.pending++
	}

	// The shape flushRound uses: the declarations are taken first and
	// charged, and what is left is the samples' budget.
	meta, sets := s.metaQ, s.setQ
	s.metaQ, s.setQ = nil, nil
	batches, taken, partial := s.takeLocked(s.sampleBudget(meta, sets))
	if taken == 0 {
		t.Fatal("no samples were taken; the flush would never make progress")
	}
	if !partial {
		t.Fatal("the whole buffer fitted, so this test no longer exercises the bound")
	}
	body, err := json.Marshal(&wire.WriteRequest{FieldMeta: meta, SetMeta: sets, Batches: batches})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > budget {
		t.Fatalf("request body is %d bytes, past the %d-byte BatchBytes", len(body), budget)
	}
}

// Declarations bigger than the whole budget still have to leave, and the
// flush still has to make progress: one sample per request until they
// have gone, rather than a take with no bound at all.
func TestOversizeDeclarationsStillMakeProgress(t *testing.T) {
	s := &Sink{
		cfg:      SinkConfig{BatchSize: 1 << 30, BatchBytes: 128},
		buffers:  map[string][]model.Sample{},
		metaSent: map[string]struct{}{},
	}
	for i := 0; i < 50; i++ {
		s.metaQ = append(s.metaQ, wire.FieldMeta{Set: "app", Field: fmt.Sprintf("f%02d", i)})
	}
	for i := 0; i < 5; i++ {
		s.buffers["app"] = append(s.buffers["app"], model.Sample{
			TSMs: 1_700_000_000_000, Fields: map[string]model.Value{"v": model.Int(1)},
		})
		s.pending++
	}
	meta, sets := s.metaQ, s.setQ
	s.metaQ, s.setQ = nil, nil
	_, taken, partial := s.takeLocked(s.sampleBudget(meta, sets))
	if taken != 1 {
		t.Fatalf("took %d samples; a budget the declarations have exhausted must still take exactly one", taken)
	}
	if !partial {
		t.Fatal("the take should have left samples behind")
	}
}

// With BatchBytes switched off the declarations bound nothing either.
func TestNoBatchBytesTakesEverything(t *testing.T) {
	s := &Sink{
		cfg:      SinkConfig{BatchSize: 1 << 30, BatchBytes: 0},
		buffers:  map[string][]model.Sample{},
		metaSent: map[string]struct{}{},
	}
	s.metaQ = append(s.metaQ, wire.FieldMeta{Set: "app", Field: "v"})
	for i := 0; i < 10; i++ {
		s.buffers["app"] = append(s.buffers["app"], model.Sample{
			TSMs: 1_700_000_000_000, Fields: map[string]model.Value{"v": model.Int(1)},
		})
		s.pending++
	}
	meta, sets := s.metaQ, s.setQ
	s.metaQ, s.setQ = nil, nil
	if got := s.sampleBudget(meta, sets); got != 0 {
		t.Fatalf("sampleBudget with BatchBytes 0 = %d, want 0 (no bound)", got)
	}
	_, taken, partial := s.takeLocked(s.sampleBudget(meta, sets))
	if taken != 10 || partial {
		t.Fatalf("took %d (partial %v), want the whole buffer", taken, partial)
	}
}
