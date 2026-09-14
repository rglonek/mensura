package store

import (
	"context"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// The window formula doubles its width because the render walk emits two
// points -- a min and a max -- from each one. A heatmap column is a
// single summed value, so the doubling spent the panel's whole budget on
// half the columns it asked for: 100 max data points drew 50 columns,
// with nothing saying so and no way to recover the resolution short of an
// explicit EVERY.
func TestHeatmapUsesTheUndoubledWindow(t *testing.T) {
	s := openTestStore(t)
	const (
		start   = int64(1_700_000_000_000)
		step    = int64(1_000)
		samples = 200
	)
	edge := 0.0
	meta := []wire.FieldMeta{
		{Set: "lat", Field: "b0", BucketSet: "h", BucketIndex: 0, BucketEdge: edge},
		{Set: "lat", Field: "b1", BucketSet: "h", BucketIndex: 1, BucketEdge: 1},
	}
	rows := make([]model.Sample, 0, samples)
	for i := int64(0); i < samples; i++ {
		rows = append(rows, model.Sample{
			TSMs:   start + i*step,
			Labels: map[string]string{"host": "web1"},
			Fields: map[string]model.Value{"b0": model.Int(1), "b1": model.Int(2)},
		})
	}
	writeSamples(t, s, "lat", rows, meta...)

	q, err := mql.Parse(`FROM lat SELECT HISTOGRAM(h) FORMAT heatmap`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	req := &wire.QueryRequest{
		AST: q, FromMs: start, ToMs: start + samples*step, MaxPoints: 20,
	}
	resp, err := s.Query(context.Background(), req)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Series) != 2 {
		t.Fatalf("%d series, want one per bucket", len(resp.Series))
	}
	// 200 s of samples over a budget of 20 points is a 10 s column, so
	// the range holds 20 of them. The doubled width gave 10.
	if got := len(resp.Series[0].TSMs); got != 20 {
		t.Fatalf("%d column(s), want 20: the heatmap is drawing at half the requested resolution", got)
	}

	// Explain must describe the plan the executor runs.
	plan, err := s.Explain(q, req)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if got := plan["downsample_window"]; got != int64(10_000) {
		t.Fatalf("explain reports window %v, executor used 10000", got)
	}

	// EVERY still wins outright.
	every := int64(50_000)
	q.EveryMs = &every
	resp, err = s.Query(context.Background(), req)
	if err != nil {
		t.Fatalf("query with EVERY: %v", err)
	}
	if got := len(resp.Series[0].TSMs); got != 4 {
		t.Fatalf("%d column(s) under EVERY 50s, want 4", got)
	}
}
