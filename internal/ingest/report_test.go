package ingest

import (
	"context"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
)

// The counters that say a record was truncated, skipped as binary or
// refused by the extractor reach the ingest set, not only the progress
// file.
//
// 02-ingest.md section 10 lists extraction errors among the fields the
// `_mensura_ingest` set carries, and `oversize_records` exists precisely
// so a truncation is not indistinguishable from data that was never
// there -- which it is when the only place it appears is a progress file
// nobody has to ask for.
func TestProgressReportCarriesTheLossCounters(t *testing.T) {
	rs := newRecordingStore()
	defer rs.srv.Close()
	_, sink := newFollowIngest(t, rs, "")
	defer func() { _ = sink.Close(context.Background()) }()

	p := NewProgress()
	p.ExtractError()
	p.OversizeRecord()
	p.OversizeRecord()
	p.SkipBinary()

	if err := p.Report(context.Background(), sink, "c1", nil); err != nil {
		t.Fatalf("report: %v", err)
	}
	// Read out of the sink's own buffer: delivery is not what is under
	// test, the shape of the sample is.
	var got model.Sample
	sink.mu.Lock()
	for _, b := range sink.buffers[model.IngestSet] {
		got = b
	}
	sink.mu.Unlock()
	if got.Fields == nil {
		t.Fatal("no progress sample was buffered")
	}
	for name, want := range map[string]int64{
		"extract_errors":   1,
		"oversize_records": 2,
		"binary_skipped":   1,
	} {
		v, ok := got.Fields[name]
		if !ok {
			t.Errorf("the ingest set carries no %q, so the loss it counts is only in the progress file", name)
			continue
		}
		if n, _ := v.AsInt(); n != want {
			t.Errorf("%s = %d, want %d", name, n, want)
		}
	}
}
