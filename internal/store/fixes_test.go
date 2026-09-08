package store

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/internal/engine"
	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// A bucket index outside the accepted range is named, not skipped.
//
// It used to `continue`, which jumped over the change detection at the
// bottom of applyFieldMeta's loop -- so the kind, unit and limits carried
// in the same declaration were written into the catalogue with
// CatalogueVersion standing still, and every client holding the old ETag
// was answered 304 for a catalogue that had moved.
func TestFieldMetaRefusesAnOutOfRangeBucketIndex(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{{
		Set: "app", Field: "b0", Kind: model.KindGauge,
		BucketSet: "latency", BucketIndex: maxBucketIndex + 1,
	}}}, "", "test")
	if err == nil {
		t.Fatal("an out-of-range bucket index was accepted")
	}
	var bad *ErrBadRequest
	if !errors.As(err, &bad) {
		t.Fatalf("expected a client fault, got %T: %v", err, err)
	}
	// A client fault is a 400, so the sink drops it once instead of
	// retrying it at full rate forever.
	if s.CatalogueVersion() != 0 {
		t.Fatalf("the refused declaration still moved the catalogue version to %d", s.CatalogueVersion())
	}
}

// Metadata that changes something bumps the version; metadata that
// repeats itself does not. The bucket-set arm used to be able to swallow
// the first of those.
func TestFieldMetaVersionTracksRealChanges(t *testing.T) {
	s := openTestStore(t)
	declare := func(kind model.Kind) {
		t.Helper()
		if _, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{{
			Set: "app", Field: "b0", Kind: kind,
			BucketSet: "latency", BucketIndex: 0, BucketEdge: 0,
		}}}, "", "test"); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	declare(model.KindGauge)
	first := s.CatalogueVersion()
	if first == 0 {
		t.Fatal("a new set and field did not move the catalogue version")
	}
	declare(model.KindGauge)
	if s.CatalogueVersion() != first {
		t.Fatalf("repeating a declaration moved the version from %d to %d", first, s.CatalogueVersion())
	}
	declare(model.KindCounter)
	if s.CatalogueVersion() == first {
		t.Fatal("a kind change alongside a bucket declaration did not move the version")
	}
}

// The ingest-progress set is the one reserved name a client may write, so
// a spec may declare its retention. applySetMeta refused every reserved
// name, which made the one set every ingester produces also the one set no
// spec could age out.
func TestSetMetaAcceptsTheIngestSet(t *testing.T) {
	s := openTestStore(t)
	ms := int64(3_600_000)
	if _, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{
		Set: model.IngestSet, RetentionMs: &ms, ShardMs: &ms,
	}}}, "", "test"); err != nil {
		t.Fatalf("declaring retention for the ingest set: %v", err)
	}
	if got := s.retentionFor(model.IngestSet); got.Milliseconds() != ms {
		t.Fatalf("retention for %s is %s, want %dms", model.IngestSet, got, ms)
	}
	// Every other reserved name is still refused.
	if _, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{
		Set: model.CatalogueSet, RetentionMs: &ms,
	}}}, "", "test"); err == nil {
		t.Fatalf("a reserved set other than %s was accepted", model.IngestSet)
	}
}

// shardsInRange is shardsFor over a list the caller already holds, so the
// LABELS filter scan can enumerate every set's shards from one pass rather
// than walking the whole store once per set. The two must agree.
func TestShardsInRangeMatchesShardsFor(t *testing.T) {
	s := openTestStore(t)
	base := int64(1756382400000)
	for i := 0; i < 3; i++ {
		writeSamples(t, s, "app", []model.Sample{{
			TSMs:   base + int64(i)*24*3600*1000,
			Labels: map[string]string{"host": "web1"},
			Fields: map[string]model.Value{"v": model.Int(int64(i))},
		}})
	}
	all := s.db.Sets()
	for _, r := range []struct{ from, to int64 }{
		{math.MinInt64, math.MaxInt64},
		{base, base},
		{base + 24*3600*1000, base + 2*24*3600*1000},
		{base - 10*24*3600*1000, base - 9*24*3600*1000},
	} {
		want := s.shardsFor("app", r.from, r.to)
		got := shardsInRange("app", all, r.from, r.to)
		if len(want) != len(got) {
			t.Fatalf("range %d..%d: shardsFor %v, shardsInRange %v", r.from, r.to, want, got)
		}
		for i := range want {
			if want[i] != got[i] {
				t.Fatalf("range %d..%d: shardsFor %v, shardsInRange %v", r.from, r.to, want, got)
			}
		}
	}
}

// labelIndicesMatching replaces a whole-dictionary copy plus one lock
// acquisition per match. It must resolve the same values, and report the
// key's full size so a regex that matches everything can still be folded.
func TestLabelIndicesMatching(t *testing.T) {
	s := openTestStore(t)
	for _, host := range []string{"web1", "web2", "db1"} {
		if _, err := s.intern("host", host); err != nil {
			t.Fatalf("intern %s: %v", host, err)
		}
	}
	vals, total := s.labelIndicesMatching("host", func(v string) bool { return len(v) > 3 && v[:3] == "web" })
	if total != 3 {
		t.Fatalf("total = %d, want 3", total)
	}
	if len(vals) != 2 {
		t.Fatalf("matched %d values, want 2", len(vals))
	}
	for _, v := range vals {
		idx, _ := v.AsInt()
		got, ok := s.labelValue("host", int32(idx))
		if !ok || got[:3] != "web" {
			t.Fatalf("index %d resolves to %q", idx, got)
		}
	}
	// An unknown key is empty rather than an error.
	if vals, total := s.labelIndicesMatching("nosuch", func(string) bool { return true }); vals != nil || total != 0 {
		t.Fatalf("unknown key: %v %d", vals, total)
	}
}

// A string-typed field carrying the text "NaN" -- or "Inf", or
// "+Infinity", all of which strconv.ParseFloat accepts -- used to coerce
// to a non-finite float, land in wire.Series.Values, and make
// encoding/json fail on the response *after* the 200 header had gone out:
// the panel received a truncated body with no status and no diagnostic.
// A value that cannot be plotted now reads as an absent one.
func TestANonFiniteStringFieldDoesNotPoisonTheResponse(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "app", []model.Sample{
		{TSMs: 1000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.String("NaN")}},
		{TSMs: 2000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.String("Inf")}},
		{TSMs: 3000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.String("7")}},
	})
	q, err := mql.Parse(`FROM app SELECT v BY host`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: 0, ToMs: 10_000, MaxPoints: 100,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if _, err := json.Marshal(resp); err != nil {
		t.Fatalf("the response cannot be encoded, so the panel gets a 200 with a truncated body: %v", err)
	}
	if len(resp.Series) != 1 {
		t.Fatalf("want 1 series, got %d", len(resp.Series))
	}
	for i, v := range resp.Series[0].Values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("point %d is %v; a non-plottable value must read as absent", i, v)
		}
	}
}

// A non-finite float already on disk. The write path refuses one, so
// only a data directory written before that check can hold it -- and the
// read path has to survive it, because runTabular used to discard
// AsFloat's verdict and append whatever it returned, so one such row made
// the whole table response unencodable.
func TestANonFiniteStoredFloatLeavesAnEmptyTableCell(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "app", []model.Sample{
		{TSMs: 1000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Float(1)}},
	})
	// Straight through the engine, the way an older build's row sits on
	// disk: rowFor builds the row, Validate is what would have refused it.
	sm := model.Sample{
		TSMs:   2000,
		Labels: map[string]string{"host": "a"},
		Fields: map[string]model.Value{"v": model.Float(math.NaN())},
	}
	row, err := s.rowFor("app", &sm)
	if err != nil {
		t.Fatalf("rowFor: %v", err)
	}
	if err := s.db.PutBatch(s.shardName("app", sm.TSMs), []engine.Record{
		{Key: model.PrimaryKey("app", &sm, model.KeyContent), Row: row},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	for _, text := range []string{
		`FROM app SELECT v BY host FORMAT table`,
		`FROM app SELECT v BY host`,
	} {
		q, perr := mql.Parse(text)
		if perr != nil {
			t.Fatalf("parse %q: %v", text, perr)
		}
		resp, qerr := s.Query(context.Background(), &wire.QueryRequest{
			AST: q, FromMs: 0, ToMs: 10_000, MaxPoints: 100,
		})
		if qerr != nil {
			t.Fatalf("query %q: %v", text, qerr)
		}
		if _, merr := json.Marshal(resp); merr != nil {
			t.Fatalf("%q: the response cannot be encoded: %v", text, merr)
		}
	}
}

// The catalogue's Stale flag is a function of wall-clock time, so the
// body changes while CatalogueVersion stands still. A client caching on
// the ETag was therefore answered 304 for the rest of the process's life
// and never saw a field go quiet.
func TestTheCatalogueETagMovesWhenAFieldGoesStale(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "app", []model.Sample{
		{TSMs: 1000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
	})
	before := s.CatalogueETag()
	version := s.CatalogueVersion()

	// Age the field past the staleness horizon without touching the
	// schema, which is exactly what a source going quiet does.
	s.mu.Lock()
	s.catalogue["app"].Fields["v"].LastSeenMs = time.Now().Add(-2 * staleAfter).UnixMilli()
	s.mu.Unlock()

	if s.CatalogueVersion() != version {
		t.Fatal("the test moved the schema version; it must not")
	}
	if got := s.CatalogueETag(); got == before {
		t.Fatalf("the ETag is still %s after the field went stale, so a caching client is answered 304 forever", got)
	}
	cat := s.Catalogue()
	if !cat.Sets[0].Fields["v"].Stale {
		t.Fatal("the rendered catalogue does not report the field as stale")
	}
	if got := catalogueETag(cat.Version, staleFieldsIn(cat)); got != s.CatalogueETag() {
		t.Fatalf("the served ETag %s disagrees with the validator %s", got, s.CatalogueETag())
	}
}

// A body that could not be decompressed is malformed, not large. Every
// body failure used to come back 413, which wire.Client classifies as
// fatal -- so the sink dropped the batch, reported it to the delivery
// observers as a hole, and froze every followed file's checkpoint.
func TestAMalformedBodyIsABadRequestRatherThanTooLarge(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{MaxRequestBytes: 1 << 20})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/write", strings.NewReader("this is not gzip"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d for a corrupt gzip body, want 400: 413 makes the client drop the batch as unretryable", resp.StatusCode)
	}
}

// A body that really is too large still says so, so back-pressure and
// "fix your batch size" stay distinguishable.
func TestAnOversizedBodyIsStillTooLarge(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{MaxRequestBytes: 64})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/write", "application/json", strings.NewReader(strings.Repeat("x", 4096)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d for an oversized body, want 413", resp.StatusCode)
	}
}

// A batch the store refuses wholesale must not produce a response body
// the write client cannot read.
//
// Every refused sample used to be named individually and the reasons are
// sentences, so a few thousand of them passed the megabyte the client
// reads: the client got truncated JSON, reported "unexpected end of JSON
// input", and the sink classified that as neither fatal nor an auth
// failure -- so it requeued the batch and retried it forever, with the
// store re-committing whatever it accepted each time and no checkpoint
// ever advancing again.
func TestWriteResponseStaysReadable(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	const n = 8000
	samples := make([]model.Sample, 0, n)
	for i := 0; i < n; i++ {
		samples = append(samples, model.Sample{
			// Past model.MaxTSMs, whose rejection reason is a sentence.
			TSMs:   999999999999999999,
			Fields: map[string]model.Value{"x": model.Int(1)},
		})
	}
	client := wire.NewClient(srv.URL, "")
	client.Compress = false
	resp, err := client.Write(context.Background(), &wire.WriteRequest{
		Batches: []model.Batch{{Set: "s", Samples: samples}},
	})
	if err != nil {
		t.Fatalf("a fully-rejected batch made the write client fail to read the reply: %v", err)
	}
	if resp.Refused() != n {
		t.Fatalf("Refused() = %d, want %d", resp.Refused(), n)
	}
	if len(resp.Rejected) != wire.MaxReportedRejections {
		t.Fatalf("named %d rejections, want the cap of %d", len(resp.Rejected), wire.MaxReportedRejections)
	}
	// The body has to stay small enough that no reader has to guess.
	direct, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "s", Samples: samples}}}, "", "")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	b, err := json.Marshal(direct)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(b) > 1<<20 {
		t.Fatalf("response body is %d bytes, past the megabyte a client reads", len(b))
	}
	// A response from a store built before the count existed still reports
	// the right number.
	legacy := &wire.WriteResponse{Rejected: []wire.Rejection{{Index: 0, Reason: "old"}}}
	if legacy.Refused() != 1 {
		t.Fatalf("Refused() = %d on a countless response, want 1", legacy.Refused())
	}
}
