package store

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// An idempotency key must only be spent by a write that committed. A retry
// after a failed write has to write, not be answered "duplicate", or the
// batch is lost with nothing counting it.
func TestIdempotencyKeySurvivesAFailedWrite(t *testing.T) {
	s := openTestStore(t)
	req := &wire.WriteRequest{Batches: []model.Batch{{Set: "http", Samples: []model.Sample{{
		TSMs:   base(),
		Fields: map[string]model.Value{"v": model.Int(1)},
	}}}}}

	// Close the engine underneath so the commit fails.
	_ = s.db.Close()
	if _, err := s.Write(req, "key-1", "test"); err == nil {
		t.Fatal("expected the write to fail once the engine was closed")
	}
	resp, err := s.Write(req, "key-1", "test")
	if err == nil {
		t.Fatalf("expected the retry to fail too, got %+v", resp)
	}
	if resp != nil && resp.Duplicate {
		t.Fatal("the retry of a failed write was answered Duplicate: the batch would be silently lost")
	}
}

// A successful write does spend its key, so a genuine duplicate is still
// recognised.
func TestIdempotentWriteStillDeduplicates(t *testing.T) {
	s := openTestStore(t)
	req := &wire.WriteRequest{Batches: []model.Batch{{Set: "http", Samples: []model.Sample{{
		TSMs:   base(),
		Fields: map[string]model.Value{"v": model.Int(1)},
	}}}}}
	if _, err := s.Write(req, "key-1", "test"); err != nil {
		t.Fatalf("first write: %v", err)
	}
	resp, err := s.Write(req, "key-1", "test")
	if err != nil {
		t.Fatalf("second write: %v", err)
	}
	if !resp.Duplicate {
		t.Fatal("a replayed request should be reported as a duplicate")
	}
}

// A label named "timestamp" would overwrite the indexed column and hide the
// row from every range scan. It must be rejected, loudly.
func TestTimestampLabelIsRejected(t *testing.T) {
	s := openTestStore(t)
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "http", Samples: []model.Sample{{
		TSMs:   base(),
		Labels: map[string]string{"timestamp": "weird"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}}}}}, "", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(resp.Rejected) != 1 {
		t.Fatalf("expected the sample to be rejected, got %+v", resp)
	}
	if resp.Accepted != 0 {
		t.Fatalf("a rejected sample must not be counted as accepted: %+v", resp)
	}
}

// LIMIT SERIES and LIMIT POINTS are part of the query contract, so they
// have to bind at execution, not only at validation.
func TestLimitSeriesIsEnforced(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	var samples []model.Sample
	for _, h := range []string{"a", "b", "c", "d", "e"} {
		samples = append(samples, model.Sample{
			TSMs: t0, Labels: map[string]string{"host": h},
			Fields: map[string]model.Value{"v": model.Int(1)},
		})
	}
	writeSamples(t, s, "http", samples)

	resp := query(t, s, `FROM http SELECT v BY host LIMIT SERIES 2`, t0-60000, t0+60000)
	if len(resp.Series) > 2 {
		t.Fatalf("LIMIT SERIES 2 returned %d series", len(resp.Series))
	}
	if !resp.Stats.Truncated || resp.Error == "" {
		t.Fatalf("hitting the limit must be reported, not silent: %+v", resp.Stats)
	}
}

// bucket_index arrives from an unauthenticated client and is used as an
// allocation size, so an absurd one must be refused rather than allocated.
func TestHostileBucketIndexIsBounded(t *testing.T) {
	s := openTestStore(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.applyFieldMeta([]wire.FieldMeta{{
			Set: "http", Field: "b", BucketSet: "hist", BucketIndex: 1 << 40,
		}})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("applyFieldMeta is still allocating: bucket_index is unbounded")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if bs, ok := s.catalogue["http"]; ok {
		if info, ok := bs.BucketSets["hist"]; ok && len(info.Buckets) > maxBucketIndex+1 {
			t.Fatalf("bucket set grew to %d entries", len(info.Buckets))
		}
	}
}

// Reading the catalogue while a write widens it must not be a concurrent
// map access. Run under -race for this to mean anything.
func TestConcurrentCatalogueReadAndWrite(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	writeSamples(t, s, "http", []model.Sample{{
		TSMs: t0, Fields: map[string]model.Value{"v": model.Int(1)},
	}})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			s.observeSet("http", []model.Sample{{
				TSMs:   t0,
				Fields: map[string]model.Value{fmt.Sprintf("f%d", i%512): model.Int(1)},
			}})
		}
	}()
	for i := 0; i < 500; i++ {
		if _, err := s.queryFields("http"); err != nil {
			t.Fatalf("queryFields: %v", err)
		}
		_ = s.Catalogue()
	}
	close(stop)
	wg.Wait()
}

// A shard width that is neither a whole number of days nor an hour count
// dividing a day is rounded, but the shard still has to report the width it
// was actually written with, or retention and range overlap are both wrong.
func TestShardSuffixCarriesItsWidth(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.Retention = 30 * 24 * time.Hour
	cfg.Shard = 6 * time.Hour
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	t0 := time.Date(2026, 8, 28, 13, 30, 0, 0, time.UTC)
	name := s.shardName("http", t0.UnixMilli())
	if name != "http@2026082812-6h" {
		t.Fatalf("shard name %q: expected the 12:00 bucket of a 6h layout", name)
	}
	start, width, err := parseShardSuffix("2026082812-6h")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if width != 6*time.Hour {
		t.Fatalf("width %s: the suffix must carry the width it was written with", width)
	}
	if !start.Equal(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("start %s", start)
	}

	// A sample written into that shard must still be found by a query
	// whose range starts after the shard's first hour.
	writeSamples(t, s, "http", []model.Sample{{
		TSMs: t0.UnixMilli(), Fields: map[string]model.Value{"v": model.Int(1)},
	}})
	shards := s.shardsFor("http", t0.Add(-time.Minute).UnixMilli(), t0.Add(time.Minute).UnixMilli())
	if len(shards) != 1 {
		t.Fatalf("expected the 6h shard to overlap the range, got %v", shards)
	}
}

// A per-set retention with no global default still has to be swept.
func TestPerSetRetentionSweepsWithoutAGlobalDefault(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.Retention = 0
	cfg.SetRetention = map[string]time.Duration{"http": 24 * time.Hour}
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if !s.hasAnyRetention() {
		t.Fatal("a per-set retention must count as retention worth sweeping")
	}
	old := time.Now().Add(-72 * time.Hour)
	writeSamples(t, s, "http", []model.Sample{{
		TSMs: old.UnixMilli(), Fields: map[string]model.Value{"v": model.Int(1)},
	}})
	n, err := s.RunRetention(time.Now())
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if n == 0 {
		t.Fatal("the aged-out shard was not dropped")
	}
}

// The spec's `sets:` block has to reach the store, or key: offset and
// per-set retention are configuration that does nothing.
func TestSetMetaIsApplied(t *testing.T) {
	s := openTestStore(t)
	retention := int64((7 * 24 * time.Hour) / time.Millisecond)
	shard := int64(time.Hour / time.Millisecond)
	if _, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{
		Set: "http", RetentionMs: &retention, ShardMs: &shard, KeyScheme: model.KeyOffset,
	}}}, "", "test"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := s.retentionFor("http"); got != 7*24*time.Hour {
		t.Fatalf("retention %s", got)
	}
	if got := s.shardWidth("http"); got != time.Hour {
		t.Fatalf("shard width %s", got)
	}
	if got := s.keyScheme("http"); got != model.KeyOffset {
		t.Fatalf("key scheme %q", got)
	}

	// Offset keying must keep two byte-identical records distinct.
	t0 := base()
	writeSamples(t, s, "http", []model.Sample{
		{TSMs: t0, Fields: map[string]model.Value{"v": model.Int(1)}, KeyHint: "a:1"},
		{TSMs: t0, Fields: map[string]model.Value{"v": model.Int(1)}, KeyHint: "a:2"},
	})
	resp := query(t, s, `FROM http SELECT v FORMAT table`, t0-1000, t0+1000)
	if len(resp.Rows) != 2 {
		t.Fatalf("offset keying should keep both occurrences, got %d row(s)", len(resp.Rows))
	}
}

// The debug surface carries no credential of its own, so it must refuse a
// non-loopback peer rather than trust the listener's binding.
func TestDebugHandlerRefusesNonLoopback(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{})
	h := api.DebugHandler()

	for _, tc := range []struct {
		addr string
		want int
	}{
		{"127.0.0.1:5000", 400}, // reaches the handler; body is empty, so 400
		{"10.1.2.3:5000", 403},
	} {
		rec := newRecorder()
		req := newRequest("POST", "/v1/debug/plan", tc.addr)
		h.ServeHTTP(rec, req)
		if rec.code != tc.want {
			t.Errorf("peer %s: got %d, want %d", tc.addr, rec.code, tc.want)
		}
	}
}

func TestQueryContextCancellationIsNotAClientError(t *testing.T) {
	s := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Query(ctx, &wire.QueryRequest{AST: mustParse(t, `FROM http SELECT v`)}); err == nil {
		t.Fatal("expected a cancelled query to report an error")
	}
}

// ---------- small helpers ----------

type recorder struct {
	code   int
	header http.Header
	body   bytes.Buffer
}

func newRecorder() *recorder { return &recorder{code: 200, header: http.Header{}} }

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *recorder) WriteHeader(code int)        { r.code = code }

func newRequest(method, path, remote string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(""))
	req.RemoteAddr = remote
	return req
}

func mustParse(t *testing.T, text string) *mql.Query {
	t.Helper()
	q, err := mql.Parse(text)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return q
}
