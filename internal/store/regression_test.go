package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rglonek/mensura/internal/engine"
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

	if s.retentionFor("http") <= 0 {
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

// A spec's set declarations travel once per ingest process. A store that
// only held them in memory forgot them on restart, and the set then
// routed to the unsharded shard that retention skips: declared retention
// stopped applying, silently and permanently.
func TestSetMetaSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.Retention = 0 // the documented shape: global 0, per-set from the spec

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	retention := int64((7 * 24 * time.Hour) / time.Millisecond)
	shard := int64(24 * time.Hour / time.Millisecond)
	if _, err := s.Write(&wire.WriteRequest{
		SetMeta: []wire.SetMeta{{Set: "http", RetentionMs: &retention, ShardMs: &shard}},
		Batches: []model.Batch{{Set: "http", Samples: []model.Sample{
			{TSMs: base(), Fields: map[string]model.Value{"v": model.Int(1)}},
		}}},
	}, "", "test"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if got := s2.retentionFor("http"); got != 7*24*time.Hour {
		t.Fatalf("retention after restart is %s, want 168h", got)
	}
	if got := s2.shardWidth("http"); got != 24*time.Hour {
		t.Fatalf("shard width after restart is %s, want 24h", got)
	}
	// The ingester does not re-declare, so the next write must still be
	// routed to a droppable shard rather than to "@all".
	writeSamples(t, s2, "http", []model.Sample{
		{TSMs: base() + 1000, Fields: map[string]model.Value{"v": model.Int(2)}},
	})
	for _, name := range s2.db.Sets() {
		if strings.HasSuffix(name, "@"+shardAll) {
			t.Fatalf("a write landed in the unsharded shard that retention skips: %v", s2.db.Sets())
		}
	}
}

// Auxiliary query forms used to return before Validate ran, so a LABELS
// predicate was never shape-checked: a node carrying two arms of the
// tagged union executed as whichever arm the lowering reaches first and
// silently dropped the rest, widening the result.
func TestLabelsQueryIsValidated(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	writeSamples(t, s, "http", []model.Sample{
		{TSMs: t0, Labels: map[string]string{"host": "web1", "dc": "eu"}, Fields: map[string]model.Value{"v": model.Int(1)}},
		{TSMs: t0 + 1, Labels: map[string]string{"host": "web2", "dc": "us"}, Fields: map[string]model.Value{"v": model.Int(2)}},
	})
	q := &mql.Query{Kind: mql.KindLabels, Label: "host", Where: mql.Expr{
		And: []mql.Expr{{Has: "v"}},
		Eq:  &mql.Compare{Label: "dc", Value: "eu"},
	}}
	_, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: t0 - 1000, ToMs: t0 + 1000,
	})
	if err == nil {
		t.Fatal("a predicate node with two arms must be refused, not executed with the rest dropped")
	}
	var d mql.Diag
	if !errors.As(err, &d) || d.Code != "E001" {
		t.Fatalf("want E001, got %v", err)
	}
}

// Two retries of one request that arrive at once must not both write.
// seen() and record() are separated by the whole write, so the check has
// to claim the key rather than merely read it.
func TestConcurrentDuplicateWritesCommitOnce(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	req := func() *wire.WriteRequest {
		return &wire.WriteRequest{Batches: []model.Batch{{Set: "http", Samples: []model.Sample{
			{TSMs: t0, Labels: map[string]string{"host": "web1"}, Fields: map[string]model.Value{"v": model.Int(1)}, KeyHint: "a:1"},
		}}}}
	}
	var wg sync.WaitGroup
	dupes := make([]bool, 8)
	for i := range dupes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := s.Write(req(), "same-key", "test")
			if err != nil {
				t.Errorf("write: %v", err)
				return
			}
			dupes[i] = resp.Duplicate
		}(i)
	}
	wg.Wait()
	committed := 0
	for _, d := range dupes {
		if !d {
			committed++
		}
	}
	if committed != 1 {
		t.Fatalf("%d of %d concurrent retries committed, want exactly 1", committed, len(dupes))
	}
}

// A label dictionary is loaded back by the index in each record's name.
// Re-deriving it from load order meant one missing record shifted every
// later value onto someone else's index, silently relabelling stored rows.
func TestDictionaryIndicesSurviveAHole(t *testing.T) {
	s := openTestStore(t)
	for _, v := range []string{"web1", "web2", "web3"} {
		if _, err := s.intern("host", v); err != nil {
			t.Fatalf("intern %s: %v", v, err)
		}
	}
	idx, ok := s.lookup("host", "web3")
	if !ok {
		t.Fatal("web3 was not interned")
	}
	// Drop the middle record, as a lost write would.
	if err := s.db.PutDict(dictEntryKey("host", 1), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	s.dict = map[string]*dictionary{}
	if err := s.loadDictionaries(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got, ok := s.lookup("host", "web3"); !ok || got != idx {
		t.Fatalf("web3 moved from index %d to %d (ok=%v) because of a hole", idx, got, ok)
	}
	for _, v := range s.LabelValues("host") {
		if v == "" {
			t.Fatal("LabelValues reported the empty placeholder left by the hole")
		}
	}
}

// Offset keying derives the row key from the hint instead of the field
// values. A sample with no hint is keyed by set, timestamp and labels
// alone, so two records in one millisecond overwrite each other while the
// response counts both accepted. It must be named, not silently lost.
func TestOffsetKeyedSampleNeedsAHint(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.Write(&wire.WriteRequest{
		SetMeta: []wire.SetMeta{{Set: "http", KeyScheme: model.KeyOffset}},
	}, "", "test"); err != nil {
		t.Fatalf("set meta: %v", err)
	}
	t0 := base()
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "http", Samples: []model.Sample{
		{TSMs: t0, Fields: map[string]model.Value{"v": model.Int(1)}},
		{TSMs: t0, Fields: map[string]model.Value{"v": model.Int(2)}, KeyHint: "a:1"},
	}}}}, "", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.Accepted != 1 || len(resp.Rejected) != 1 {
		t.Fatalf("accepted %d, rejected %+v; the hintless sample must be refused", resp.Accepted, resp.Rejected)
	}
	if !strings.Contains(resp.Rejected[0].Reason, "key_hint") {
		t.Fatalf("rejection does not say why: %q", resp.Rejected[0].Reason)
	}
}

// field_meta used to reach entryLocked with no validation at all, so any
// client holding the write scope could put a reserved name -- or one
// carrying the '@' that separates a set from its shard suffix -- into the
// catalogue. Such an entry was persisted, served from /v1/catalogue,
// accepted by the MQL validator as a real set, and could not be removed
// through DELETE /v1/admin/sets/, which does validate.
func TestFieldMetaCannotForgeASetName(t *testing.T) {
	for _, bad := range []string{"_mensura_catalogue", "not a valid@name", "has spaces", "@"} {
		s := openTestStore(t)
		_, err := s.Write(&wire.WriteRequest{
			FieldMeta: []wire.FieldMeta{{Set: bad, Field: "x"}},
		}, "", "c")
		if err == nil {
			t.Errorf("field_meta for set %q was accepted", bad)
		}
		if got := s.Sets(); len(got) != 0 {
			t.Errorf("field_meta for set %q left %v in the catalogue", bad, got)
		}
	}
	// The one reserved set a client may write is still allowed.
	s := openTestStore(t)
	if _, err := s.Write(&wire.WriteRequest{
		FieldMeta: []wire.FieldMeta{{Set: model.IngestSet, Field: "records"}},
	}, "", "c"); err != nil {
		t.Fatalf("field_meta for the ingest set was refused: %v", err)
	}
	// A field name is checked too: it shares the row's column namespace
	// with the labels.
	if _, err := s.Write(&wire.WriteRequest{
		FieldMeta: []wire.FieldMeta{{Set: "app", Field: "timestamp"}},
	}, "", "c"); err == nil {
		t.Fatal("field_meta naming the reserved timestamp column was accepted")
	}
}

// CatalogueVersion is the version of the schema, and the /v1/catalogue
// ETag is built from it. Re-declaring the same field metadata -- which
// every ingest process does on start -- must not move it, or the ETag
// misses and every client watching the number is woken for nothing.
func TestRedeclaringFieldMetaDoesNotChurnTheCatalogueVersion(t *testing.T) {
	s := openTestStore(t)
	meta := []wire.FieldMeta{{Set: "app", Field: "latency", Kind: model.KindGauge, Unit: "ms"}}
	if _, err := s.Write(&wire.WriteRequest{FieldMeta: meta}, "", "c"); err != nil {
		t.Fatalf("write: %v", err)
	}
	settled := s.CatalogueVersion()
	for i := 0; i < 5; i++ {
		if _, err := s.Write(&wire.WriteRequest{FieldMeta: meta}, "", "c"); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if got := s.CatalogueVersion(); got != settled {
		t.Fatalf("catalogue version moved from %d to %d on unchanged metadata", settled, got)
	}
	// A real change still moves it.
	changed := []wire.FieldMeta{{Set: "app", Field: "latency", Kind: model.KindCounter, Unit: "ms"}}
	if _, err := s.Write(&wire.WriteRequest{FieldMeta: changed}, "", "c"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if s.CatalogueVersion() == settled {
		t.Fatal("catalogue version did not move on a changed kind")
	}
}

// A field carrying NaN cannot be encoded, so it must be refused by name
// rather than accepted and left to break the request it travels in.
func TestNonFiniteFieldValueIsRejectedNotStored(t *testing.T) {
	s := openTestStore(t)
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "app", Samples: []model.Sample{
		{TSMs: 1000, Fields: map[string]model.Value{"latency": model.Float(math.NaN())}},
		{TSMs: 1001, Fields: map[string]model.Value{"latency": model.Float(1.5)}},
	}}}}, "", "c")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.Accepted != 1 {
		t.Fatalf("accepted %d, expected the finite sample only", resp.Accepted)
	}
	if len(resp.Rejected) != 1 || !strings.Contains(resp.Rejected[0].Reason, "latency") {
		t.Fatalf("rejection does not name the field: %+v", resp.Rejected)
	}
}

// Two samples whose label sets differ only in where the framing bytes
// fall must stay two rows. The content key used to hash "k=v\x00" with no
// length prefix, so they collapsed into one and the write API still
// reported both as accepted.
func TestAmbiguousLabelSetsStayDistinctRows(t *testing.T) {
	s := openTestStore(t)
	ts := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "app", Samples: []model.Sample{
		{TSMs: ts, Labels: map[string]string{"a": "b", "c": "d"}, Fields: map[string]model.Value{"n": model.Int(1)}},
		{TSMs: ts, Labels: map[string]string{"a": "b\x00c=d"}, Fields: map[string]model.Value{"n": model.Int(1)}},
	}}}}, "", "c")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.Accepted != 2 {
		t.Fatalf("accepted %d, expected 2", resp.Accepted)
	}
	rows := 0
	p := &queryPlan{shards: s.shardsFor("app", ts-1000, ts+1000), projection: []string{model.TimestampField}}
	if err := s.scan(context.Background(), p, &wire.QueryRequest{FromMs: ts - 1000, ToMs: ts + 1000}, nil, func(engine.Row) bool {
		rows++
		return true
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if rows != 2 {
		t.Fatalf("stored %d row(s) for two distinct samples", rows)
	}
}

// A fault in what the client sent must come back as a 4xx.
//
// wire.Client classifies >= 500 as retryable and the ingest sink drops a
// batch that runs out of retries, so answering 500 to a spec typo -- a
// negative retention, say -- destroyed every batch the ingester produced,
// at full rate, for the life of the process: the poison metadata was
// requeued for the next flush every time.
func TestClientInputFaultsAreNotServerErrors(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{MaxRequestBytes: 1 << 20})
	h := api.Handler()

	neg := int64(-5000)
	unknown := int64(1000)
	for _, tc := range []struct {
		name string
		req  wire.WriteRequest
	}{
		{"negative retention", wire.WriteRequest{SetMeta: []wire.SetMeta{{Set: "app", RetentionMs: &neg}}}},
		{"negative shard", wire.WriteRequest{SetMeta: []wire.SetMeta{{Set: "app", ShardMs: &neg}}}},
		{"unknown key scheme", wire.WriteRequest{SetMeta: []wire.SetMeta{{Set: "app", ShardMs: &unknown, KeyScheme: "sideways"}}}},
		{"reserved set in field meta", wire.WriteRequest{FieldMeta: []wire.FieldMeta{{Set: "_mensura_nope", Field: "v"}}}},
	} {
		body, err := json.Marshal(tc.req)
		if err != nil {
			t.Fatalf("%s: marshal: %v", tc.name, err)
		}
		rec := newRecorder()
		req := httptest.NewRequest("POST", "/v1/write", bytes.NewReader(body))
		h.ServeHTTP(rec, req)
		if rec.code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400 -- a %d is retryable and ends in a dropped batch", tc.name, rec.code, rec.code)
		}
	}
}

// A genuine store failure is still a 500: a 400 would tell the client to
// fix a batch that is not the problem.
func TestEngineFailuresAreStillServerErrors(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{MaxRequestBytes: 1 << 20})
	h := api.Handler()
	_ = s.db.Close()

	body, err := json.Marshal(wire.WriteRequest{Batches: []model.Batch{{Set: "http", Samples: []model.Sample{{
		TSMs: base(), Fields: map[string]model.Value{"v": model.Int(1)},
	}}}}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rec := newRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/write", bytes.NewReader(body)))
	if rec.code != http.StatusInternalServerError {
		t.Fatalf("got %d, want 500", rec.code)
	}
}

// The query listener is the read surface. Mounting the full mux on it meant
// an address published to Grafana also accepted writes and admin calls.
func TestQueryHandlerCarriesNoWriteOrAdminSurface(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{MaxRequestBytes: 1 << 20})
	h := api.QueryHandler()
	for _, path := range []string{"/v1/write", "/v1/admin/compact", "/v1/admin/quiesce", "/v1/admin/sets/http"} {
		rec := newRecorder()
		h.ServeHTTP(rec, newRequest("POST", path, "10.0.0.1:5000"))
		if rec.code != http.StatusNotFound {
			t.Errorf("%s is reachable on the query listener (got %d)", path, rec.code)
		}
	}
	rec := newRecorder()
	h.ServeHTTP(rec, newRequest("GET", "/v1/catalogue", "10.0.0.1:5000"))
	if rec.code != http.StatusOK {
		t.Fatalf("the read surface is missing from the query listener: /v1/catalogue got %d", rec.code)
	}
}

// LABELS <key> WHERE ... is the path a dashboard hits on every variable
// refresh. It scans every set carrying the label across the whole range
// and accumulates into a map, so it needs the same bound a graph has.
func TestLabelsFilterScanIsBounded(t *testing.T) {
	s := openTuned(t, func(c *Config) { c.MaxSeriesPerGraph = 3 })
	now := time.Now().UnixMilli()
	for i := 0; i < 20; i++ {
		put(t, s, "app", model.Sample{
			TSMs:   now + int64(i),
			Labels: map[string]string{"host": fmt.Sprintf("h%02d", i), "dc": "eu"},
			Fields: map[string]model.Value{"v": model.Int(1)},
		})
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: mustParse(t, `LABELS host WHERE dc = "eu"`), FromMs: 0, ToMs: math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if resp.Error == "" || !resp.Stats.Truncated {
		t.Fatalf("the filter scan ran past the ceiling unreported: %d value(s), err=%q", len(resp.Rows), resp.Error)
	}
	if len(resp.Rows) > 3 {
		t.Fatalf("returned %d values with a ceiling of 3", len(resp.Rows))
	}
}

// A truncated table used to set Stats.Truncated and nothing else. The
// plugin renders warnings and the error, so the operator saw the first
// rows of a range with nothing saying there were more.
func TestTabularTruncationIsVisible(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	for i := 0; i < 5; i++ {
		put(t, s, "http", model.Sample{TSMs: t0 + int64(i), Fields: map[string]model.Value{"v": model.Int(int64(i))}})
	}
	resp := query(t, s, `FROM http SELECT v FORMAT table LIMIT POINTS 2`, t0-1000, t0+1000)
	if len(resp.Rows) != 2 || !resp.Stats.Truncated {
		t.Fatalf("expected 2 truncated rows, got %d (truncated=%v)", len(resp.Rows), resp.Stats.Truncated)
	}
	if resp.Error == "" {
		t.Fatal("a truncated table reported no error")
	}
	if !hasWarning(resp.Warnings, "W401") {
		t.Fatalf("a truncated table carried no W401: %+v", resp.Warnings)
	}
}

func hasWarning(ds []mql.Diag, code string) bool {
	for _, d := range ds {
		if d.Code == code {
			return true
		}
	}
	return false
}

// The spec accepts any duration for max_interval, so the wire has to carry
// milliseconds. Whole seconds truncated "500ms" to zero -- gap detection
// silently off, and a query then claiming the field has no declared
// cadence -- and rounded "1500ms" down to a tighter gap than declared.
func TestFieldMetaCarriesSubSecondCadence(t *testing.T) {
	s := openTestStore(t)
	if err := s.applyFieldMeta([]wire.FieldMeta{
		{Set: "http", Field: "fast", Kind: model.KindGauge, MaxIntervalMs: 500},
		{Set: "http", Field: "odd", Kind: model.KindGauge, MaxIntervalMs: 1500},
		{Set: "http", Field: "legacy", Kind: model.KindGauge, MaxIntervalS: 5},
	}); err != nil {
		t.Fatalf("field meta: %v", err)
	}
	sc := s.Schema()
	for _, tc := range []struct {
		field string
		want  int64
	}{{"fast", 500}, {"odd", 1500}, {"legacy", 5000}} {
		fi, ok := sc.Field("http", tc.field)
		if !ok {
			t.Fatalf("%s is missing from the catalogue", tc.field)
		}
		if fi.MaxInterval != tc.want {
			t.Errorf("%s: MaxInterval = %d ms, want %d", tc.field, fi.MaxInterval, tc.want)
		}
	}
}

// Catalogue() used to hand back the live BucketSets map. The caller
// marshals it after the lock is released, and a concurrent write carrying
// bucket metadata rewrites that same map under the write lock -- which is
// not a race the runtime tolerates but a fatal error that takes the
// process down, and in plugin mode that process owns the data directory.
func TestCatalogueDoesNotAliasLiveBucketSets(t *testing.T) {
	s := openTestStore(t)
	if err := s.applyFieldMeta([]wire.FieldMeta{
		{Set: "http", Field: "b0", Kind: model.KindGauge, BucketSet: "lat", BucketIndex: 0, BucketEdge: 0},
	}); err != nil {
		t.Fatalf("field meta: %v", err)
	}
	cat := s.Catalogue()
	if len(cat.Sets) != 1 || len(cat.Sets[0].BucketSets) != 1 {
		t.Fatalf("catalogue does not describe the bucket set: %+v", cat.Sets)
	}
	// Mutating the rendered catalogue must not reach the store, and a
	// later write must not reach the rendered catalogue.
	cat.Sets[0].BucketSets["scribbled"] = wire.BucketSetInfo{}
	cat.Sets[0].BucketSets["lat"].Buckets[0] = "scribbled"
	if err := s.applyFieldMeta([]wire.FieldMeta{
		{Set: "http", Field: "b1", Kind: model.KindGauge, BucketSet: "lat", BucketIndex: 1, BucketEdge: 1},
	}); err != nil {
		t.Fatalf("second field meta: %v", err)
	}
	if got := len(cat.Sets[0].BucketSets["lat"].Buckets); got != 1 {
		t.Errorf("the rendered catalogue grew to %d bucket(s) under a concurrent write: it aliases store state", got)
	}
	fresh := s.Catalogue()
	bs := fresh.Sets[0].BucketSets["lat"]
	if len(bs.Buckets) != 2 || bs.Buckets[0] != "b0" || bs.Buckets[1] != "b1" {
		t.Errorf("store state was scribbled on through the rendered catalogue: %+v", bs)
	}
	if _, leaked := fresh.Sets[0].BucketSets["scribbled"]; leaked {
		t.Error("a key added to the rendered catalogue reached the store")
	}
}

// The same aliasing, caught the way it actually bites: rendering the
// catalogue while writes carry bucket metadata. Run under -race.
func TestCatalogueIsSafeUnderConcurrentFieldMeta(t *testing.T) {
	s := openTestStore(t)
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
			_ = s.applyFieldMeta([]wire.FieldMeta{{
				Set: "http", Field: fmt.Sprintf("b%d", i%16), Kind: model.KindGauge,
				BucketSet: fmt.Sprintf("lat%d", i%4), BucketIndex: i % 16,
			}})
		}
	}()
	for i := 0; i < 500; i++ {
		// Marshalled outside the store lock, exactly as the HTTP handler
		// and the plugin's resource handler do it.
		if _, err := json.Marshal(s.Catalogue()); err != nil {
			t.Fatalf("marshal catalogue: %v", err)
		}
		if _, ok := s.Schema().BucketSet("http", "lat0"); ok {
			_ = ok
		}
	}
	close(stop)
	wg.Wait()
}

// CatalogueVersion is the version of the catalogue *schema*, and its whole
// point is that it does not move when nothing changed. LimitMin and
// LimitMax are pointers, and a decoded request allocates fresh ones every
// time, so comparing the struct made every re-declaration of identical
// limits look like a change -- and woke every client watching the ETag.
func TestRedeclaringIdenticalLimitsDoesNotBumpTheVersion(t *testing.T) {
	s := openTestStore(t)
	lo, hi := 0.0, 100.0
	meta := func() []wire.FieldMeta {
		min, max := lo, hi
		return []wire.FieldMeta{{
			Set: "http", Field: "latency", Kind: model.KindGauge,
			LimitMin: &min, LimitMax: &max,
		}}
	}
	if err := s.applyFieldMeta(meta()); err != nil {
		t.Fatalf("field meta: %v", err)
	}
	v := s.CatalogueVersion()
	for i := 0; i < 3; i++ {
		if err := s.applyFieldMeta(meta()); err != nil {
			t.Fatalf("field meta: %v", err)
		}
	}
	if got := s.CatalogueVersion(); got != v {
		t.Errorf("CatalogueVersion moved from %d to %d on metadata that did not change", v, got)
	}
	// A real change still moves it.
	changed := 5.0
	if err := s.applyFieldMeta([]wire.FieldMeta{{Set: "http", Field: "latency", LimitMin: &changed}}); err != nil {
		t.Fatalf("field meta: %v", err)
	}
	if s.CatalogueVersion() == v {
		t.Error("a changed limit did not move CatalogueVersion")
	}
}

// The same churn from the neighbouring branch: a bucket-set declaration
// marked the catalogue changed whether or not it was, so every ingest
// process that declared one moved the version on startup.
func TestRedeclaringABucketSetDoesNotBumpTheVersion(t *testing.T) {
	s := openTestStore(t)
	meta := []wire.FieldMeta{
		{Set: "lat", Field: "b0", Kind: model.KindGauge, BucketSet: "hdr", BucketIndex: 0, BucketEdge: 0, Unit: "ms"},
		{Set: "lat", Field: "b1", Kind: model.KindGauge, BucketSet: "hdr", BucketIndex: 1, BucketEdge: 1, Unit: "ms"},
	}
	if err := s.applyFieldMeta(meta); err != nil {
		t.Fatalf("field meta: %v", err)
	}
	v := s.CatalogueVersion()
	for i := 0; i < 3; i++ {
		if err := s.applyFieldMeta(meta); err != nil {
			t.Fatalf("field meta: %v", err)
		}
	}
	if got := s.CatalogueVersion(); got != v {
		t.Errorf("CatalogueVersion moved from %d to %d on a bucket set that did not change", v, got)
	}
	// A new bucket is a real change.
	if err := s.applyFieldMeta([]wire.FieldMeta{
		{Set: "lat", Field: "b2", Kind: model.KindGauge, BucketSet: "hdr", BucketIndex: 2, BucketEdge: 2, Unit: "ms"},
	}); err != nil {
		t.Fatalf("field meta: %v", err)
	}
	if s.CatalogueVersion() == v {
		t.Error("a new bucket did not move CatalogueVersion")
	}
	bs, ok := s.Catalogue().Sets[0].BucketSets["hdr"]
	if !ok || len(bs.Buckets) != 3 || bs.Buckets[2] != "b2" {
		t.Errorf("the bucket set does not describe the new bucket: %+v", bs)
	}
}

// Dropping a set has to take its spec-supplied retention and shard width
// with it. They used to be left behind, so a set of the same name created
// afterwards silently inherited the policy of the one that was deleted --
// until a restart, which loaded neither, and then it silently did not.
func TestDroppingASetForgetsItsRetentionOverride(t *testing.T) {
	s := openTestStore(t)
	s.SetRetentionFor("http", 7*24*time.Hour, time.Hour)
	if got := s.retentionFor("http"); got != 7*24*time.Hour {
		t.Fatalf("retention override was not applied: %v", got)
	}
	writeSamples(t, s, "http", []model.Sample{{TSMs: base(), Fields: map[string]model.Value{"v": model.Int(1)}}})

	// Through the admin endpoint, which is where the set is dropped.
	api := NewAPI(s, APIConfig{MaxRequestBytes: 1 << 20})
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, httptest.NewRequest("DELETE", "/v1/admin/sets/http", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE /v1/admin/sets/http = %d: %s", rec.Code, rec.Body.String())
	}
	if got := s.retentionFor("http"); got != s.cfg.Retention {
		t.Errorf("retention for a dropped set is %v, want the default %v", got, s.cfg.Retention)
	}
	if got := s.shardWidth("http"); got != s.cfg.Shard {
		t.Errorf("shard width for a dropped set is %v, want the default %v", got, s.cfg.Shard)
	}
	if _, ok := s.catalogue["http"]; ok {
		t.Error("the catalogue entry survived the drop")
	}
}

// seriesKey frames its parts with lengths, for the reason PrimaryKey does:
// a label value is any valid UTF-8, so it may contain the '=' and the NUL
// that separated it. Two distinct groupings that build one key are two
// series drawn as one.
func TestSeriesKeySeparatesAmbiguousLabelSets(t *testing.T) {
	by := []string{"a", "ab"}
	one := map[string]string{"a": "x\x00ab=y", "ab": "z"}
	two := map[string]string{"a": "x", "ab": "y\x00ab=z"}
	if seriesKey(one, by, "f") == seriesKey(two, by, "f") {
		t.Fatal("two distinct label sets share a series key: the series would be merged")
	}
	// The field name must not be able to run into the last label either.
	three := map[string]string{"a": "x", "ab": "y"}
	if seriesKey(three, by, "f") == seriesKey(map[string]string{"a": "x", "ab": "y\x00f"}, by, "") {
		t.Fatal("a label value that swallows the field name shares its series key")
	}
}

// The store's own day arithmetic used a truncating division while the
// modulo next to it was written for negatives: a pre-epoch timestamp
// floored into the following day.
func TestShardNameFloorsPreEpochTimestamps(t *testing.T) {
	s := openTestStore(t)
	s.SetRetentionFor("old", 24*365*100*time.Hour, 24*time.Hour)
	ts := time.Date(1969, 12, 30, 6, 0, 0, 0, time.UTC).UnixMilli()
	if got, want := s.shardName("old", ts), "old@19691230"; got != want {
		t.Errorf("shardName = %q, want %q", got, want)
	}
}

// An unknown storage profile used to be ignored, so a typo quietly served
// the local tuning on a network filesystem.
func TestUnknownStorageProfileIsRefused(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.StorageProfile = "netwrok-fs"
	s, err := Open(cfg)
	if err == nil {
		_ = s.Close()
		t.Fatal("an unknown storage profile was accepted")
	}
}

// The `db:` block reaches the engine. It used to be parsed and dropped, so
// an operator sizing the store for a small host kept the 1 GiB default.
func TestEngineTuningReachesTheEngine(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.CacheBytes = 8 << 20
	cfg.MemTableSizeBytes = 4 << 20
	cfg.MaxConcurrentCompactions = 1
	cfg.Compression = "zstd"
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	writeSamples(t, s, "http", []model.Sample{{TSMs: base(), Fields: map[string]model.Value{"v": model.Int(1)}}})

	cfg.DataDir = t.TempDir()
	cfg.Compression = "zstdd"
	bad, err := Open(cfg)
	if err == nil {
		_ = bad.Close()
		t.Fatal("an unknown compression profile was accepted; the engine falls back to snappy for one it does not know")
	}
}

// A set whose every shard has aged out is forgotten completely, not just
// removed from the catalogue.
//
// The spec-supplied retention and shard width live in their own maps and
// are persisted on the catalogue entry, so deleting only the entry left
// them stranded twice over: a set later created with the same name
// inherited the policy of the one that aged out, and after a restart the
// persisted declaration was gone -- an ingester that is already running
// never repeats it -- so with the documented "--retention 0 globally,
// per-set retention from the spec" deployment the set came back with no
// retention at all and was routed to the unsharded shard the sweep skips.
func TestRetentionSweepForgetsThePolicyWithTheSet(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.Retention = 0 // the documented "keep everything unless a spec says otherwise"
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	day := int64(24 * 60 * 60 * 1000)
	retention := 7 * day
	shard := day
	old := time.Now().Add(-40 * 24 * time.Hour).UnixMilli()
	if _, err := s.Write(&wire.WriteRequest{
		SetMeta: []wire.SetMeta{{Set: "app", RetentionMs: &retention, ShardMs: &shard}},
		Batches: []model.Batch{{Set: "app", Samples: []model.Sample{
			{TSMs: old, Fields: map[string]model.Value{"v": model.Int(1)}},
		}}},
	}, "", "test"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := s.retentionFor("app"); got != time.Duration(retention)*time.Millisecond {
		t.Fatalf("spec retention did not take effect: %v", got)
	}

	n, err := s.RunRetention(time.Now())
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if n == 0 {
		t.Fatal("expected the aged-out shard to be dropped")
	}
	// The override went with the entry, so a set recreated under the same
	// name does not silently inherit a dead set's policy.
	s.retentionMu.RLock()
	_, keptRetention := s.setRetention["app"]
	_, keptShard := s.setShard["app"]
	s.retentionMu.RUnlock()
	if keptRetention || keptShard {
		t.Fatalf("retention sweep left the spec policy behind: retention=%v shard=%v", keptRetention, keptShard)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// And it is gone after a restart too, rather than the entry being
	// absent while the in-memory maps still held it.
	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got := s2.retentionFor("app"); got != 0 {
		t.Fatalf("expected the default retention after the set aged out, got %v", got)
	}
}

// Close is called from a deferred cleanup that an error path may already
// have taken. A second saveCatalogue and a second db.Close is at best
// wasted work and at worst a write against a closed engine -- which is
// what the second call used to attempt, logging an error about a
// shutdown that had already succeeded.
func TestCloseIsIdempotent(t *testing.T) {
	var logs bytes.Buffer
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.Logger = log.New(&logs, "", 0)
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close should be a no-op, got %v", err)
	}
	if strings.Contains(logs.String(), "ERROR") {
		t.Fatalf("a second Close must not touch the closed engine: %s", logs.String())
	}
}

// The cardinality budget counts values, not holes. A lost dictionary
// record leaves a hole that takeHole exists to reuse, so counting it
// against MaxLabelCardinality refused writes on a key that was under its
// budget -- and charged for the same position twice.
func TestCardinalityBudgetCountsLiveValuesNotHoles(t *testing.T) {
	s := openTestStore(t)
	s.cfg.MaxLabelCardinality = 3

	for _, v := range []string{"a", "b", "c"} {
		if _, err := s.intern("host", v); err != nil {
			t.Fatalf("intern %q: %v", v, err)
		}
	}
	if _, err := s.intern("host", "d"); err == nil {
		t.Fatal("expected the fourth value to exceed the limit of 3")
	}

	// Punch a hole the way a lost record does, then rebuild the free list
	// exactly as loadDictionaries does on the next open.
	s.dictMu.Lock()
	d := s.dict["host"]
	delete(d.index, "b")
	d.Entries[1] = ""
	d.rebuildIndex()
	s.dictMu.Unlock()

	if _, err := s.intern("host", "d"); err != nil {
		t.Fatalf("a freed position must be reusable within the budget: %v", err)
	}
	if _, err := s.intern("host", "e"); err == nil {
		t.Fatal("the budget must still bind once the hole is filled")
	}
}

// HISTOGRAM under a timeseries format used to validate with no diagnostic
// and then draw nothing: the planner resolves no field for a bucket set,
// so runTimeseries iterates an empty list. An empty panel that nothing
// explains is the failure the validator exists to prevent.
func TestHistogramOutsideHeatmapIsRefusedByTheStore(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "app", []model.Sample{{
		TSMs:   base(),
		Fields: map[string]model.Value{"b0": model.Int(3), "b1": model.Int(4)},
	}},
		wire.FieldMeta{Set: "app", Field: "b0", BucketSet: "lat", BucketIndex: 0},
		wire.FieldMeta{Set: "app", Field: "b1", BucketSet: "lat", BucketIndex: 1, BucketEdge: 1},
	)

	q := &mql.Query{Kind: mql.KindQuery, From: "app", Select: []mql.FieldExpr{{Histogram: "lat"}}}
	_, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: base() - 1000, ToMs: base() + 1000, MaxPoints: 100, IntervalMs: 1000,
	})
	if err == nil {
		t.Fatal("expected a HISTOGRAM without FORMAT heatmap to be refused rather than drawing an empty panel")
	}
	// The heatmap form still runs.
	q.Format = mql.FormatHeatmap
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: base() - 1000, ToMs: base() + 1000, MaxPoints: 100, IntervalMs: 1000,
	})
	if err != nil {
		t.Fatalf("FORMAT heatmap should still run: %v", err)
	}
	if len(resp.Series) == 0 {
		t.Fatal("expected heatmap series")
	}
}

// Explain must report the window the executor would really use. It
// clamps a negative EVERY to zero; reporting the raw value made the plan
// describe something that never runs.
func TestExplainReportsTheClampedWindow(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "app", []model.Sample{{TSMs: base(), Fields: map[string]model.Value{"v": model.Int(1)}}})
	every := int64(-5)
	q := &mql.Query{Kind: mql.KindQuery, From: "app", Select: []mql.FieldExpr{{Field: "v"}}, EveryMs: &every}
	plan, err := s.Explain(q, &wire.QueryRequest{FromMs: base() - 1000, ToMs: base() + 1000, MaxPoints: 100})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if got := plan["downsample_window"].(int64); got != 0 {
		t.Fatalf("expected the window Explain reports to be clamped like the executor's, got %d", got)
	}
}

// A spec's sets: block reaching the store as SetMeta alone creates the
// catalogue entry, so it has to move the catalogue version with it.
// entryLocked created the set and nothing bumped catVer, so /v1/catalogue
// answered 304 to every client still holding the old ETag and a datasource
// that caches it never saw the set at all.
func TestSetMetaOnlyWriteMovesTheCatalogueVersion(t *testing.T) {
	s := openTestStore(t)
	before := s.CatalogueVersion()
	day := (24 * time.Hour).Milliseconds()
	if _, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{Set: "app", RetentionMs: &day}}}, "", "test"); err != nil {
		t.Fatalf("write: %v", err)
	}
	found := false
	for _, si := range s.Catalogue().Sets {
		if si.Name == "app" {
			found = true
		}
	}
	if !found {
		t.Fatal("the set-meta write did not add the set to the catalogue")
	}
	if s.CatalogueVersion() == before {
		t.Fatalf("the catalogue gained a set while the version stayed at %d: a client holding that ETag is answered 304 forever", before)
	}
	// Repeating the identical declaration is not a change: an ingest that
	// re-declares it on every start must not wake every watching client.
	steady := s.CatalogueVersion()
	if _, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{Set: "app", RetentionMs: &day}}}, "", "test"); err != nil {
		t.Fatalf("re-declare: %v", err)
	}
	if got := s.CatalogueVersion(); got != steady {
		t.Fatalf("re-declaring identical set metadata moved the version from %d to %d", steady, got)
	}
}

// The wire catalogue has to carry the stale flag, because that is the form
// a proxy-mode plugin validates against. Computing it only in Schema.Field
// meant the same query answered W203 "has not been seen recently" under
// mode: plugin and nothing at all under mode: proxy.
func TestCatalogueCarriesTheStaleFlag(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "app", []model.Sample{{TSMs: base(), Fields: map[string]model.Value{"v": model.Int(1)}}})
	s.mu.Lock()
	s.catalogue["app"].Fields["v"].LastSeenMs = time.Now().Add(-30 * 24 * time.Hour).UnixMilli()
	s.mu.Unlock()

	if info, ok := s.Schema().Field("app", "v"); !ok || !info.Stale {
		t.Fatalf("the embedded schema does not report the field as stale: %+v", info)
	}
	for _, si := range s.Catalogue().Sets {
		if si.Name != "app" {
			continue
		}
		if !si.Fields["v"].Stale {
			t.Fatal("the catalogue sent over the wire dropped the stale flag, so a proxy-mode plugin never warns")
		}
		return
	}
	t.Fatal("set app is missing from the catalogue")
}

// A hole in a legacy packed dictionary is a free position, not a value.
// Indexing it made lookup(key, "") answer with the hole's index, so a
// query lowering label = "" produced an equality against a position
// instead of the constant-false plus W201 an unknown value gets.
func TestALegacyDictionaryHoleIsNotAValue(t *testing.T) {
	s := openTestStore(t)
	packed, err := json.Marshal(dictionary{Entries: []string{"web1", "", "web3"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.PutDict(dictPackedPrefix+"host", packed); err != nil {
		t.Fatalf("put: %v", err)
	}
	s.dict = map[string]*dictionary{}
	if err := s.loadDictionaries(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if idx, ok := s.lookup("host", ""); ok {
		t.Fatalf("the empty string resolved to dictionary index %d, which is a hole", idx)
	}
	if _, ok := s.lookup("host", "web3"); !ok {
		t.Fatal("a real value was lost while skipping the hole")
	}
}

// Request bodies are buffered before a write slot is taken, so nothing but
// this budget bounds what the API holds in memory: peak footprint used to
// be concurrent connections x max_request_bytes, with nothing capping the
// connections.
func TestRequestBodiesAreBoundedInAggregate(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{MaxRequestBytes: 1 << 20, MaxBufferedRequestBytes: 1 << 20})
	body := `{"batches":[{"set":"app","samples":[]}]}`

	// Nothing outstanding: the request is admitted.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/write", strings.NewReader(body))
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("an ordinary write was refused: %d %s", rec.Code, rec.Body.String())
	}

	// With the budget already committed, the next body is shed rather
	// than read: the client backs off instead of the store growing.
	api.bodyBytes.Add(1 << 20)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/write", strings.NewReader(body))
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("a body past the buffer budget got %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("a shed write must say when to come back")
	}
	api.bodyBytes.Add(-(1 << 20))
	if got := api.bodyBytes.Load(); got != 0 {
		t.Fatalf("the budget leaked: %d bytes still reserved after every request finished", got)
	}
}

// With auth off the store cannot tell its callers apart, and answering
// "anonymous" for all of them then *overwrote* whatever client label the
// ingester sent. Every ingester on a loopback store -- the documented
// posture, and what the quick start runs -- reported under one name, so
// --client-name did nothing and two ingesters drew as one series whose
// counters interleave.
func TestIngestClientLabelSurvivesWithAuthDisabled(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{}) // auth.mode unset, i.e. none
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	post := func(client string) {
		t.Helper()
		body, err := json.Marshal(wire.WriteRequest{Batches: []model.Batch{{
			Set: model.IngestSet,
			Samples: []model.Sample{{
				TSMs:   base(),
				Labels: map[string]string{"client": client},
				Fields: map[string]model.Value{"records": model.Int(1)},
			}},
		}}})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		resp, err := http.Post(srv.URL+"/v1/write", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("write returned %d", resp.StatusCode)
		}
	}
	post("web1")
	post("web2")

	got := map[string]bool{}
	for _, v := range s.LabelValues("client") {
		got[v] = true
	}
	for _, want := range []string{"web1", "web2"} {
		if !got[want] {
			t.Fatalf("client %q was not recorded; the store stamped its own name over it (saw %v)", want, s.LabelValues("client"))
		}
	}
	if got["anonymous"] {
		t.Fatal("the store stamped \"anonymous\" onto the ingest set even though nothing authenticated the caller")
	}
}

// With bearer auth the store really does know who is writing, so the
// authenticated name is the authority and still wins.
func TestIngestClientLabelIsStampedWhenAuthenticated(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{
		AuthMode: "bearer",
		Clients:  []ClientAuth{{Name: "collector", Hash: HashSecret("s3cret"), Scopes: []Scope{ScopeWrite}}},
	})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	body, err := json.Marshal(wire.WriteRequest{Batches: []model.Batch{{
		Set: model.IngestSet,
		Samples: []model.Sample{{
			TSMs:   base(),
			Labels: map[string]string{"client": "spoofed"},
			Fields: map[string]model.Value{"records": model.Int(1)},
		}},
	}}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/write", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer s3cret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write returned %d", resp.StatusCode)
	}
	for _, v := range s.LabelValues("client") {
		if v == "spoofed" {
			t.Fatal("a client's own claim outranked the credential the store verified")
		}
	}
}

// Retention persists the catalogue as soon as it forgets a set, the way
// the admin drop does. Waiting for the 30-second tick meant a crash in
// that window brought the entry back, advertising fields and a time range
// whose shards had just been range-deleted.
func TestRetentionPersistsTheForgottenSet(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.Retention = time.Hour
	cfg.Shard = time.Hour
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	old := time.Now().Add(-72 * time.Hour).UnixMilli()
	writeSamples(t, s, "app", []model.Sample{{
		TSMs:   old,
		Labels: map[string]string{"host": "a"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}})
	// The periodic save the HTTP layer runs on a timer has fired, so the
	// entry is on disk. That is the state retention has to clean up
	// rather than leave for the next tick.
	if err := s.SaveCatalogue(); err != nil {
		t.Fatalf("save catalogue: %v", err)
	}
	if n, err := s.RunRetention(time.Now()); err != nil || n == 0 {
		t.Fatalf("retention dropped %d shard(s): %v", n, err)
	}
	if len(s.Sets()) != 0 {
		t.Fatalf("catalogue still holds %v after every shard aged out", s.Sets())
	}
	// Reopened without a clean Close, which is what a crash looks like.
	if err := s.db.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
	again, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer again.Close()
	if sets := again.Sets(); len(sets) != 0 {
		t.Fatalf("set %v came back after a crash; retention forgot it in memory only", sets)
	}
}

// The legend renders every BY slot, including one the row did not carry.
// Dropping those collapsed two distinct series onto one name, so the panel
// drew two lines that could not be told apart.
func TestSeriesNameKeepsAbsentLabelSlotsDistinct(t *testing.T) {
	by := []string{"host", "pool"}
	a := seriesName(map[string]string{"host": "a"}, by, "")
	b := seriesName(map[string]string{"pool": "a"}, by, "")
	if a == b {
		t.Fatalf("two different series both render as %q", a)
	}
	// The ordinary case is untouched.
	if got := seriesName(map[string]string{"host": "a", "pool": "b"}, by, "req/s"); got != "a : b : req/s" {
		t.Fatalf("legend is %q, want %q", got, "a : b : req/s")
	}
}

// A hole in a label dictionary is a position whose record was lost, not a
// value. Every other reader already skips one -- LabelValues and the regex
// lowering both do -- but labelValue reported it as the empty string, so
// the filtered form of a LABELS query, which is the one that reads indices
// off rows, listed an empty value among the hosts. A dashboard variable
// built on it grew a blank option the unfiltered form never showed.
func TestADictionaryHoleIsNotALabelValue(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	now := time.Now()
	writeSamples(t, s, "http", []model.Sample{
		{TSMs: now.UnixMilli(), Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
		{TSMs: now.Add(time.Millisecond).UnixMilli(), Labels: map[string]string{"host": "b"}, Fields: map[string]model.Value{"v": model.Int(2)}},
	})
	// Punch a hole where "a" was, the way a lost dictionary record leaves
	// one: the rows keep pointing at the position, the position holds
	// nothing.
	idx, ok := s.lookup("host", "a")
	if !ok {
		t.Fatal("host=a was never interned")
	}
	s.dictMu.Lock()
	d := s.dict["host"]
	d.Entries[idx] = ""
	delete(d.index, "a")
	d.rebuildIndex()
	s.dictMu.Unlock()

	if v, ok := s.labelValue("host", idx); ok {
		t.Fatalf("a hole resolved to %q; it is not a value", v)
	}
	for _, v := range s.LabelValues("host") {
		if v == "" {
			t.Fatal("the unfiltered LABELS form listed a hole")
		}
	}
	// The filtered form scans rows and translates their indices, so it is
	// the one that saw the hole.
	resp := query(t, s, `LABELS host WHERE HAS v`, now.Add(-time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli())
	for _, r := range resp.Rows {
		if len(r.Values) > 0 && r.Values[0] == "" {
			t.Fatalf("LABELS with a predicate listed an empty value: %+v", resp.Rows)
		}
	}
}

// The auxiliary query forms answer the same shape the data path does.
// Series has no omitempty, so leaving it nil put "series": null on the
// wire for exactly the queries a dashboard variable runs, while every
// other query answers "series": [].
func TestAuxiliaryFormsAnswerAnEmptySeriesList(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	now := time.Now()
	writeSamples(t, s, "http", []model.Sample{{
		TSMs: now.UnixMilli(), Labels: map[string]string{"host": "a"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}})
	for _, text := range []string{`SETS`, `FIELDS FROM http`, `LABEL KEYS FROM http`, `LABELS host`} {
		resp := query(t, s, text, now.Add(-time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli())
		if resp.Series == nil {
			t.Errorf("%s answered series: null", text)
		}
		b, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("%s: marshal: %v", text, err)
		}
		if bytes.Contains(b, []byte(`"series":null`)) {
			t.Errorf("%s marshalled series as null", text)
		}
	}
}

// An unrecognised field kind is client input and is refused as such.
//
// It used to be stored verbatim, served from /v1/catalogue and read back
// by the MQL validator, which knows four values and quietly ignores
// anything else -- so a mistyped kind reached a dashboard as metadata
// that looks declared and behaves as if it were absent. Refusing it with
// a 4xx is what the set and field names already get, and for the same
// reason: the fault is in what was sent, so it has to be fatal on the
// first attempt rather than retried at full rate.
func TestWriteRefusesAnUnknownFieldKind(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Write(&wire.WriteRequest{
		FieldMeta: []wire.FieldMeta{{Set: "app", Field: "cpu", Kind: model.Kind("couter")}},
	}, "", "test")
	if err == nil {
		t.Fatal("an unknown kind was accepted into the catalogue")
	}
	var bad *ErrBadRequest
	if !errors.As(err, &bad) {
		t.Fatalf("an unknown kind came back as %T, which handleWrite answers 500 to; the client retries a 500 forever: %v", err, err)
	}
	if !strings.Contains(bad.Msg, "couter") || !strings.Contains(bad.Msg, "app.cpu") {
		t.Fatalf("refusal names neither the kind nor the field: %s", bad.Msg)
	}
	if len(s.Catalogue().Sets) != 0 {
		t.Fatal("the refused declaration still reached the catalogue")
	}
	// The four documented kinds still land.
	for i, kind := range []model.Kind{model.KindCounter, model.KindGauge, model.KindDelta, model.KindString} {
		if _, err := s.Write(&wire.WriteRequest{
			FieldMeta: []wire.FieldMeta{{Set: "app", Field: fmt.Sprintf("f%d", i), Kind: kind}},
		}, "", "test"); err != nil {
			t.Fatalf("kind %q was refused: %v", kind, err)
		}
	}
}
