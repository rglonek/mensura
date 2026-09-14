package store

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

func openTuned(t *testing.T, tune func(*Config)) *Store {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Retention = 0
	if tune != nil {
		tune(&cfg)
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func put(t *testing.T, s *Store, set string, samples ...model.Sample) *wire.WriteResponse {
	t.Helper()
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: set, Samples: samples}}}, "", "")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	return resp
}

// A negative LIMIT POINTS used to reach runTabular as a slice bound and
// panic there. In plugin mode nothing recovers that panic, so it took down
// the process holding the data directory.
func TestNegativeLimitIsRejectedNotPanicked(t *testing.T) {
	s := openTuned(t, nil)
	now := time.Now().UnixMilli()
	put(t, s, "app",
		model.Sample{TSMs: now, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
		model.Sample{TSMs: now + 1, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(2)}},
	)
	for _, limit := range []int{-1, 0, -1000} {
		limit := limit
		t.Run("", func(t *testing.T) {
			q := &mql.Query{
				Kind: mql.KindQuery, From: "app", Format: mql.FormatTable,
				Select: []mql.FieldExpr{{Field: "v"}},
				Limits: mql.Limits{Points: &limit},
			}
			// Must be refused, and must not panic on the way.
			_, err := s.Query(context.Background(), &wire.QueryRequest{
				AST: q, FromMs: 0, ToMs: math.MaxInt64, MaxPoints: 100,
			})
			if err == nil {
				t.Fatalf("LIMIT POINTS %d was accepted", limit)
			}
		})
	}
}

// The executor is defended independently of the validator, because a
// caller that builds a wire.QueryRequest reaches it directly.
func TestRunTabularClampsNegativeLimit(t *testing.T) {
	s := openTuned(t, nil)
	now := time.Now().UnixMilli()
	put(t, s, "app",
		model.Sample{TSMs: now, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
	)
	q := &mql.Query{Kind: mql.KindQuery, From: "app", Format: mql.FormatTable, Select: []mql.FieldExpr{{Field: "v"}}}
	neg := -1
	q.Limits.Points = &neg
	plan, _, err := s.plan(q, &wire.QueryRequest{FromMs: 0, ToMs: math.MaxInt64})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	resp := &wire.QueryResponse{}
	if err := s.runTabular(context.Background(), q, &wire.QueryRequest{FromMs: 0, ToMs: math.MaxInt64}, plan, resp); err != nil {
		t.Fatalf("runTabular: %v", err)
	}
	if len(resp.Rows) != 0 {
		t.Fatalf("expected no rows for a clamped limit, got %d", len(resp.Rows))
	}
}

// A timestamp the shard-suffix encoding cannot express produced a shard
// that no query, no catalogue read and no retention sweep could ever see.
func TestOutOfRangeTimestampIsRejected(t *testing.T) {
	s := openTuned(t, func(c *Config) { c.Retention = 24 * time.Hour })
	resp := put(t, s, "app",
		model.Sample{TSMs: math.MaxInt64 / 2, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
		model.Sample{TSMs: model.MaxTSMs + 1, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
	)
	if resp.Accepted != 0 {
		t.Fatalf("accepted %d out-of-range samples", resp.Accepted)
	}
	if len(resp.Rejected) != 2 {
		t.Fatalf("expected 2 rejections, got %v", resp.Rejected)
	}
	// Nothing unreachable was created.
	for _, name := range s.db.Sets() {
		if _, _, err := parseShardSuffix(name[len("app@"):]); err != nil {
			t.Fatalf("shard %q is not parseable, so it would be invisible forever", name)
		}
	}
	// The largest expressible timestamp still works end to end.
	if r := put(t, s, "app",
		model.Sample{TSMs: model.MaxTSMs, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
	); r.Accepted != 1 {
		t.Fatalf("the boundary timestamp was rejected: %v", r.Rejected)
	}
	if got, _ := s.shardsFor("app", math.MinInt64, math.MaxInt64); len(got) != 1 {
		t.Fatalf("boundary sample is not in a visible shard: %v", got)
	}
}

// Retention declared by a spec over the write API has to be swept even
// when the store's own default is "keep everything".
func TestSpecSuppliedRetentionIsSwept(t *testing.T) {
	s := openTuned(t, func(c *Config) {
		c.Retention = 0
		c.RetentionSweep = time.Hour
		c.Shard = time.Hour
	})
	day := int64(24 * 3600 * 1000)
	if err := s.applySetMeta([]wire.SetMeta{{Set: "app", RetentionMs: &day}}); err != nil {
		t.Fatalf("set meta: %v", err)
	}
	old := time.Now().Add(-72 * time.Hour).UnixMilli()
	put(t, s, "app",
		model.Sample{TSMs: old, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
	)
	if got, _ := s.shardsFor("app", math.MinInt64, math.MaxInt64); len(got) == 0 {
		t.Fatal("the sample was not stored in a shard")
	}
	n, err := s.RunRetention(time.Now())
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if n == 0 {
		t.Fatal("spec-declared retention dropped nothing")
	}
}

// The heatmap path is bounded by the same ceilings as the timeseries path.
func TestHeatmapHonoursSeriesCeiling(t *testing.T) {
	s := openTuned(t, func(c *Config) { c.MaxSeriesPerGraph = 2 })
	now := time.Now().UnixMilli()
	edge := 0.0
	metas := []wire.FieldMeta{}
	for i, b := range []string{"b0", "b1"} {
		metas = append(metas, wire.FieldMeta{Set: "h", Field: b, BucketSet: "lat", BucketIndex: i, BucketEdge: edge})
		edge++
	}
	s.applyFieldMeta(metas)
	for host := 0; host < 8; host++ {
		put(t, s, "h", model.Sample{
			TSMs:   now + int64(host),
			Labels: map[string]string{"host": string(rune('a' + host))},
			Fields: map[string]model.Value{"b0": model.Int(1), "b1": model.Int(2)},
		})
	}
	q := &mql.Query{
		Kind: mql.KindQuery, From: "h", Format: mql.FormatHeatmap,
		Select: []mql.FieldExpr{{Histogram: "lat"}}, By: []string{"host"},
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{AST: q, FromMs: 0, ToMs: math.MaxInt64, MaxPoints: 100})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if resp.Error == "" || !resp.Stats.Truncated {
		t.Fatalf("heatmap ran past the series ceiling unreported: %d series, err=%q", len(resp.Series), resp.Error)
	}
	if len(resp.Series) > 2 {
		t.Fatalf("heatmap returned %d series with a ceiling of 2", len(resp.Series))
	}
}

// LABELS ... WHERE is a documented form; the filter used to be dropped.
func TestLabelsWhereFiltersValues(t *testing.T) {
	s := openTuned(t, nil)
	now := time.Now().UnixMilli()
	put(t, s, "app",
		model.Sample{TSMs: now, Labels: map[string]string{"host": "eu1", "dc": "eu"}, Fields: map[string]model.Value{"v": model.Int(1)}},
		model.Sample{TSMs: now + 1, Labels: map[string]string{"host": "us1", "dc": "us"}, Fields: map[string]model.Value{"v": model.Int(1)}},
	)
	q, err := mql.Parse(`LABELS host WHERE dc = "eu"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{AST: q, FromMs: 0, ToMs: math.MaxInt64})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var got []string
	for _, r := range resp.Rows {
		got = append(got, r.Values[0].(string))
	}
	if len(got) != 1 || got[0] != "eu1" {
		t.Fatalf("filter ignored: got %v, want [eu1]", got)
	}
}

// The catalogue version tracks schema, not the moving time range.
func TestCatalogueVersionIgnoresTimeRange(t *testing.T) {
	s := openTuned(t, nil)
	now := time.Now().UnixMilli()
	put(t, s, "app",
		model.Sample{TSMs: now, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
	)
	before := s.CatalogueVersion()
	for i := 1; i < 5; i++ {
		put(t, s, "app",
			model.Sample{TSMs: now + int64(i), Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(int64(i))}},
		)
	}
	if got := s.CatalogueVersion(); got != before {
		t.Fatalf("version moved from %d to %d for writes that changed no schema", before, got)
	}
	// A genuinely new field is a schema change and must move it.
	put(t, s, "app",
		model.Sample{TSMs: now + 9, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"fresh": model.Int(1)}},
	)
	if s.CatalogueVersion() == before {
		t.Fatal("version did not move for a new field")
	}
}

// A rejected sample must not leave anything behind in the dictionary.
func TestRejectedSampleDoesNotBurnCardinality(t *testing.T) {
	s := openTuned(t, nil)
	now := time.Now().UnixMilli()
	// "v" is both a label and a field, so the sample is rejected -- after
	// the point at which its labels used to have been interned.
	resp := put(t, s, "app", model.Sample{
		TSMs:   now,
		Labels: map[string]string{"host": "ghost", "v": "x"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	})
	if resp.Accepted != 0 {
		t.Fatalf("expected the sample to be rejected, accepted %d", resp.Accepted)
	}
	if vals := s.LabelValues("host"); len(vals) != 0 {
		t.Fatalf("rejected sample interned %v", vals)
	}
}

// Scan statistics are reported rather than left at zero.
func TestQueryReportsScanStats(t *testing.T) {
	s := openTuned(t, nil)
	now := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		put(t, s, "app", model.Sample{
			TSMs: now + int64(i), Labels: map[string]string{"host": "a"},
			Fields: map[string]model.Value{"v": model.Int(int64(i))},
		})
	}
	q := &mql.Query{Kind: mql.KindQuery, From: "app", Select: []mql.FieldExpr{{Field: "v"}}}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{AST: q, FromMs: 0, ToMs: math.MaxInt64, MaxPoints: 100})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if resp.Stats.ShardsScanned == 0 || resp.Stats.RowsScanned == 0 {
		t.Fatalf("scan stats not reported: %+v", resp.Stats)
	}
}

// FORMAT logs returns the most recent records. Reading forward to a limit
// returned the oldest ones in the range and never the newest, which is the
// opposite of what a logs panel is for.
func TestLogsFormatReturnsNewestRecords(t *testing.T) {
	s := openTuned(t, nil)
	start := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	for i := 0; i < 50; i++ {
		put(t, s, "app", model.Sample{
			TSMs:   start + int64(i)*1000,
			Labels: map[string]string{"host": "a"},
			Fields: map[string]model.Value{"msg": model.String("line")},
		})
	}
	limit := 5
	q := &mql.Query{
		Kind: mql.KindQuery, From: "app", Format: mql.FormatLogs,
		Select: []mql.FieldExpr{{Field: "msg"}},
		Limits: mql.Limits{Points: &limit},
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{AST: q, FromMs: 0, ToMs: math.MaxInt64})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Rows) != limit {
		t.Fatalf("got %d rows, want %d", len(resp.Rows), limit)
	}
	if !resp.Stats.Truncated {
		t.Fatal("a truncated logs result was not reported as truncated")
	}
	// Newest five, still handed back oldest-first within the page.
	wantFirst := start + int64(45)*1000
	got := resp.Rows[0].Values[0].(int64)
	if got != wantFirst {
		t.Fatalf("first row is %d, want %d: the page is not the newest records", got, wantFirst)
	}
	last := resp.Rows[len(resp.Rows)-1].Values[0].(int64)
	if last != start+int64(49)*1000 {
		t.Fatalf("last row is %d, want the newest record", last)
	}
	for i := 1; i < len(resp.Rows); i++ {
		if resp.Rows[i-1].Values[0].(int64) > resp.Rows[i].Values[0].(int64) {
			t.Fatal("rows are not in ascending time order")
		}
	}
}

// A heatmap over a set that also holds rows without the bucket columns
// must not accumulate a group for every one of them.
//
// groups[] was recorded per scanned row, before any bucket column was
// looked at, so a row that produced no cell still cost an entry -- and
// neither ceiling ever moved, because both count cells. A set holding
// more than the histogram, grouped by a high-cardinality label, therefore
// grew a map bounded by nothing at all: the very accumulation these gates
// were added to stop, one map along from where they were put.
func TestHeatmapDoesNotGroupRowsThatCarryNoBuckets(t *testing.T) {
	s := openTuned(t, nil)
	now := time.Now().UnixMilli()
	s.applyFieldMeta([]wire.FieldMeta{
		{Set: "h", Field: "b0", BucketSet: "lat", BucketIndex: 0, BucketEdge: 0},
		{Set: "h", Field: "b1", BucketSet: "lat", BucketIndex: 1, BucketEdge: 1},
	})
	// One host carries the histogram; a hundred others carry only an
	// unrelated column, which is ordinary in a set that holds more than
	// one kind of record.
	put(t, s, "h", model.Sample{
		TSMs:   now,
		Labels: map[string]string{"host": "with-buckets"},
		Fields: map[string]model.Value{"b0": model.Int(1), "b1": model.Int(2)},
	})
	for i := 0; i < 100; i++ {
		put(t, s, "h", model.Sample{
			TSMs:   now + int64(i) + 1,
			Labels: map[string]string{"host": fmt.Sprintf("no-buckets-%d", i)},
			Fields: map[string]model.Value{"other": model.Int(1)},
		})
	}
	q := &mql.Query{
		Kind: mql.KindQuery, From: "h", Format: mql.FormatHeatmap,
		Select: []mql.FieldExpr{{Histogram: "lat"}}, By: []string{"host"},
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{AST: q, FromMs: 0, ToMs: math.MaxInt64, MaxPoints: 100})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// Only the host that carried buckets draws, and it draws both of them.
	if len(resp.Series) != 2 {
		t.Fatalf("expected the two buckets of the one host that carries them, got %d series", len(resp.Series))
	}
	for _, ser := range resp.Series {
		if ser.Labels["host"] != "with-buckets" {
			t.Fatalf("series %q is grouped under %q, which carries no buckets", ser.Name, ser.Labels["host"])
		}
	}
}
