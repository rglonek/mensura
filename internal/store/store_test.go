package store

import (
	"context"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	// Keep the test's memory footprint small; the tuned defaults are sized
	// for an ingest burst, not for a unit test.
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func writeSamples(t *testing.T, s *Store, set string, samples []model.Sample, meta ...wire.FieldMeta) {
	t.Helper()
	resp, err := s.Write(&wire.WriteRequest{
		FieldMeta: meta,
		Batches:   []model.Batch{{Set: set, Samples: samples}},
	}, "", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(resp.Rejected) > 0 {
		t.Fatalf("unexpected rejections: %+v", resp.Rejected)
	}
}

func query(t *testing.T, s *Store, text string, from, to int64) *wire.QueryResponse {
	t.Helper()
	q, err := mql.Parse(text)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: from, ToMs: to, MaxPoints: 1000, IntervalMs: 1000,
	})
	if err != nil {
		t.Fatalf("query %q: %v", text, err)
	}
	return resp
}

func base() int64 { return time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli() }

func TestWriteThenQueryTimeseries(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	var samples []model.Sample
	for i := 0; i < 10; i++ {
		for _, host := range []string{"web1", "web2"} {
			samples = append(samples, model.Sample{
				TSMs:   t0 + int64(i)*1000,
				Labels: map[string]string{"host": host, "dc": "eu1"},
				Fields: map[string]model.Value{"inflight": model.Int(int64(i * 10))},
			})
		}
	}
	writeSamples(t, s, "http", samples)

	resp := query(t, s, `FROM http SELECT inflight BY host`, t0-1000, t0+20000)
	if len(resp.Series) != 2 {
		t.Fatalf("expected one series per host, got %d: %+v", len(resp.Series), resp.Series)
	}
	if resp.Series[0].Name != "web1 : inflight" {
		t.Fatalf("legend: %q", resp.Series[0].Name)
	}
	if len(resp.Series[0].TSMs) == 0 {
		t.Fatal("series has no points")
	}
	// Extremes must survive: the maximum of the input is 90.
	maxSeen := 0.0
	for _, v := range resp.Series[0].Values {
		if v > maxSeen {
			maxSeen = v
		}
	}
	if maxSeen != 90 {
		t.Fatalf("expected the maximum 90 to survive downsampling, got %v", maxSeen)
	}
}

func TestFilterOnUnknownValueReturnsEmptyWithWarning(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	writeSamples(t, s, "http", []model.Sample{{
		TSMs:   t0,
		Labels: map[string]string{"host": "web1"},
		Fields: map[string]model.Value{"inflight": model.Int(1)},
	}})

	resp := query(t, s, `FROM http SELECT inflight WHERE host = "typo"`, t0-1000, t0+1000)
	if len(resp.Series) != 0 {
		t.Fatalf("an unknown label value must not widen the query: %+v", resp.Series)
	}
	found := false
	for _, w := range resp.Warnings {
		if w.Code == "W201" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected W201, got %+v", resp.Warnings)
	}
}

func TestStrictLabelMatching(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	writeSamples(t, s, "http", []model.Sample{
		{TSMs: t0, Labels: map[string]string{"host": "web1"}, Fields: map[string]model.Value{"v": model.Int(1)}},
		{TSMs: t0 + 1000, Labels: map[string]string{}, Fields: map[string]model.Value{"v": model.Int(2)}},
	})
	// A row missing the label must not match an equality on it...
	resp := query(t, s, `FROM http SELECT v SSE OFF WHERE host = "web1"`, t0-1000, t0+5000)
	if n := totalPoints(resp); n != 1 {
		t.Fatalf("expected only the labelled row, got %d points", n)
	}
	// ...unless the query says so explicitly.
	resp = query(t, s, `FROM http SELECT v SSE OFF WHERE host = "web1" OR MISSING host`, t0-1000, t0+5000)
	if n := totalPoints(resp); n != 2 {
		t.Fatalf("expected both rows, got %d points", n)
	}
}

func totalPoints(resp *wire.QueryResponse) int {
	n := 0
	for _, s := range resp.Series {
		for i := range s.TSMs {
			if !s.IsNull[i] {
				n++
			}
		}
	}
	return n
}

func TestFieldMetadataDrivesDefaults(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	// A counter with a declared 5 s cadence and a floor of 0.
	zero := 0.0
	writeSamples(t, s, "http", []model.Sample{
		{TSMs: t0, Labels: map[string]string{"host": "web1"}, Fields: map[string]model.Value{"reqs": model.Int(100)}},
		{TSMs: t0 + 1000, Labels: map[string]string{"host": "web1"}, Fields: map[string]model.Value{"reqs": model.Int(150)}},
		// A 60 s gap: with the declared cadence this must produce a break.
		{TSMs: t0 + 61000, Labels: map[string]string{"host": "web1"}, Fields: map[string]model.Value{"reqs": model.Int(7)}},
	}, wire.FieldMeta{Set: "http", Field: "reqs", Kind: model.KindCounter, MaxIntervalS: 5, LimitMin: &zero})

	resp := query(t, s, `FROM http SELECT reqs DELTA BY host`, t0-1000, t0+120000)
	if len(resp.Series) != 1 {
		t.Fatalf("expected one series, got %d", len(resp.Series))
	}
	ser := resp.Series[0]
	sawNull := false
	for i := range ser.TSMs {
		if ser.IsNull[i] {
			sawNull = true
		}
	}
	if !sawNull {
		t.Fatalf("declared cadence should have injected a connect-break: %+v", ser)
	}
	// The counter reset produced a negative delta; the declared floor with
	// the raw-value escape must surface the raw counter, not a clamp.
	sawRaw := false
	for i, v := range ser.Values {
		if !ser.IsNull[i] && v == 7 {
			sawRaw = true
		}
	}
	if !sawRaw {
		t.Fatalf("expected the raw counter value after the reset: %+v", ser.Values)
	}
}

func TestTimeShardingAndRetention(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.Retention = 48 * time.Hour
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	old := now.Add(-10 * 24 * time.Hour)
	writeSamples(t, s, "http", []model.Sample{
		{TSMs: old.UnixMilli(), Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(1)}},
		{TSMs: now.UnixMilli(), Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"v": model.Int(2)}},
	})
	shards, _ := s.shardsFor("http", 0, 1<<62)
	if len(shards) != 2 {
		t.Fatalf("expected two day shards, got %v", shards)
	}
	// A query for the recent window must not open the old shard.
	if got, _ := s.shardsFor("http", now.Add(-time.Hour).UnixMilli(), now.UnixMilli()); len(got) != 1 {
		t.Fatalf("expected one overlapping shard, got %v", got)
	}
	n, err := s.RunRetention(now)
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected one shard dropped, got %d", n)
	}
	if got, _ := s.shardsFor("http", 0, 1<<62); len(got) != 1 {
		t.Fatalf("expected one shard to remain, got %v", got)
	}
}

func TestIdempotentWrite(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	req := &wire.WriteRequest{Batches: []model.Batch{{Set: "http", Samples: []model.Sample{
		{TSMs: t0, Labels: map[string]string{"host": "web1"}, Fields: map[string]model.Value{"v": model.Int(1)}},
	}}}}
	if _, err := s.Write(req, "key-1", "test"); err != nil {
		t.Fatalf("write: %v", err)
	}
	resp, err := s.Write(req, "key-1", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if !resp.Duplicate {
		t.Fatal("a repeated idempotency key must not write twice")
	}
	// Even without the key, content addressing collapses the replay into
	// the same row.
	if _, err := s.Write(req, "", "test"); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := query(t, s, `FROM http SELECT v SSE OFF`, t0-1000, t0+1000)
	if n := totalPoints(got); n != 1 {
		t.Fatalf("expected the replay to collapse into one row, got %d points", n)
	}
}

func TestCardinalityGuard(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.MaxLabelCardinality = 3
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	var samples []model.Sample
	for i := 0; i < 10; i++ {
		samples = append(samples, model.Sample{
			TSMs:   base() + int64(i),
			Labels: map[string]string{"req_id": string(rune('a' + i))},
			Fields: map[string]model.Value{"v": model.Int(1)},
		})
	}
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "http", Samples: samples}}}, "", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(resp.Rejected) == 0 {
		t.Fatal("an unbounded label must be rejected, loudly and early")
	}
	if resp.Accepted != 3 {
		t.Fatalf("expected the first three values to be accepted, got %d", resp.Accepted)
	}
}

func TestReservedSetsRejected(t *testing.T) {
	s := openTestStore(t)
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{
		Set:     "_mensura_catalogue",
		Samples: []model.Sample{{TSMs: base(), Fields: map[string]model.Value{"v": model.Int(1)}}},
	}}}, "", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(resp.Rejected) != 1 {
		t.Fatalf("a reserved set must be rejected: %+v", resp)
	}
	// The ingest-progress set is the documented exception.
	resp, err = s.Write(&wire.WriteRequest{Batches: []model.Batch{{
		Set:     "_mensura_ingest",
		Samples: []model.Sample{{TSMs: base(), Fields: map[string]model.Value{"samples": model.Int(1)}}},
	}}}, "", "ingest-1")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.Accepted != 1 {
		t.Fatalf("the ingest set must be writable: %+v", resp)
	}
}

func TestSafetyGateReturnsPartialResults(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.MaxSeriesPerGraph = 2
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	t0 := base()
	var samples []model.Sample
	for i := 0; i < 5; i++ {
		samples = append(samples, model.Sample{
			TSMs:   t0 + int64(i)*1000,
			Labels: map[string]string{"host": string(rune('a' + i))},
			Fields: map[string]model.Value{"v": model.Int(int64(i))},
		})
	}
	writeSamples(t, s, "http", samples)
	resp := query(t, s, `FROM http SELECT v BY host`, t0-1000, t0+10000)
	if resp.Error == "" {
		t.Fatal("expected the series gate to report why results are partial")
	}
	if len(resp.Series) != 2 {
		t.Fatalf("expected partial results, got %d series", len(resp.Series))
	}
}

func TestTableFormat(t *testing.T) {
	s := openTestStore(t)
	t0 := base()
	writeSamples(t, s, "http", []model.Sample{
		{TSMs: t0, Labels: map[string]string{"host": "web1"}, Fields: map[string]model.Value{"v": model.Int(3)}},
		{TSMs: t0 + 1000, Labels: map[string]string{"host": "web1"}, Fields: map[string]model.Value{"v": model.Int(4)}},
	})
	resp := query(t, s, `FROM http SELECT v BY host FORMAT table`, t0-1000, t0+5000)
	if len(resp.Rows) != 2 || len(resp.Columns) != 3 {
		t.Fatalf("unexpected table: cols=%+v rows=%+v", resp.Columns, resp.Rows)
	}
}

func TestAuxiliaryQueryForms(t *testing.T) {
	s := openTestStore(t)
	writeSamples(t, s, "http", []model.Sample{{
		TSMs: base(), Labels: map[string]string{"host": "web1"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}})
	if r := query(t, s, `SETS`, 0, 0); len(r.Rows) != 1 {
		t.Fatalf("SETS: %+v", r.Rows)
	}
	if r := query(t, s, `FIELDS FROM http`, 0, 0); len(r.Rows) != 1 {
		t.Fatalf("FIELDS: %+v", r.Rows)
	}
	if r := query(t, s, `LABEL KEYS FROM http`, 0, 0); len(r.Rows) != 1 {
		t.Fatalf("LABEL KEYS: %+v", r.Rows)
	}
	if r := query(t, s, `LABELS host`, 0, 0); len(r.Rows) != 1 {
		t.Fatalf("LABELS: %+v", r.Rows)
	}
}

func TestReopenKeepsCatalogue(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	writeSamples(t, s, "http", []model.Sample{{
		TSMs: base(), Labels: map[string]string{"host": "web1"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}})
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if !s2.Schema().HasSet("http") {
		t.Fatal("the catalogue must survive a restart")
	}
	if got := s2.LabelValues("host"); len(got) != 1 || got[0] != "web1" {
		t.Fatalf("the label dictionary must survive a restart, got %v", got)
	}
	if n := totalPoints(query(t, s2, `FROM http SELECT v SSE OFF`, base()-1000, base()+1000)); n != 1 {
		t.Fatalf("expected the row to survive, got %d points", n)
	}
}
