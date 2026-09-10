package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

func reviewStore(t *testing.T) *Store {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Retention = 0
	cfg.RetentionSweep = 0
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func diagCodes(ds []mql.Diag) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.Code)
	}
	return out
}

func hasCode(ds []mql.Diag, code string) bool {
	for _, d := range ds {
		if d.Code == code {
			return true
		}
	}
	return false
}

// 06-query.md section 8 states the aggregation caveat plainly: with no BY
// clause, rows from many streams land in one series and the walk's
// duplicate-timestamp rule keeps the first and drops the rest. It names
// W301 as the mitigation, and section 12 lists the code -- and nothing
// ever emitted it, so half the samples in the range vanished into a
// plausible-looking line with no diagnostic anywhere.
func TestNoByInterleaveWarns(t *testing.T) {
	s := reviewStore(t)
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).UnixMilli()
	var samples []model.Sample
	for i := 0; i < 5; i++ {
		ts := base + int64(i)*1000
		samples = append(samples,
			model.Sample{TSMs: ts, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"cpu": model.Float(1)}},
			model.Sample{TSMs: ts, Labels: map[string]string{"host": "b"}, Fields: map[string]model.Value{"cpu": model.Float(99)}},
		)
	}
	if _, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "app", Samples: samples}}}, "", ""); err != nil {
		t.Fatalf("write: %v", err)
	}

	run := func(text string) *wire.QueryResponse {
		t.Helper()
		q, err := mql.Parse(text)
		if err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
		resp, err := s.Query(context.Background(), &wire.QueryRequest{
			AST: q, FromMs: base - 1000, ToMs: base + 60000, MaxPoints: 1000, IntervalMs: 1000,
		})
		if err != nil {
			t.Fatalf("query %q: %v", text, err)
		}
		return resp
	}

	merged := run(`FROM app SELECT cpu`)
	if !hasCode(merged.Warnings, "W301") {
		t.Errorf("no W301 for a query with no BY that dropped half its samples; warnings were %v", diagCodes(merged.Warnings))
	}
	for _, d := range merged.Warnings {
		if d.Code == "W301" && !strings.Contains(d.Msg, "host") {
			t.Errorf("W301 does not name a label to group by: %q", d.Msg)
		}
	}

	// Grouped, nothing is dropped, so nothing is warned about.
	grouped := run(`FROM app SELECT cpu BY host`)
	if hasCode(grouped.Warnings, "W301") {
		t.Errorf("W301 on a query that dropped nothing; warnings were %v", diagCodes(grouped.Warnings))
	}
	if len(grouped.Series) != 2 {
		t.Fatalf("BY host produced %d series, want 2", len(grouped.Series))
	}
}

// A spec's `sets:` block travels once per ingest process, so a store that
// only held it in memory until the next thirty-second catalogue tick lost
// it to any unclean stop inside that window -- and the ingester, already
// running, never repeats it. The set is then kept forever in the
// unsharded shard the retention sweep skips, which is the exact failure
// SetRetentionFor's own comment describes preventing.
func TestDeclarationsArePersistedImmediately(t *testing.T) {
	s := reviewStore(t)
	ms := int64(3600_000)
	shard := int64(3600_000)
	req := &wire.WriteRequest{
		SetMeta:   []wire.SetMeta{{Set: "app", RetentionMs: &ms, ShardMs: &shard, KeyScheme: model.KeyOffset}},
		FieldMeta: []wire.FieldMeta{{Set: "app", Field: "cpu", Kind: model.KindCounter, Unit: "pct"}},
	}
	if _, err := s.Write(req, "", ""); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Read the record straight off the engine, without going through
	// Close: what survives a kill is exactly what is on disk now.
	b, ok, err := s.db.GetDict(catalogueDictKey)
	if err != nil || !ok {
		t.Fatalf("catalogue record: ok=%v err=%v", ok, err)
	}
	var stored struct {
		Sets map[string]*setEntry `json:"sets"`
	}
	if err := json.Unmarshal(b, &stored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	e := stored.Sets["app"]
	if e == nil {
		t.Fatal("the declared set is not in the persisted catalogue")
	}
	if e.RetentionMs == nil || *e.RetentionMs != ms {
		t.Errorf("persisted retention is %v, want %d", e.RetentionMs, ms)
	}
	if e.ShardMs == nil || *e.ShardMs != shard {
		t.Errorf("persisted shard width is %v, want %d", e.ShardMs, shard)
	}
	if e.KeyScheme != model.KeyOffset {
		t.Errorf("persisted key scheme is %q, want %q", e.KeyScheme, model.KeyOffset)
	}
	if f := e.Fields["cpu"]; f == nil || f.Kind != model.KindCounter || f.Unit != "pct" {
		t.Errorf("persisted field metadata is %+v, want a counter in pct", f)
	}
}

// A write that declares nothing new must not pay for a catalogue save, or
// the persist above would run on every batch an ingester sends.
func TestRepeatedDeclarationDoesNotResave(t *testing.T) {
	s := reviewStore(t)
	metas := []wire.FieldMeta{{Set: "app", Field: "cpu", Kind: model.KindGauge}}
	if _, err := s.Write(&wire.WriteRequest{FieldMeta: metas}, "", ""); err != nil {
		t.Fatalf("write: %v", err)
	}
	before := s.CatalogueVersion()
	if _, err := s.Write(&wire.WriteRequest{FieldMeta: metas}, "", ""); err != nil {
		t.Fatalf("write: %v", err)
	}
	if after := s.CatalogueVersion(); after != before {
		t.Errorf("re-declaring identical metadata moved the catalogue version from %d to %d", before, after)
	}
}
