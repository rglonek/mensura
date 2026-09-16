package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// A field's declared limits become the query layer's default CLAMP with
// the counter-reset escape hatch wired up, and that hatch substitutes the
// *raw* sample -- which 07-downsampling.md section 3 stage 6 says "undoes
// both DELTA and NEGATE".
//
// So on the single most ordinary declaration there is, `limits: {min: 0}`
// on a counter, a query that asked for the mirror image got the original:
// `SELECT tx NEGATE` drew the counter the right way up, and
// `SELECT tx RATE NEGATE` -- the mirrored-axis panel NEGATE exists for --
// drew the raw counter instead of the rate, three orders of magnitude
// out, with no diagnostic anywhere.
func TestNegateIsNotUndoneByDeclaredLimits(t *testing.T) {
	s := openTestStore(t)
	zero := 0.0
	var rows []model.Sample
	for i := 0; i < 6; i++ {
		rows = append(rows, model.Sample{
			TSMs:   base() + int64(i)*1000,
			Labels: map[string]string{"host": "web1"},
			Fields: map[string]model.Value{"tx": model.Float(float64(1000 + i*100))},
		})
	}
	writeSamples(t, s, "app", rows, wire.FieldMeta{
		Set: "app", Field: "tx", Kind: model.KindCounter, LimitMin: &zero,
	})

	run := func(text string) []float64 {
		t.Helper()
		q, err := mql.Parse(text)
		if err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
		resp, err := s.Query(context.Background(), &wire.QueryRequest{
			AST: q, FromMs: base() - 1000, ToMs: base() + 10_000,
			MaxPoints: 1000, IntervalMs: 1,
		})
		if err != nil {
			t.Fatalf("query %q: %v", text, err)
		}
		if len(resp.Series) != 1 {
			t.Fatalf("%q: %d series, want 1", text, len(resp.Series))
		}
		return resp.Series[0].Values
	}

	for _, v := range run(`FROM app SELECT tx NEGATE BY host`) {
		if v > 0 {
			t.Fatalf("NEGATE drew %g: the declared limits substituted the raw sample and undid the modifier", v)
		}
	}
	for _, v := range run(`FROM app SELECT tx RATE NEGATE BY host`) {
		// Every delta is exactly -100 per second here, so anything else
		// is the raw counter arriving in its place.
		if v != -100 {
			t.Fatalf("RATE NEGATE drew %g, want -100: the default clamp replaced the rate with the raw counter", v)
		}
	}
	// The plain rate is unaffected: the hatch still exists for the case it
	// was built for.
	for _, v := range run(`FROM app SELECT tx RATE BY host`) {
		if v != 100 {
			t.Fatalf("RATE drew %g, want 100", v)
		}
	}
	// And an explicit CLAMP still wins over the declaration, negated or
	// not.
	for _, v := range run(`FROM app SELECT tx NEGATE CLAMP MIN -50 ELSE BOUND BY host`) {
		if v != -50 {
			t.Fatalf("an explicit CLAMP was not applied: got %g, want -50", v)
		}
	}
}

// Retention removes a shard with one range delete, which walks no rows --
// so nothing could raise the observed start of a set whose oldest data it
// took. FirstTSMs is only ever lowered by observeSet, so the catalogue
// went on advertising a start time with nothing under it: the number the
// query builder offers as "all data" and the plugin's health check
// prints.
func TestRetentionRaisesTheObservedStart(t *testing.T) {
	s := openTestStore(t)
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	recent := now.Add(-1 * time.Hour)
	s.SetRetentionFor("app", 7*24*time.Hour, 24*time.Hour)
	writeSamples(t, s, "app", []model.Sample{
		{TSMs: old.UnixMilli(), Fields: map[string]model.Value{"v": model.Int(1)}},
		{TSMs: recent.UnixMilli(), Fields: map[string]model.Value{"v": model.Int(2)}},
	})
	if got := s.Catalogue().Sets[0].FirstTSMs; got != old.UnixMilli() {
		t.Fatalf("first_ts_ms is %d before the sweep, want %d", got, old.UnixMilli())
	}
	if _, err := s.RunRetention(now); err != nil {
		t.Fatalf("retention: %v", err)
	}
	cat := s.Catalogue()
	if len(cat.Sets) != 1 {
		t.Fatalf("%d set(s) after the sweep, want 1", len(cat.Sets))
	}
	first := cat.Sets[0].FirstTSMs
	if first == old.UnixMilli() {
		t.Fatal("first_ts_ms still names an instant whose shard has been range-deleted")
	}
	if first > recent.UnixMilli() {
		t.Fatalf("first_ts_ms %d is past the oldest surviving sample %d", first, recent.UnixMilli())
	}
	// The floor is the oldest surviving shard's own start, so the reported
	// range still contains every row that is left.
	if resp := query(t, s, `FROM app SELECT v`, first, now.UnixMilli()); len(resp.Series) != 1 {
		t.Fatalf("the advertised range holds no data: %d series", len(resp.Series))
	}
}

// applySetMeta converts a declared retention with
// `time.Duration(ms) * time.Millisecond`, which overflows past ~292 years:
// the wrapped value is usually negative, so SetRetentionFor's own sign
// tests drop it and the set silently keeps the store's defaults.
func TestDeclaredDurationBeyondWhatADurationHoldsIsRefused(t *testing.T) {
	s := openTestStore(t)
	over := maxDeclaredDurationMs + 1
	_, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{Set: "app", RetentionMs: &over}}}, "", "test")
	if err == nil {
		t.Fatal("a retention a duration cannot hold was accepted")
	}
	if !strings.Contains(err.Error(), "beyond") {
		t.Fatalf("unexpected error: %v", err)
	}
	var bad *ErrBadRequest
	if !errors.As(err, &bad) {
		t.Fatalf("a client-input fault must be a 4xx, got %v", err)
	}
	_, err = s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{Set: "app", ShardMs: &over}}}, "", "test")
	if err == nil {
		t.Fatal("a shard width a duration cannot hold was accepted")
	}
	// The largest value that does fit is still accepted.
	ok := maxDeclaredDurationMs
	if _, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{{Set: "app", RetentionMs: &ok}}}, "", "test"); err != nil {
		t.Fatalf("the largest representable retention was refused: %v", err)
	}
}
