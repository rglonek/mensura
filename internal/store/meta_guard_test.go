package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

func floatPtr(v float64) *float64 { return &v }

// Declared limits become the default clamp, with the counter-reset escape
// hatch wired up, so a pair whose bounds cross replaces every rendered
// value with the raw sample rather than failing to bound anything. MQL
// refuses the same pair typed as CLAMP and extract.Compile refuses it in a
// spec; the write API was the one door left open.
func TestWriteRefusesCrossedFieldLimits(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Write(&wire.WriteRequest{
		FieldMeta: []wire.FieldMeta{{
			Set: "app", Field: "reqs", Kind: model.KindCounter,
			LimitMin: floatPtr(100), LimitMax: floatPtr(1),
		}},
	}, "", "test")
	if err == nil {
		t.Fatal("a crossed limit pair was accepted")
	}
	var bad *ErrBadRequest
	if !errors.As(err, &bad) {
		t.Fatalf("a bad declaration must come back as a client fault, got %T: %v", err, err)
	}
	if !strings.Contains(bad.Msg, "limit_min") {
		t.Fatalf("the rejection must name the offending key, got %q", bad.Msg)
	}
	// Nothing was applied: the validating pass runs before any of it.
	if _, known := s.Schema().Field("app", "reqs"); known {
		t.Fatal("a refused declaration must not reach the catalogue")
	}
	// The ordered pair is still accepted.
	if _, err := s.Write(&wire.WriteRequest{
		FieldMeta: []wire.FieldMeta{{
			Set: "app", Field: "reqs", LimitMin: floatPtr(0), LimitMax: floatPtr(100),
		}},
	}, "", "test"); err != nil {
		t.Fatalf("an ordered pair must still be accepted: %v", err)
	}
}

// A negative cadence matched neither arm of the merge switch, so it was
// accepted, stored as zero, and then reported by W103 as a field with no
// declared cadence at all.
func TestWriteRefusesNegativeMaxInterval(t *testing.T) {
	s := openTestStore(t)
	for _, m := range []wire.FieldMeta{
		{Set: "app", Field: "cpu", MaxIntervalMs: -5},
		{Set: "app", Field: "cpu", MaxIntervalS: -5},
	} {
		_, err := s.Write(&wire.WriteRequest{FieldMeta: []wire.FieldMeta{m}}, "", "test")
		var bad *ErrBadRequest
		if !errors.As(err, &bad) {
			t.Fatalf("max_interval %d/%d was accepted (%v)", m.MaxIntervalMs, m.MaxIntervalS, err)
		}
	}
}

// A crossed pair an earlier build already persisted must not install a
// clamp: with min above max every value falls outside the range, so the
// escape hatch substitutes the raw sample for every point and a RATE query
// draws the raw counter.
func TestACrossedCataloguePairInstallsNoClamp(t *testing.T) {
	s := openTestStore(t)
	const ts = int64(1_700_000_000_000)
	var samples []model.Sample
	for i := 0; i < 4; i++ {
		samples = append(samples, model.Sample{
			TSMs:   ts + int64(i)*1000,
			Labels: map[string]string{"host": "web1"},
			Fields: map[string]model.Value{"reqs": model.Int(int64(1000 + i*7))},
		})
	}
	writeSamples(t, s, "app", samples)
	// Planted the way a catalogue written before the check would hold it.
	s.mu.Lock()
	s.catalogue["app"].Fields["reqs"].Kind = model.KindCounter
	s.catalogue["app"].Fields["reqs"].LimitMin = floatPtr(100)
	s.catalogue["app"].Fields["reqs"].LimitMax = floatPtr(1)
	s.mu.Unlock()

	q, err := mql.Parse(`FROM app SELECT reqs RATE BY host`)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: ts - 60_000, ToMs: ts + 60_000, MaxPoints: 1000, IntervalMs: 1000,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Series) != 1 {
		t.Fatalf("expected one series, got %d", len(resp.Series))
	}
	for i, v := range resp.Series[0].Values {
		if v > 100 {
			t.Fatalf("point %d is %v: the crossed clamp substituted the raw counter for the rate", i, v)
		}
	}
}

// A regex that matches no dictionary value folds to a constant false
// exactly as an equality on an unknown value does, so it must skip the
// shards rather than open each one and walk it with a filter that can
// never be true.
func TestAnImpossibleRegexReadsNoShards(t *testing.T) {
	s := openTestStore(t)
	const ts = int64(1_700_000_000_000)
	writeSamples(t, s, "app", []model.Sample{{
		TSMs:   ts,
		Labels: map[string]string{"host": "web1"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}})
	for _, text := range []string{
		`FROM app SELECT v WHERE host =~ /nosuchhost/`,
		`FROM app SELECT v WHERE host =~ /nosuchhost/ OR host = "gone"`,
	} {
		q, err := mql.Parse(text)
		if err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
		resp, err := s.Query(context.Background(), &wire.QueryRequest{
			AST: q, FromMs: ts - 60_000, ToMs: ts + 60_000, MaxPoints: 100, IntervalMs: 1000,
		})
		if err != nil {
			t.Fatalf("query %q: %v", text, err)
		}
		if resp.Stats.ShardsScanned != 0 {
			t.Fatalf("%q scanned %d shard(s); a predicate no value can satisfy reads none", text, resp.Stats.ShardsScanned)
		}
		if len(resp.Series) != 0 {
			t.Fatalf("%q returned %d series", text, len(resp.Series))
		}
	}
	// A negated match is not impossible: it lowers to an existence test.
	q, err := mql.Parse(`FROM app SELECT v WHERE host !~ /nosuchhost/`)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: ts - 60_000, ToMs: ts + 60_000, MaxPoints: 100, IntervalMs: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Series) != 1 {
		t.Fatalf("a negated match that names no value still matches rows, got %d series", len(resp.Series))
	}
}
