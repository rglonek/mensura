package store

import (
	"context"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// The "this predicate can never match" verdict is now derived from the
// lowering itself rather than from a second walk over the AST, which used
// to recompile every regex and sweep the whole label dictionary again.
// The verdict must be unchanged in every direction: a conjunction is
// impossible as soon as one arm is, a disjunction only when every arm is,
// and a negation never is.
func TestImpossiblePredicateVerdictPerShape(t *testing.T) {
	s := openTestStore(t)
	ts := base()
	writeSamples(t, s, "app", []model.Sample{{
		TSMs:   ts,
		Labels: map[string]string{"host": "web1", "pool": "default"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}})

	for _, tc := range []struct {
		text       string
		wantShards int
		wantSeries int
	}{
		// One impossible arm makes the whole conjunction impossible.
		{`FROM app SELECT v WHERE host = "web1" AND pool = "gone"`, 0, 0},
		{`FROM app SELECT v WHERE pool =~ /nosuch/ AND host = "web1"`, 0, 0},
		// A disjunction survives while any arm can match.
		{`FROM app SELECT v WHERE host = "gone" OR host = "web1"`, 1, 1},
		{`FROM app SELECT v WHERE host =~ /nosuch/ OR pool = "default"`, 1, 1},
		// Every arm impossible, so the disjunction is too.
		{`FROM app SELECT v WHERE host = "gone" OR pool =~ /nosuch/`, 0, 0},
		// A negation of something that can never match is something that
		// always does, so it is not impossible -- and it still has to run.
		{`FROM app SELECT v WHERE NOT host = "gone"`, 1, 1},
		{`FROM app SELECT v WHERE NOT (host =~ /nosuch/)`, 1, 1},
		// An inequality lowers to an existence test, never to a constant.
		{`FROM app SELECT v WHERE host != "gone"`, 1, 1},
		// Nested: an impossible conjunction inside a live disjunction.
		{`FROM app SELECT v WHERE (host = "gone" AND pool = "default") OR host = "web1"`, 1, 1},
	} {
		q, err := mql.Parse(tc.text)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.text, err)
		}
		resp, err := s.Query(context.Background(), &wire.QueryRequest{
			AST: q, FromMs: ts - 60_000, ToMs: ts + 60_000, MaxPoints: 100, IntervalMs: 1000,
		})
		if err != nil {
			t.Fatalf("query %q: %v", tc.text, err)
		}
		if resp.Stats.ShardsScanned != tc.wantShards {
			t.Errorf("%q scanned %d shard(s), want %d", tc.text, resp.Stats.ShardsScanned, tc.wantShards)
		}
		if len(resp.Series) != tc.wantSeries {
			t.Errorf("%q returned %d series, want %d", tc.text, len(resp.Series), tc.wantSeries)
		}
	}
}
