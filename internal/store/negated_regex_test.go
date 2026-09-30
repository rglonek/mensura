package store

import (
	"context"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// A negated regex that matches every known value of its label can never
// match a row, and it was the one arm of the lowering that said neither.
//
// `!~` excludes every row that carries the label, and a row that does not
// carry it fails the existence half -- so the clause is constant false.
// It lowered to And(Exists, Not(In(every value))), which is correct and
// costs a full read of every selected shard, and it emitted no
// diagnostic: the panel came back empty with nothing at all explaining
// it, which is the failure the W201 beside it exists to prevent. The
// positive form has always answered with a constant and a warning.
func TestANegatedRegexThatMatchesEverythingIsImpossible(t *testing.T) {
	s := openTestStore(t)
	ts := base()
	writeSamples(t, s, "app", []model.Sample{
		{TSMs: ts, Labels: map[string]string{"host": "web1"}, Fields: map[string]model.Value{"v": model.Int(1)}},
		{TSMs: ts + 1000, Labels: map[string]string{"host": "web2"}, Fields: map[string]model.Value{"v": model.Int(2)}},
	})

	run := func(text string) *wire.QueryResponse {
		t.Helper()
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
		return resp
	}

	resp := run(`FROM app SELECT v WHERE host !~ /^web/ BY host`)
	if resp.Stats.ShardsScanned != 0 || resp.Stats.RowsScanned != 0 {
		t.Errorf("a clause that can never match scanned %d shard(s) and %d row(s)",
			resp.Stats.ShardsScanned, resp.Stats.RowsScanned)
	}
	if len(resp.Series) != 0 {
		t.Errorf("returned %d series", len(resp.Series))
	}
	var said bool
	for _, w := range resp.Warnings {
		if w.Code == "W201" && strings.Contains(w.Msg, "can never match") {
			said = true
		}
	}
	if !said {
		t.Errorf("no W201 explained the empty panel: %v", resp.Warnings)
	}

	// A negated regex that matches only *some* values is an ordinary
	// filter and still runs.
	resp = run(`FROM app SELECT v WHERE host !~ /web1/ BY host`)
	if resp.Stats.ShardsScanned == 0 {
		t.Error("a partial negated regex read no shards, so it filtered nothing")
	}
	if len(resp.Series) != 1 {
		t.Errorf("returned %d series, want 1 (web2)", len(resp.Series))
	}

	// And one that matches nothing still folds to an existence test, so it
	// keeps every row that carries the label.
	resp = run(`FROM app SELECT v WHERE host !~ /nosuchhost/ BY host`)
	if len(resp.Series) != 2 {
		t.Errorf("returned %d series, want 2", len(resp.Series))
	}
}
