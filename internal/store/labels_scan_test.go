package store

import (
	"context"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// The LABELS filter scan lowers its predicate once, not once per set.
//
// There is one label-value dictionary per key for the whole store
// (ADR-004), so the lowering does not depend on the set it is about to be
// scanned against -- buildExpr took a set and never read it. The
// expensive half of a lowering does not depend on it either: a regex
// clause is evaluated against every value of the key, up to
// max_label_cardinality, so repeating it per set multiplied a dashboard
// variable's cost by the number of sets carrying the label. This checks
// the answer is the same one, which is what the hoist must not change.
func TestLabelFilterAcrossSetsAnswersOnce(t *testing.T) {
	s := openTestStore(t)
	ts := int64(1_700_000_000_000)
	for i, set := range []string{"a", "b", "c"} {
		writeSamples(t, s, set, []model.Sample{
			{
				TSMs:   ts + int64(i),
				Labels: map[string]string{"host": "web1", "dc": "eu"},
				Fields: map[string]model.Value{"v": model.Int(1)},
			},
			{
				TSMs:   ts + int64(i) + 100,
				Labels: map[string]string{"host": "db1", "dc": "us"},
				Fields: map[string]model.Value{"v": model.Int(2)},
			},
		})
	}
	resp := labelValues(t, s, `LABELS host WHERE dc =~ /^e/`, ts-1000, ts+1000)
	if len(resp) != 1 || resp[0] != "web1" {
		t.Fatalf("values = %v, want [web1]", resp)
	}
	// The unfiltered form reads the dictionary rather than the rows, and
	// the two must agree about what exists.
	all := labelValues(t, s, `LABELS host`, ts-1000, ts+1000)
	if len(all) != 2 {
		t.Fatalf("unfiltered values = %v, want two", all)
	}
}

// A clause that folds away is reported once, whatever the set count.
func TestLabelFilterFoldIsReportedOnce(t *testing.T) {
	s := openTestStore(t)
	ts := int64(1_700_000_000_000)
	for _, set := range []string{"a", "b", "c"} {
		writeSamples(t, s, set, []model.Sample{{
			TSMs:   ts,
			Labels: map[string]string{"host": "web1", "dc": "eu"},
			Fields: map[string]model.Value{"v": model.Int(1)},
		}})
	}
	q, err := mql.Parse(`LABELS host WHERE dc =~ /.*/`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: ts - 1000, ToMs: ts + 1000, MaxPoints: 100,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	n := 0
	for _, w := range resp.Warnings {
		if w.Code == "W202" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("W202 appeared %d time(s) for one clause: %v", n, resp.Warnings)
	}
}

func labelValues(t *testing.T, s *Store, text string, from, to int64) []string {
	t.Helper()
	q, err := mql.Parse(text)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: from, ToMs: to, MaxPoints: 100,
	})
	if err != nil {
		t.Fatalf("query %q: %v", text, err)
	}
	out := make([]string, 0, len(resp.Rows))
	for _, r := range resp.Rows {
		if len(r.Values) > 0 {
			if v, ok := r.Values[0].(string); ok {
				out = append(out, v)
			}
		}
	}
	return out
}
