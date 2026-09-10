package store

import (
	"context"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// The LABELS filter scan lowers its predicate once per set carrying the
// label, and the predicate resolves against one dictionary for the whole
// store, so every set produced the identical "no values match" warning: a
// dashboard variable over a store with a dozen sets came back with a
// dozen copies of one sentence.
func TestLabelFilterWarningsAreNotRepeated(t *testing.T) {
	s := openTestStore(t)
	ts := int64(1_700_000_000_000)
	for _, set := range []string{"a", "b", "c"} {
		writeSamples(t, s, set, []model.Sample{{
			TSMs:   ts,
			Labels: map[string]string{"host": "web1", "pool": "main"},
			Fields: map[string]model.Value{"v": model.Int(1)},
		}})
	}
	q, err := mql.Parse(`LABELS host WHERE pool = "nosuchvalue"`)
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
		if w.Code == "W201" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("W201 appeared %d time(s) for one clause: %v", n, resp.Warnings)
	}
}
