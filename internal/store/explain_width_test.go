package store

import (
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// Explain does not run mql.Validate -- a half-built query has to explain
// -- so the bounds Validate applies have to be taken here. plan() resolves
// every selected field against the catalogue under a read lock and
// length-prefixes every BY slot, so an unbounded width is unbounded locked
// work on an endpoint the query editor calls freely.
func TestExplainBoundsQueryWidth(t *testing.T) {
	s := openTestStore(t)
	wide := &mql.Query{Kind: mql.KindQuery, From: "app"}
	for i := 0; i <= mql.MaxSelectFields; i++ {
		wide.Select = append(wide.Select, mql.FieldExpr{Field: "v"})
	}
	_, err := s.Explain(wide, &wire.QueryRequest{AST: wide, MaxPoints: 100})
	if err == nil {
		t.Fatal("Explain accepted a query past MaxSelectFields")
	}
	if !strings.Contains(err.Error(), "fields") {
		t.Fatalf("error %q does not name the fields", err)
	}

	byWide := &mql.Query{Kind: mql.KindQuery, From: "app", Select: []mql.FieldExpr{{Field: "v"}}}
	for i := 0; i <= mql.MaxByLabels; i++ {
		byWide.By = append(byWide.By, "host")
	}
	_, err = s.Explain(byWide, &wire.QueryRequest{AST: byWide, MaxPoints: 100})
	if err == nil {
		t.Fatal("Explain accepted a query past MaxByLabels")
	}
	if !strings.Contains(err.Error(), "labels") {
		t.Fatalf("error %q does not name the labels", err)
	}

	// An ordinary query still explains.
	ok := &mql.Query{Kind: mql.KindQuery, From: "app", Select: []mql.FieldExpr{{Field: "v"}}, By: []string{"host"}}
	if _, err := s.Explain(ok, &wire.QueryRequest{AST: ok, MaxPoints: 100}); err != nil {
		t.Fatalf("an ordinary query failed to explain: %v", err)
	}
}
