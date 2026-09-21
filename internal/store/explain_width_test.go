package store

import (
	"encoding/json"
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

// The plan's lists travel as lists, for the reason QueryResponse.Series,
// LabelValues.Values and the catalogue's own `sets` and `labels` do: the
// builder's Explain button should not have to tell "none" apart from "no
// array".
func TestExplainListsAreNeverNull(t *testing.T) {
	s := openTestStore(t)
	q := &mql.Query{Kind: mql.KindQuery, From: "nosuchset", Select: []mql.FieldExpr{{Field: "v"}}}
	plan, err := s.Explain(q, &wire.QueryRequest{AST: q, MaxPoints: 100})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	body, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]json.RawMessage
	if err := json.Unmarshal(body, &back); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"warnings", "shards", "projection", "resolved_fields"} {
		if string(back[key]) == "null" {
			t.Errorf("%s travelled as null rather than as a list: %s", key, body)
		}
	}
}
