package store

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// /v1/query bounds a predicate's depth through mql.Validate and /v1/print
// bounds it explicitly, but /v1/debug/plan ran the planner's own
// recursive lowering on an AST nothing had checked. An AST arrives as
// JSON, whose decoder allows far deeper nesting than anything downstream
// is written for.
func TestDebugPlanBoundsPredicateDepth(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{})
	h := api.DebugHandler()

	deep := mql.Expr{Eq: &mql.Compare{Label: "host", Value: "web1"}}
	for i := 0; i < mql.MaxPredicateDepth+8; i++ {
		inner := deep
		deep = mql.Expr{And: []mql.Expr{inner}}
	}
	body, err := json.Marshal(&wire.QueryRequest{
		AST: &mql.Query{From: "http", Select: []mql.FieldExpr{{Field: "v"}}, Where: deep},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/v1/debug/plan", strings.NewReader(string(body)))
	req.RemoteAddr = "127.0.0.1:5000"
	rec := newRecorder()
	h.ServeHTTP(rec, req)
	if rec.code != 400 {
		t.Fatalf("a predicate past the depth limit was planned: status %d, body %s", rec.code, rec.body.String())
	}
	if !strings.Contains(rec.body.String(), "nests deeper") {
		t.Errorf("the refusal does not name the reason: %s", rec.body.String())
	}

	// A predicate inside the bound still plans.
	shallow, err := json.Marshal(&wire.QueryRequest{
		AST: &mql.Query{
			From:   "http",
			Select: []mql.FieldExpr{{Field: "v"}},
			Where:  mql.Expr{Eq: &mql.Compare{Label: "host", Value: "web1"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest("POST", "/v1/debug/plan", strings.NewReader(string(shallow)))
	req.RemoteAddr = "127.0.0.1:5000"
	rec = newRecorder()
	h.ServeHTTP(rec, req)
	if rec.code != 200 {
		t.Fatalf("an ordinary predicate was refused: status %d, body %s", rec.code, rec.body.String())
	}
}
