package store

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// max_label_cardinality bounds the values under one label key; nothing
// bounded the number of *keys*. Every key allocates its own dictionary,
// is persisted into a catalogue written as one JSON record, and is never
// reclaimed, so a sender choosing its own label names -- which the line
// protocol, /ingest/v1/samples and /v1/write all allow -- could grow the
// store without limit.
func TestNewLabelKeysAreBounded(t *testing.T) {
	s := openTuned(t, func(c *Config) { c.MaxLabelKeys = 4 })
	now := time.Now().UnixMilli()
	accepted, refused := 0, 0
	var reason string
	for i := 0; i < 10; i++ {
		resp := put(t, s, "app", model.Sample{
			TSMs:   now + int64(i),
			Labels: map[string]string{fmt.Sprintf("k%d", i): "v"},
			Fields: map[string]model.Value{"v": model.Int(1)},
		})
		accepted += resp.Accepted
		if n := resp.Refused(); n > 0 {
			refused += n
			if reason == "" && len(resp.Rejected) > 0 {
				reason = resp.Rejected[0].Reason
			}
		}
	}
	if accepted != 4 {
		t.Fatalf("accepted %d samples, want the 4 that fit the key budget", accepted)
	}
	if refused != 6 {
		t.Fatalf("refused %d samples, want 6", refused)
	}
	if !strings.Contains(reason, "k4") || !strings.Contains(reason, "distinct label keys") {
		t.Fatalf("the rejection does not name the key and the limit: %q", reason)
	}
}

// A key the store already holds keeps working once the budget is full:
// the bound is on growth, not on use.
func TestAKnownLabelKeyStillWorksAtTheLimit(t *testing.T) {
	s := openTuned(t, func(c *Config) { c.MaxLabelKeys = 1 })
	now := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		resp := put(t, s, "app", model.Sample{
			TSMs:   now + int64(i),
			Labels: map[string]string{"host": fmt.Sprintf("h%d", i)},
			Fields: map[string]model.Value{"v": model.Int(int64(i))},
		})
		if resp.Accepted != 1 {
			t.Fatalf("sample %d was refused under a key the store already holds: %+v", i, resp.Rejected)
		}
	}
}

// A zero means "use the default"; a negative value switches the gate off,
// the way every other limit here is switched off.
func TestLabelKeyBudgetCanBeDisabled(t *testing.T) {
	s := openTuned(t, func(c *Config) { c.MaxLabelKeys = -1 })
	now := time.Now().UnixMilli()
	for i := 0; i < 50; i++ {
		resp := put(t, s, "app", model.Sample{
			TSMs:   now + int64(i),
			Labels: map[string]string{fmt.Sprintf("k%d", i): "v"},
			Fields: map[string]model.Value{"v": model.Int(1)},
		})
		if resp.Accepted != 1 {
			t.Fatalf("a disabled key budget still refused sample %d: %+v", i, resp.Rejected)
		}
	}
	if got := DefaultConfig().MaxLabelKeys; got <= 0 {
		t.Fatalf("the documented default is a positive bound, got %d", got)
	}
}

// Print returns a string rather than an error, so /v1/print was the one
// endpoint that would walk a predicate deeper than both the parser and
// the validator refuse -- rebuilding the whole sub-expression at every
// level of a body the JSON decoder allows ten thousand levels of.
func TestPrintRefusesAPredicateDeeperThanTheGrammar(t *testing.T) {
	s := openTuned(t, nil)
	api := NewAPI(s, APIConfig{})
	deep := mql.Expr{Eq: &mql.Compare{Label: "host", Value: "a"}}
	for i := 0; i < mql.MaxPredicateDepth+5; i++ {
		deep = mql.Expr{And: []mql.Expr{deep}}
	}
	body, err := json.Marshal(&mql.Query{
		Kind: mql.KindQuery, From: "app",
		Select: []mql.FieldExpr{{Field: "v"}}, Where: deep,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/print", strings.NewReader(string(body)))
	api.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400; body %s", rec.Code, rec.Body.String())
	}
	var out wire.APIError
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Code != "E001" {
		t.Fatalf("code %q, want E001 (%s)", out.Code, out.Error)
	}
}

// An ordinary query still prints, including a half-built one with no
// SELECT: the builder round-trips one of those on every edit.
func TestPrintStillAnswersAnOrdinaryQuery(t *testing.T) {
	s := openTuned(t, nil)
	api := NewAPI(s, APIConfig{})
	for _, q := range []*mql.Query{
		{Kind: mql.KindQuery, From: "app", Select: []mql.FieldExpr{{Field: "v"}},
			Where: mql.Expr{Eq: &mql.Compare{Label: "host", Value: "a"}}},
		{Kind: mql.KindQuery, From: "app"},
	} {
		body, err := json.Marshal(q)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rec := httptest.NewRecorder()
		api.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/print", strings.NewReader(string(body))))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d for an ordinary query; body %s", rec.Code, rec.Body.String())
		}
	}
}
