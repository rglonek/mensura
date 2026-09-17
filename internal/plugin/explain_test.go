package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/rglonek/mensura/internal/engine"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// stubService is a QueryService whose answers the tests choose.
type stubService struct {
	plan      map[string]any
	planErr   error
	stats     engine.StatsSnapshot
	hasEngine bool
	cat       wire.Catalogue
}

func (s *stubService) Query(context.Context, *wire.QueryRequest) (*wire.QueryResponse, error) {
	return &wire.QueryResponse{}, nil
}
func (s *stubService) Catalogue(context.Context) (wire.Catalogue, error) { return s.cat, nil }
func (s *stubService) LabelValues(context.Context, string) ([]string, error) {
	return []string{}, nil
}
func (s *stubService) Hello(context.Context) (wire.Hello, error) {
	return wire.Hello{Version: "test", Mode: "plugin", Protocol: wire.ProtocolVersion}, nil
}
func (s *stubService) Parse(_ context.Context, text string) (*mql.Query, []mql.Diag, error) {
	q, d, err := mql.ParseDiags(text)
	return q, d, err
}
func (s *stubService) Explain(context.Context, *wire.QueryRequest) (map[string]any, error) {
	return s.plan, s.planErr
}
func (s *stubService) EngineStats() (engine.StatsSnapshot, bool) { return s.stats, s.hasEngine }

type capture struct {
	status int
	body   []byte
}

func (c *capture) Send(r *backend.CallResourceResponse) error {
	c.status, c.body = r.Status, r.Body
	return nil
}

// docs/design/08-plugin.md section 2.3 lists `POST /explain` beside parse
// and print. It was the one entry in that table with nothing behind it,
// so the builder's Explain button answered 404 "unknown resource".
func TestExplainResourceReturnsThePlan(t *testing.T) {
	svc := &stubService{plan: map[string]any{"shards": []string{"app@20231114"}}}
	ds := &Datasource{svc: svc}
	body, _ := json.Marshal(&wire.QueryRequest{
		AST: mustParse(t, "FROM app SELECT cpu"), FromMs: 1, ToMs: 2, MaxPoints: 10,
	})
	c := &capture{}
	if err := ds.CallResource(context.Background(), &backend.CallResourceRequest{Path: "explain", Body: body}, c); err != nil {
		t.Fatal(err)
	}
	if c.status != http.StatusOK {
		t.Fatalf("status %d: %s", c.status, c.body)
	}
	if !strings.Contains(string(c.body), "app@20231114") {
		t.Fatalf("plan did not come back: %s", c.body)
	}
}

func TestExplainResourceNeedsAnAST(t *testing.T) {
	ds := &Datasource{svc: &stubService{}}
	c := &capture{}
	if err := ds.CallResource(context.Background(), &backend.CallResourceRequest{Path: "explain", Body: []byte(`{}`)}, c); err != nil {
		t.Fatal(err)
	}
	if c.status != http.StatusBadRequest {
		t.Fatalf("status %d: %s", c.status, c.body)
	}
}

// A proxy-mode datasource has no local engine, and the store's plan
// endpoint lives on its loopback-only debug listener, so saying so beats
// inventing a plan on this side.
func TestProxyExplainSaysThereIsNoLocalEngine(t *testing.T) {
	r := &remoteService{}
	if _, err := r.Explain(context.Background(), &wire.QueryRequest{}); err != ErrNoLocalEngine {
		t.Fatalf("want ErrNoLocalEngine, got %v", err)
	}
	if _, ok := r.EngineStats(); ok {
		t.Fatal("a proxy reported engine statistics it cannot have")
	}
}

// Section 2.2 says CheckHealth reports the engine's disk usage and open
// iterator count in embedded mode. It never did; a proxy still says
// nothing rather than reporting zeroes that read like an idle store.
func TestCheckHealthReportsEngineStatsWhenEmbedded(t *testing.T) {
	svc := &stubService{hasEngine: true, stats: engine.StatsSnapshot{DiskBytes: 3 << 20, OpenIterators: 2}}
	ds := &Datasource{svc: svc}
	res, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Message, "3.0 MiB on disk") || !strings.Contains(res.Message, "2 open iterator") {
		t.Fatalf("health message omits the engine statistics: %s", res.Message)
	}

	svc.hasEngine = false
	res, err = ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Message, "on disk") {
		t.Fatalf("a proxy reported disk usage: %s", res.Message)
	}
}

func mustParse(t *testing.T, text string) *mql.Query {
	t.Helper()
	q, err := mql.Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	return q
}
