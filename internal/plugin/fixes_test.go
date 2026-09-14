package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// A table or logs query that matched nothing still describes a table. It
// used to produce no frame at all, so Grafana drew "No data" where the
// honest answer is the declared columns with no rows under them -- and
// the difference matters while a filter is being narrowed, because an
// empty table says the query ran and "No data" says nothing.
func TestEmptyTableStillProducesItsColumns(t *testing.T) {
	resp := &wire.QueryResponse{
		Series: []wire.Series{},
		Columns: []wire.Column{
			{Name: "time", Type: "time"},
			{Name: "host", Type: "string"},
			{Name: "latency", Type: "number"},
		},
	}
	frames := toFrames(resp, "FROM app SELECT latency FORMAT table")
	if len(frames) != 1 {
		t.Fatalf("want one frame for a declared-but-empty table, got %d", len(frames))
	}
	f := frames[0]
	if len(f.Fields) != 3 {
		t.Fatalf("want 3 columns, got %d", len(f.Fields))
	}
	for i, want := range []string{"time", "host", "latency"} {
		if f.Fields[i].Name != want {
			t.Fatalf("column %d is %q, want %q", i, f.Fields[i].Name, want)
		}
		if f.Fields[i].Len() != 0 {
			t.Fatalf("column %q carries %d rows, want none", want, f.Fields[i].Len())
		}
	}
}

// A timeseries response declares no columns, so it must still produce
// exactly one frame per series and no table frame beside them.
func TestTimeseriesProducesNoTableFrame(t *testing.T) {
	resp := &wire.QueryResponse{
		Series: []wire.Series{{
			Name: "a", TSMs: []int64{1000, 2000},
			Values: []float64{1, 2}, IsNull: []bool{false, false},
		}},
	}
	if got := len(toFrames(resp, "FROM app SELECT v")); got != 1 {
		t.Fatalf("want one frame for one series, got %d", got)
	}
}

// Proxy-mode validation must not be a round trip per keystroke.
//
// The query editor calls the "parse" resource on every character typed,
// and proxy-mode Parse needs the upstream catalogue and the upstream
// ceilings. Fetching both every time cost two HTTP requests per keystroke
// -- and rendering a catalogue walks every shard on the far side -- which
// is the cost CallResource stopped paying by not fetching the catalogue
// for "parse" itself, one layer further in.
func TestProxyParseDoesNotRefetchMetadataPerKeystroke(t *testing.T) {
	var catalogues, hellos int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/catalogue":
			atomic.AddInt32(&catalogues, 1)
			_ = json.NewEncoder(w).Encode(wire.Catalogue{Sets: []wire.SetInfo{{
				Name:   "app",
				Fields: map[string]mql.FieldInfo{"v": {Kind: "gauge"}},
				Labels: []string{"host"},
			}}})
		case "/v1/hello":
			atomic.AddInt32(&hellos, 1)
			_ = json.NewEncoder(w).Encode(wire.Hello{Product: "mensura-store", Protocol: wire.ProtocolVersion})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	svc := &remoteService{client: wire.NewClient(srv.URL, "")}
	for _, text := range []string{
		`FROM app SELECT v`,
		`FROM app SELECT v WHERE host = "a"`,
		`FROM app SELECT v WHERE host = "ab"`,
	} {
		if _, _, err := svc.Parse(context.Background(), text); err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
	}
	if n := atomic.LoadInt32(&catalogues); n != 1 {
		t.Fatalf("fetched the catalogue %d times for three keystrokes", n)
	}
	if n := atomic.LoadInt32(&hellos); n != 1 {
		t.Fatalf("fetched hello %d times for three keystrokes", n)
	}
	// The cache is a TTL, not a one-shot: it refreshes once it expires.
	svc.mu.Lock()
	svc.catAt = svc.catAt.Add(-2 * metadataTTL)
	svc.helloAt = svc.helloAt.Add(-2 * metadataTTL)
	svc.mu.Unlock()
	if _, _, err := svc.Parse(context.Background(), `FROM app SELECT v`); err != nil {
		t.Fatalf("parse after expiry: %v", err)
	}
	if n := atomic.LoadInt32(&catalogues); n != 2 {
		t.Fatalf("the catalogue was not refetched after the TTL expired: %d fetches", n)
	}
}
