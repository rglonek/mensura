package plugin

import (
	"testing"

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
