package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// A name this set already holds as a label may not arrive as a field, and
// the other way round. Each sample is internally consistent, so rowFor's
// within-sample rule accepts both -- and the two then land in the same
// column of the same shard, one as a dictionary index and one as a
// measurement.
func TestColumnNamespaceCollisionAcrossSamples(t *testing.T) {
	s := openTestStore(t)
	now := base()
	writeSamples(t, s, "app", []model.Sample{{
		TSMs:   now,
		Labels: map[string]string{"host": "a", "status": "ok"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}})

	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "app", Samples: []model.Sample{{
		TSMs:   now + 1000,
		Labels: map[string]string{"host": "a"},
		Fields: map[string]model.Value{"status": model.Int(503), "v": model.Int(2)},
	}}}}}, "", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.Refused() != 1 {
		t.Fatalf("a sample carrying a label name as a field was accepted: %+v", resp)
	}
	if !strings.Contains(resp.Rejected[0].Reason, `"status"`) {
		t.Fatalf("the rejection does not name the column: %q", resp.Rejected[0].Reason)
	}

	// And the mirror: a field name arriving as a label.
	writeSamples(t, s, "other", []model.Sample{{
		TSMs:   now,
		Labels: map[string]string{"host": "a"},
		Fields: map[string]model.Value{"code": model.Int(7)},
	}})
	resp, err = s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "other", Samples: []model.Sample{{
		TSMs:   now + 1000,
		Labels: map[string]string{"host": "a", "code": "seven"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}}}}}, "", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.Refused() != 1 {
		t.Fatalf("a sample carrying a field name as a label was accepted: %+v", resp)
	}

	// The catalogue must not list the name under both.
	for _, si := range s.Catalogue().Sets {
		fields := make([]string, 0, len(si.Fields))
		for f := range si.Fields {
			fields = append(fields, f)
		}
		for _, l := range si.Labels {
			if _, dup := si.Fields[l]; dup {
				t.Fatalf("set %s lists %q as a label and as a field (fields %v)", si.Name, l, fields)
			}
		}
	}
}

// A `fields:` block may perfectly well declare a unit or a description
// for a name the profile also lists under `labels:`. That creates a
// catalogue Fields entry for a column no row carries as a field, and it
// must not make every sample the profile produces look like a collision.
func TestDeclaredFieldIsNotEvidenceOfAFieldColumn(t *testing.T) {
	s := openTestStore(t)
	now := base()
	if err := s.applyFieldMeta([]wire.FieldMeta{
		{Set: "app", Field: "status", Unit: "state", Description: "the request status"},
	}); err != nil {
		t.Fatalf("declare: %v", err)
	}
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "app", Samples: []model.Sample{{
		TSMs:   now,
		Labels: map[string]string{"host": "a", "status": "ok"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}}}}}, "", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.Accepted != 1 || resp.Refused() != 0 {
		t.Fatalf("a declared-but-never-observed field blocked a label of the same name: %+v", resp.Rejected)
	}
}

// The catalogue is only updated once a shard has committed, so the check
// also has to see the classifications the samples earlier in this very
// request used.
func TestColumnNamespaceCollisionWithinOneBatch(t *testing.T) {
	s := openTestStore(t)
	now := base()
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{Set: "app", Samples: []model.Sample{
		{TSMs: now, Labels: map[string]string{"status": "ok"}, Fields: map[string]model.Value{"v": model.Int(1)}},
		{TSMs: now + 1, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"status": model.Int(503)}},
	}}}}, "", "test")
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.Accepted != 1 || resp.Refused() != 1 {
		t.Fatalf("expected one accepted and one refused, got %+v", resp)
	}
}

// A store that already holds a mixed column cannot be repaired by a
// write, so the query layer has to say so.
func TestMixedColumnWarnsAtQueryTime(t *testing.T) {
	s := openTestStore(t)
	// The shape an earlier build could persist: one name under both.
	s.mu.Lock()
	e := s.entryLocked("app")
	e.Labels["status"] = struct{}{}
	e.Fields["status"] = &fieldEntry{}
	e.Fields["v"] = &fieldEntry{}
	s.mu.Unlock()
	s.catVer.Add(1)

	for _, text := range []string{
		`FROM app SELECT status`,
		`FROM app SELECT v BY status`,
	} {
		q, err := mql.Parse(text)
		if err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
		warns, verr := mql.Validate(q, s.Schema(), 0, 0)
		if verr != nil {
			t.Fatalf("validate %q: %v", text, verr)
		}
		found := false
		for _, w := range warns {
			if w.Code == "W105" {
				found = true
			}
		}
		if !found {
			t.Fatalf("%q: no W105 for a column that is both a label and a field: %+v", text, warns)
		}
	}
}

// A heatmap cell that the datapoint gate creates and then trips on holds
// no points, and its three parallel arrays must still travel as lists.
func TestHeatmapGateDoesNotEmitNullArrays(t *testing.T) {
	s := openTestStore(t)
	meta := []wire.FieldMeta{
		{Set: "h", Field: "b0", BucketSet: "hdr", BucketIndex: 0, BucketEdge: 0},
		{Set: "h", Field: "b1", BucketSet: "hdr", BucketIndex: 1, BucketEdge: 1},
	}
	now := base()
	var samples []model.Sample
	for i := 0; i < 4; i++ {
		samples = append(samples, model.Sample{
			TSMs:   now + int64(i)*1000,
			Labels: map[string]string{"host": "a"},
			Fields: map[string]model.Value{"b0": model.Int(1), "b1": model.Int(2)},
		})
	}
	writeSamples(t, s, "h", samples, meta...)

	q, err := mql.Parse(`FROM h SELECT HISTOGRAM(hdr) FORMAT heatmap LIMIT POINTS 1`)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s.Query(context.Background(), &wire.QueryRequest{
		AST: q, FromMs: now - 1000, ToMs: now + 100000, MaxPoints: 100, IntervalMs: 1000,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !resp.Stats.Truncated {
		t.Fatalf("the datapoint gate did not trip: %+v", resp.Stats)
	}
	for i, ser := range resp.Series {
		if ser.TSMs == nil || ser.Values == nil || ser.IsNull == nil {
			t.Errorf("series %d (%s) carries a nil array", i, ser.Name)
		}
	}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(b), `"ts_ms":null`) || strings.Contains(string(b), `"values":null`) {
		t.Fatalf("heatmap response carries a null array: %s", b)
	}
}

// Stats is answered while the store is shutting down, and without
// reaching into the engine once it has been closed.
func TestStatsAfterCloseAnswersCounters(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	writeSamples(t, s, "app", []model.Sample{{
		TSMs: base(), Labels: map[string]string{"host": "a"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}})
	before := s.Stats()
	if before.Puts == 0 {
		t.Fatalf("no puts counted before close: %+v", before)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	after := s.Stats()
	if after.Puts != before.Puts {
		t.Fatalf("counters lost across close: before %d, after %d", before.Puts, after.Puts)
	}
}

// Stats is reached from a Prometheus scrape and, in plugin mode, from a
// health check that goes through no http.Server at all, so it runs
// concurrently with a shutdown as a matter of course. Pebble's contract
// is that no method may run beside Close; the gate is what holds it.
func TestStatsDoesNotRaceClose(t *testing.T) {
	for i := 0; i < 10; i++ {
		cfg := DefaultConfig()
		cfg.DataDir = t.TempDir()
		cfg.Durability = "batch"
		cfg.RetentionSweep = 0
		s, err := Open(cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		writeSamples(t, s, "app", []model.Sample{{
			TSMs: base(), Labels: map[string]string{"host": "a"},
			Fields: map[string]model.Value{"v": model.Int(1)},
		}})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for j := 0; j < 300; j++ {
				_ = s.Stats()
			}
		}()
		if err := s.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		<-done
	}
}
