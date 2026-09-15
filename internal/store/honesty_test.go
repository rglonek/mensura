package store

import (
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// runTabular does no downsampling at all, so a downsample window in the
// plan describes a stage that does not run -- and Explain's whole
// contract is that it describes the plan the executor runs.
func TestExplainReportsNoWindowForTabularFormats(t *testing.T) {
	s := openTestStore(t)
	const ts = int64(1_700_000_000_000)
	writeSamples(t, s, "app", []model.Sample{{
		TSMs:   ts,
		Labels: map[string]string{"host": "web1"},
		Fields: map[string]model.Value{"v": model.Int(1)},
	}})
	for _, format := range []string{"table", "logs"} {
		q, err := mql.Parse("FROM app SELECT v FORMAT " + format + " LIMIT POINTS 10")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		plan, err := s.Explain(q, &wire.QueryRequest{
			AST: q, FromMs: ts - 60_000, ToMs: ts + 60_000, MaxPoints: 100,
		})
		if err != nil {
			t.Fatalf("explain: %v", err)
		}
		if w := plan["downsample_window"].(int64); w != 0 {
			t.Fatalf("FORMAT %s reports a downsample window of %d; the tabular executor never downsamples", format, w)
		}
	}
	// A timeseries query still reports the window the walk really uses.
	q, err := mql.Parse(`FROM app SELECT v`)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.Explain(q, &wire.QueryRequest{
		AST: q, FromMs: ts - 60_000, ToMs: ts + 60_000, MaxPoints: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if w := plan["downsample_window"].(int64); w <= 0 {
		t.Fatalf("a timeseries plan must still report its window, got %d", w)
	}
}

// A label key this call created holds nothing and was never persisted, so
// leaving it in the map after the write failed spent one of
// MaxLabelKeys on a dictionary that can never answer a lookup --
// permanently, because nothing ever removes one.
func TestAFailedFirstInternDoesNotBurnALabelKey(t *testing.T) {
	s := openTestStore(t)
	// The engine refuses every write once it is closed, which is the
	// cheapest way in-package to make PutDict fail. The store's own Close
	// is idempotent and still runs from the test cleanup.
	if err := s.DB().Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
	before := len(s.dict)
	if _, err := s.intern("brand_new_key", "v"); err == nil {
		t.Fatal("interning against a closed engine must fail")
	}
	s.dictMu.RLock()
	_, kept := s.dict["brand_new_key"]
	after := len(s.dict)
	s.dictMu.RUnlock()
	if kept {
		t.Fatal("a key whose first value could not be written was left in the dictionary map")
	}
	if after != before {
		t.Fatalf("the key budget moved from %d to %d on a failed write", before, after)
	}
}

// applyFieldMeta validates every declaration before it applies any of
// them. applySetMeta did not, so an entry the *next* one made the request
// fail landed anyway: the caller sees a 400 and the store keeps half of
// what it refused.
func TestARefusedSetMetaBatchAppliesNoneOfIt(t *testing.T) {
	s := openTestStore(t)
	keep := int64(7 * 24 * time.Hour / time.Millisecond)
	_, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{
		{Set: "good", RetentionMs: &keep},
		{Set: "_mensura_nope"},
	}}, "", "test")
	if err == nil {
		t.Fatal("a reserved set name must make the declaration fail")
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, name := range s.Sets() {
		if name == "good" {
			t.Fatal("the earlier declaration was applied even though the request was refused")
		}
	}
	if d := s.retentionFor("good"); d != s.cfg.Retention {
		t.Fatalf("set %q kept a retention of %s from a refused request", "good", d)
	}
	// The same batch without the bad entry still applies.
	if _, err := s.Write(&wire.WriteRequest{SetMeta: []wire.SetMeta{
		{Set: "good", RetentionMs: &keep},
	}}, "", "test"); err != nil {
		t.Fatalf("a valid declaration must still apply: %v", err)
	}
	if d := s.retentionFor("good"); d != 7*24*time.Hour {
		t.Fatalf("retention is %s, want 168h", d)
	}
}
