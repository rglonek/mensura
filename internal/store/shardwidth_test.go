package store

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/pkg/wire"
)

// A shard suffix can express whole days and the hour counts that divide
// one, and nothing else; anything else is rounded down. Open says so for
// a width that came from the config file, and said nothing at all for one
// that came from a spec's `sets:` block -- which is the documented way to
// declare per-set sharding, and the only way an ingester can. An operator
// who wrote `shard: 30m` got hourly shards with no diagnostic anywhere.
func TestSpecShardWidthRoundingIsReported(t *testing.T) {
	var buf bytes.Buffer
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.Logger = log.New(&buf, "", 0)
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	ms := (30 * time.Minute).Milliseconds()
	keep := (7 * 24 * time.Hour).Milliseconds()
	if err := s.applySetMeta([]wire.SetMeta{{Set: "app", RetentionMs: &keep, ShardMs: &ms}}); err != nil {
		t.Fatalf("applySetMeta: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "set app declares a shard width of 30m") {
		t.Fatalf("no warning for an inexact spec shard width; log was:\n%s", got)
	}
	if !strings.Contains(got, "using 1h") {
		t.Fatalf("the warning does not say what width is used; log was:\n%s", got)
	}

	// Repeated on every ingest process start, so an unchanged declaration
	// must not repeat the line.
	buf.Reset()
	if err := s.applySetMeta([]wire.SetMeta{{Set: "app", RetentionMs: &keep, ShardMs: &ms}}); err != nil {
		t.Fatalf("applySetMeta: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("re-declaring the same width logged again:\n%s", buf.String())
	}

	// A width the encoding can express exactly says nothing.
	buf.Reset()
	exact := time.Hour.Milliseconds()
	if err := s.applySetMeta([]wire.SetMeta{{Set: "app", ShardMs: &exact}}); err != nil {
		t.Fatalf("applySetMeta: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("an exact width warned:\n%s", buf.String())
	}
}
