package store

import (
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/internal/engine"
)

// openFor is Open on a throwaway directory, for the configurations below
// that are expected to be refused before anything is created.
func openFor(t *testing.T, mutate func(*Config)) error {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = time.Hour
	mutate(&cfg)
	s, err := Open(cfg)
	if err == nil {
		t.Cleanup(func() { _ = s.Close() })
	}
	return err
}

// A negative retention, shard width or sweep interval is not a tighter
// setting: every reader tests `> 0`, so it is the gate switched off -- and
// a set with no retention is routed to the unsharded "@all" shard that the
// sweep skips by name, so the data is kept for ever from a spelling that
// reads like the opposite.
func TestNegativeRetentionDurationsAreRefused(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"default retention", func(c *Config) { c.Retention = -time.Hour }, "retention"},
		{"default shard", func(c *Config) { c.Shard = -time.Hour }, "shard width"},
		{"sweep", func(c *Config) { c.RetentionSweep = -time.Hour }, "sweep interval"},
		{"per-set retention", func(c *Config) {
			c.SetRetention = map[string]time.Duration{"app": -time.Hour}
		}, "retention for set app"},
		{"per-set shard", func(c *Config) {
			c.SetShard = map[string]time.Duration{"app": -time.Hour}
		}, "shard width for set app"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := openFor(t, tc.mutate)
			if err == nil {
				t.Fatalf("a negative %s opened the store", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name %q", err, tc.want)
			}
		})
	}
}

// Zero keeps meaning "keep everything" / "leave the default", which is how
// the documented `--retention 0` posture is written.
func TestZeroRetentionDurationsStillOpen(t *testing.T) {
	if err := openFor(t, func(c *Config) {
		c.Retention, c.Shard, c.RetentionSweep = 0, 0, 0
	}); err != nil {
		t.Fatalf("zero durations were refused: %v", err)
	}
}

// Pebble's own contract is that MaxConcurrentCompactions is greater than
// zero, and it does not enforce it: the scheduler compares its compaction
// count against this number, so a negative one stops every compaction and
// the LSM grows until L0StopWritesThreshold stalls every write.
func TestNegativeEngineKnobsAreRefused(t *testing.T) {
	err := openFor(t, func(c *Config) { c.MaxConcurrentCompactions = -1 })
	if err == nil {
		t.Fatal("a negative db.max_concurrent_compactions opened the store")
	}
	if !strings.Contains(err.Error(), "max_concurrent_compactions") {
		t.Fatalf("error %q does not name the setting", err)
	}
	err = openFor(t, func(c *Config) { c.CacheBytes = -5 })
	if err == nil {
		t.Fatal("a negative db.cache_bytes that is not the sentinel opened the store")
	}
	if !strings.Contains(err.Error(), "cache_bytes") {
		t.Fatalf("error %q does not name the setting", err)
	}
	// The one negative that means something still works.
	if err := openFor(t, func(c *Config) { c.CacheBytes = engine.NoBlockCache }); err != nil {
		t.Fatalf("engine.NoBlockCache was refused: %v", err)
	}
}

// A datasource ceiling switched off must not take the query's own LIMIT
// with it: a limit may only ever narrow.
func TestNarrowerHonoursLimitWithNoCeiling(t *testing.T) {
	five := 5
	for _, ceiling := range []int{0, -1, -1000} {
		if got := narrower(ceiling, &five); got != 5 {
			t.Fatalf("narrower(%d, LIMIT 5) = %d, want 5", ceiling, got)
		}
	}
	if got := narrower(10, &five); got != 5 {
		t.Fatalf("narrower(10, LIMIT 5) = %d, want 5", got)
	}
	if got := narrower(3, &five); got != 3 {
		t.Fatalf("narrower(3, LIMIT 5) = %d, want 3", got)
	}
	if got := narrower(-1, nil); got != -1 {
		t.Fatalf("narrower(-1, no limit) = %d, want the ceiling through", got)
	}
}
