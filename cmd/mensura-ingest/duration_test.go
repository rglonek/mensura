package main

import (
	"testing"
	"time"
)

// --from and --to take the duration syntax the rest of the product uses.
// Go's parser has no day unit, so `--from 7d` used to fail with
// `time: unknown unit "d"` while `--retention 30d`, a spec's
// `retention: 30d` and a query's `EVERY 1d` all take it.
func TestRelativeTimeFlagTakesDays(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		in   string
		back time.Duration
	}{
		{"", 0},
		{"90m", 90 * time.Minute},
		{"1h30m", 90 * time.Minute},
		{"7d", 7 * 24 * time.Hour},
		{"30d", 30 * 24 * time.Hour},
		{"500ms", 500 * time.Millisecond},
	} {
		got, err := parseTimeFlag(tc.in)
		if err != nil {
			t.Fatalf("%q: %v", tc.in, err)
		}
		if tc.in == "" {
			if !got.IsZero() {
				t.Fatalf("an empty flag must leave the window open, got %v", got)
			}
			continue
		}
		want := now.Add(-tc.back)
		if d := got.Sub(want); d > time.Second || d < -time.Second {
			t.Fatalf("%q resolved to %v, want about %v", tc.in, got, want)
		}
	}
	for _, bad := range []string{"7w", "yesterday", "1h30", "99999999999999999999d"} {
		if _, err := parseTimeFlag(bad); err == nil {
			t.Fatalf("%q was accepted as a relative time", bad)
		}
	}
	// An absolute stamp still wins.
	got, err := parseTimeFlag("2026-09-01T10:00:00Z")
	if err != nil {
		t.Fatalf("RFC3339: %v", err)
	}
	if got.UTC().Format(time.RFC3339) != "2026-09-01T10:00:00Z" {
		t.Fatalf("RFC3339 stamp was not taken literally: %v", got)
	}
}
