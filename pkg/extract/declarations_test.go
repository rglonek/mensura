package extract

import (
	"sort"
	"testing"
)

func declaredFields(t *testing.T, p *Profile) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, d := range p.Declarations() {
		names := make([]string, 0, len(d.Fields))
		for n := range d.Fields {
			names = append(names, n)
		}
		sort.Strings(names)
		out[d.Set] = names
	}
	return out
}

func sameNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// `extract:` and `route:` are alternatives, so each branch's captures
// belong to that branch's set and to no other -- and a pattern with no
// `extract:` never writes its own `set:` at all.
//
// Declaring them everywhere put a set nothing can write into the
// catalogue, where the query builder offers it and mql.Validate resolves
// a field against it: `FROM hist SELECT ms_only` validated clean and drew
// nothing, with no W203 to say the field is not there.
func TestDeclarationsFollowTheBranchThatWritesTheSet(t *testing.T) {
	s := mustSpec(t, `
version: 1
profiles:
  - name: p
    labels: [ns]
    timestamp:
      formats:
        - layout: epoch_ms
          regex: '^[0-9]+'
      strip: true
    fields:
      ms_only: {kind: gauge, unit: milliseconds}
      us_only: {kind: gauge, unit: microseconds}
    patterns:
      - set: hist
        search: 'histogram dump'
        route:
          - regex: 'dump \{(?P<ns>[^}]+)\} ms (?P<ms_only>\d+)'
            set: hist_ms
          - regex: 'dump \{(?P<ns>[^}]+)\} us (?P<us_only>\d+)'
            set: hist_us
`)
	got := declaredFields(t, s.Profiles[0])
	if _, phantom := got["hist"]; phantom {
		t.Errorf("a route-only pattern declared its own set %q, which nothing can write", "hist")
	}
	if !sameNames(got["hist_ms"], []string{"ms_only"}) {
		t.Errorf("hist_ms declares %v, want [ms_only]", got["hist_ms"])
	}
	if !sameNames(got["hist_us"], []string{"us_only"}) {
		t.Errorf("hist_us declares %v, want [us_only]", got["hist_us"])
	}
}

// A pattern that has both branches writes both sets, each with its own
// captures, and `default_values` and an aggregated column reach every
// destination because they are applied after a branch has matched.
func TestDeclarationsSplitExtractAndRoute(t *testing.T) {
	s := mustSpec(t, `
version: 1
profiles:
  - name: p
    labels: [op]
    timestamp:
      formats:
        - layout: epoch_ms
          regex: '^[0-9]+'
      strip: true
    fields:
      main: {kind: gauge}
      alt: {kind: gauge}
      filled: {kind: gauge}
    patterns:
      - set: base
        search: 'REQ'
        extract: ['REQ main=(?P<main>\d+)']
        default_values: {filled: "0"}
        route:
          - regex: 'REQ alt=(?P<alt>\d+)'
            set: other
`)
	got := declaredFields(t, s.Profiles[0])
	if !sameNames(got["base"], []string{"filled", "main"}) {
		t.Errorf("base declares %v, want [filled main]", got["base"])
	}
	if !sameNames(got["other"], []string{"alt", "filled"}) {
		t.Errorf("other declares %v, want [alt filled]", got["other"])
	}
}

// A route that names no set of its own writes the pattern's set, so that
// set is a destination and carries the route's captures.
func TestDeclarationsRouteWithoutOwnSet(t *testing.T) {
	s := mustSpec(t, `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_ms
          regex: '^[0-9]+'
      strip: true
    fields:
      v: {kind: gauge}
    patterns:
      - set: only
        search: 'REQ'
        route:
          - regex: 'REQ v=(?P<v>\d+)'
`)
	got := declaredFields(t, s.Profiles[0])
	if !sameNames(got["only"], []string{"v"}) {
		t.Errorf("only declares %v, want [v]", got["only"])
	}
	if len(got) != 1 {
		t.Errorf("declared %d sets, want 1: %v", len(got), got)
	}
}

// A bucket set is expanded after the captures are collected, whichever
// branch produced them, so every destination carries its columns.
func TestDeclarationsBucketSetsReachEveryDestination(t *testing.T) {
	s := mustSpec(t, `
version: 1
profiles:
  - name: p
    labels: [ns]
    timestamp:
      formats:
        - layout: epoch_ms
          regex: '^[0-9]+'
      strip: true
    bucket_sets:
      - name: hdr
        parse: paren_pairs
        buckets: ["00", "01"]
    patterns:
      - set: hist
        search: 'histogram dump'
        bucket_set: hdr
        route:
          - regex: 'dump \{(?P<ns>[^}]+)\} ms (?P<buckets>.*)'
            set: hist_ms
          - regex: 'dump \{(?P<ns>[^}]+)\} us (?P<buckets>.*)'
            set: hist_us
`)
	for _, d := range s.Profiles[0].Declarations() {
		if len(d.BucketSets) == 0 {
			t.Errorf("set %s carries no bucket set", d.Set)
		}
	}
	got := declaredFields(t, s.Profiles[0])
	if _, phantom := got["hist"]; phantom {
		t.Errorf("the route-only pattern still declared %q", "hist")
	}
}
