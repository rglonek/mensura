package store

import "testing"

// The forward map is derived from the entries, not accumulated beside
// them.
//
// The two on-disk dictionary forms are loaded one after the other into
// the same struct -- the legacy packed array first, then the per-value
// records, which write into positions the array already filled -- and
// each loader added its own map entries as it went. An overwritten
// position therefore left the old value in the map pointing at a slot
// that now holds a different one, so `label = "<old>"` lowered to that
// slot and the scan returned the rows of whichever value really lives
// there. Not an empty result: the wrong rows.
func TestRebuildIndexDropsAStaleEntry(t *testing.T) {
	d := &dictionary{
		Entries: []string{"web1", "web2"},
		// What a mixed load leaves behind: "gone" was at position 1
		// before a per-value record overwrote it with "web2".
		index: map[string]int32{"web1": 0, "gone": 1, "web2": 1},
	}
	d.rebuildIndex()

	if _, hit := d.positions("gone"); hit {
		t.Fatal("a value no entry holds still resolves to a position; a query for it would return another value's rows")
	}
	for want, at := range map[string]int32{"web1": 0, "web2": 1} {
		got, hit := d.positions(want)
		if !hit || len(got) != 1 || got[0] != at {
			t.Fatalf("%q resolved to %v, want [%d]", want, got, at)
		}
	}
	if d.live != 2 {
		t.Fatalf("live is %d, want 2", d.live)
	}
}

// A value an older build repaired a hole with occupies two positions, and
// both must stay reachable: the map names the first and the alias list
// carries the rest, so a comparison cannot silently exclude the rows
// stored under one of them.
func TestRebuildIndexKeepsEveryPositionOfADuplicate(t *testing.T) {
	d := &dictionary{Entries: []string{"a", "", "a", "b"}, index: map[string]int32{}}
	d.rebuildIndex()

	got, hit := d.positions("a")
	if !hit || len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("positions(a) = %v, want [0 2]", got)
	}
	if len(d.holes) != 1 || d.holes[0] != 1 {
		t.Fatalf("holes = %v, want [1]", d.holes)
	}
	if d.live != 3 {
		t.Fatalf("live is %d, want 3", d.live)
	}
	// A hole is not a value: nothing may intern one, so resolving the
	// empty string to its position would make `label = ""` match rows.
	if _, hit := d.positions(""); hit {
		t.Fatal("the empty string resolved to a position")
	}
}
