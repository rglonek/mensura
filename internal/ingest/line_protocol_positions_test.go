package ingest

import (
	"strings"
	"testing"
	"time"
)

// The line protocol is positional and has exactly four positions. A fifth
// used to be discarded while the rest of the line was stored, so a sender
// that separated its fields with a space instead of a comma was told its
// sample had landed and lost every field past the first.
func TestLineProtocolRefusesContentPastTheTimestamp(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	_, _, err := ParseLineProtocol(`app host=h1 v=1 1700000000000 lat=9`, now)
	if err == nil {
		t.Fatal("content after the timestamp was accepted and dropped")
	}
	if !strings.Contains(err.Error(), "lat=9") {
		t.Fatalf("error %q does not name what it refused", err)
	}
	// The same mistake without a timestamp: the fields section is split
	// at the space, so the tail lands past the timestamp position.
	if _, _, err := ParseLineProtocol(`app host=h1 v=1 lat=9 extra=2`, now); err == nil {
		t.Fatal("a space-separated field list was accepted")
	}
}

// Trailing whitespace is only whitespace.
func TestLineProtocolTolerantOfTrailingSpace(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	set, sample, err := ParseLineProtocol(`app host=h1 v=1 1700000000000 `, now)
	if err != nil {
		t.Fatalf("a trailing space was refused: %v", err)
	}
	if set != "app" || sample.TSMs != 1_700_000_000_000 {
		t.Fatalf("set %q ts %d", set, sample.TSMs)
	}
	// And the three- and four-position forms still parse.
	if _, _, err := ParseLineProtocol(`app - v=1`, now); err != nil {
		t.Fatalf("three positions: %v", err)
	}
	if _, _, err := ParseLineProtocol(`app host=h1 v=1,w=2 1700000000000`, now); err != nil {
		t.Fatalf("four positions: %v", err)
	}
}
