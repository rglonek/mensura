package main

import (
	"testing"
	"time"
)

// "1h30d" used to expand to "24h": Sscanf stopped at the 'h' and reported
// no error, so a plausible typo became a silently wrong retention.
func TestExpandDaysRejectsCompoundForms(t *testing.T) {
	cases := []struct {
		in    string
		want  time.Duration
		valid bool
	}{
		{"30d", 720 * time.Hour, true},
		{"1.5d", 36 * time.Hour, true},
		{"0", 0, true},
		{"90m", 90 * time.Minute, true},
		{"1h30d", 0, false},
		{"xd", 0, false},
		{"d", 0, false},
	}
	for _, c := range cases {
		got, err := durationOr(c.in, -1)
		if c.valid {
			if err != nil {
				t.Errorf("%q: unexpected error %v", c.in, err)
				continue
			}
			if got != c.want {
				t.Errorf("%q: got %v, want %v", c.in, got, c.want)
			}
			continue
		}
		if err == nil {
			t.Errorf("%q was accepted as %v; a malformed duration must fail loudly", c.in, got)
		}
	}
}
