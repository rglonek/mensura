package extract

import (
	"strings"
	"testing"
)

const framingBase = `
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    framing:
%s
    patterns:
      - set: lines
        search: 'n='
        extract: ['n=(?P<n>\d+)']
`

// A framing bound that is declared and then silently ignored is worse
// than one that is rejected: every reader of max_record_bytes tests
// `n > 0`, so a negative value removed the cap the operator was trying to
// set, and a negative multiline idle_timeout flushed every buffered
// record on the next tick instead of joining anything.
func TestFramingBoundsAreValidated(t *testing.T) {
	cases := []struct {
		name    string
		framing string
		want    string
	}{
		{"negative record cap", "      max_record_bytes: -1\n", "max_record_bytes"},
		{
			"negative idle timeout",
			"      multiline:\n        - start_contains: 'BEGIN'\n          continue_regex: '^ '\n          idle_timeout: -5s\n",
			"idle_timeout",
		},
		{
			"zero idle timeout",
			"      multiline:\n        - start_contains: 'BEGIN'\n          continue_regex: '^ '\n          idle_timeout: 0s\n",
			"idle_timeout",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(strings.Replace(framingBase, "%s", c.framing, 1)))
			if err == nil {
				t.Fatalf("the spec compiled; a declared bound that does nothing must be refused")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not name %s", err, c.want)
			}
		})
	}
}

// The ordinary shapes still compile: unset means "take the default".
func TestFramingBoundsAcceptTheOrdinaryShapes(t *testing.T) {
	for _, framing := range []string{
		"      record: line\n",
		"      max_record_bytes: 4096\n",
		"      multiline:\n        - start_contains: 'BEGIN'\n          continue_regex: '^ '\n",
		"      multiline:\n        - start_contains: 'BEGIN'\n          continue_regex: '^ '\n          idle_timeout: 5s\n",
	} {
		if _, err := Parse([]byte(strings.Replace(framingBase, "%s", framing, 1))); err != nil {
			t.Errorf("framing %q was refused: %v", framing, err)
		}
	}
}
