package extract

import (
	"fmt"
	"testing"
)

const zeroRetentionSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_ms
          regex: '^\d{13}'
    patterns:
      - set: app
        search: 'v='
        extract: ['v=(?P<v>\d+)']
sets:
  app: {retention: 0}
`

// `sets: {app: {retention: 0}}` is the documented way to withdraw a set's
// retention -- 05-storage.md section 7.1 and 12-implementation.md section
// 6.131 both name that exact form, and the store's own `--retention 0`
// accepts it. The spec compiler read it through mql.ParseDuration, which
// refused a bare zero, so the one spelling the documents use failed to
// compile with "invalid duration unit in \"0\"".
func TestSetRetentionZeroCompiles(t *testing.T) {
	s, err := Parse([]byte(zeroRetentionSpec))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	opt, ok := s.Sets["app"]
	if !ok {
		t.Fatal("the sets: block did not survive compilation")
	}
	ms := opt.RetentionMs()
	if ms == nil {
		t.Fatal("retention: 0 compiled to no declaration at all; the store would keep its own default")
	}
	if *ms != 0 {
		t.Fatalf("retention: 0 compiled to %d ms", *ms)
	}
}

// A negative retention is still a spec error, and a shard width of zero
// is still refused: neither is a setting, both are typos.
func TestZeroIsNotAcceptedWhereItWouldMeanNothing(t *testing.T) {
	for _, body := range []string{"  app: {retention: -5s}", "  app: {shard: 0}"} {
		spec := zeroRetentionSpec[:len(zeroRetentionSpec)-len("  app: {retention: 0}\n")] + body + "\n"
		if _, err := Parse([]byte(spec)); err == nil {
			t.Fatalf("%q was accepted", body)
		}
	}
}

const limitsSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats:
        - layout: epoch_ms
          regex: '^\d{13}'
    fields:
      v: {kind: gauge, limits: {min: %s, max: %s}}
    patterns:
      - set: app
        search: 'v='
        extract: ['v=(?P<v>\d+)']
`

// The declared limits become the clamp the query layer installs by
// default, and nothing checked them. A non-finite bound -- YAML reads
// `.nan` and `.inf` as real float64s -- makes the whole write request
// carrying the declaration unencodable, which the sink reports as a lost
// batch and every followed file's checkpoint freezes behind. An inverted
// pair is quieter: MQL refuses `CLAMP MIN 5, MAX 1` as E007, while the
// same pair arriving from the catalogue installed a clamp that bounds
// nothing.
func TestFieldLimitsAreValidated(t *testing.T) {
	bad := [][2]string{{".nan", "10"}, {"0", ".inf"}, {"0", "-.inf"}, {"5", "1"}}
	for _, pair := range bad {
		spec := fmt.Sprintf(limitsSpec, pair[0], pair[1])
		if _, err := Parse([]byte(spec)); err == nil {
			t.Fatalf("limits {min: %s, max: %s} was accepted", pair[0], pair[1])
		}
	}
	if _, err := Parse([]byte(fmt.Sprintf(limitsSpec, "0", "100"))); err != nil {
		t.Fatalf("an ordinary limits block was refused: %v", err)
	}
}
