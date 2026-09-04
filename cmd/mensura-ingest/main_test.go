package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/extract"
)

const testSpec = `
version: 1
profiles:
  - name: p
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: app
        search: "n="
        extract: ['n=(?P<n>\d+)']
`

// An operator label that fails validation used to be discarded on its way
// to the store, silently: a typo cost every sample its dc or env and
// nothing said so. It is a startup error now.
func TestInvalidOperatorLabelsAreRefusedAtStartup(t *testing.T) {
	dir := t.TempDir()
	spec := filepath.Join(dir, "spec.yaml")
	if err := os.WriteFile(spec, []byte(testSpec), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	for _, c := range []struct {
		name   string
		labels labelFlag
		ok     bool
	}{
		{"good", labelFlag{"dc": "eu-west-1"}, true},
		{"empty value", labelFlag{"dc": ""}, false},
		{"reserved key", labelFlag{"timestamp": "now"}, false},
		{"bad key", labelFlag{"dc-": "x"}, true}, // '-' is legal after the first rune
		{"key starting with a digit", labelFlag{"1dc": "x"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			common := commonFlags{spec: spec, storeURL: "http://127.0.0.1:1", labels: c.labels}
			_, sink, err := common.setup()
			if sink != nil {
				t.Cleanup(func() { _ = sink.Close(nil) })
			}
			if c.ok && err != nil {
				t.Fatalf("a valid label was refused: %v", err)
			}
			if !c.ok {
				if err == nil {
					t.Fatal("an invalid label was accepted; it would be dropped silently later")
				}
				if !strings.Contains(err.Error(), "--label") {
					t.Errorf("the error does not name the flag: %v", err)
				}
			}
		})
	}
}

// `check` predicts what the import will do, so it has to select a profile
// the way the import does: processFile passes the operator labels, and
// checkSample passed nil, so a profile chosen by select.label_equals could
// never match and the tool reported "no profile matched" for a spec that
// works.
func TestCheckSelectsOnOperatorLabels(t *testing.T) {
	const spec = `
version: 1
profiles:
  - name: prod
    select:
      label_equals: {env: prod}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^[0-9]+'}]
    patterns:
      - set: app
        search: "n="
        extract: ['n=(?P<n>\d+)']
`
	dir := t.TempDir()
	specPath := filepath.Join(dir, "spec.yaml")
	if err := os.WriteFile(specPath, []byte(spec), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	sample := filepath.Join(dir, "app.log")
	if err := os.WriteFile(sample, []byte("1756382400000 n=1\n"), 0o644); err != nil {
		t.Fatalf("write sample: %v", err)
	}
	s, err := extract.Load(specPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := checkSample(s, sample, nil, 0, false); err == nil {
		t.Fatal("a label-selected profile matched with no labels supplied")
	}
	if err := checkSample(s, sample, map[string]string{"env": "prod"}, 0, false); err != nil {
		t.Fatalf("--label did not reach profile selection: %v", err)
	}
}
