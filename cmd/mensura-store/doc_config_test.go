package main

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The example configuration in docs/design/09-operations.md is what an
// operator copies, and it is the one input the loader cannot warn about
// usefully: `metrics:{addr: …}` -- a key with no space after its colon --
// is not a mapping at all, so go-yaml reported "line 6: did not find
// expected key", pointing at `listen:` and saying nothing about the
// entry three lines below it.
//
// The block is decoded rather than run: the documented placeholders
// (`cert: …`, `hash: "sha256:…"`) are deliberately not values, and the
// semantic checks that reject them are loadConfig's own business. What is
// checked here is that the example is valid YAML and that every key in it
// is a key the loader has -- which is the other way an example drifts,
// since KnownFields(true) refuses anything else.
func TestDocumentedConfigExampleDecodes(t *testing.T) {
	const doc = "../../docs/design/09-operations.md"
	b, err := os.ReadFile(doc)
	if err != nil {
		t.Skipf("%s: %v", doc, err)
	}
	blocks := regexp.MustCompile("(?s)```ya?ml\n(.*?)```").FindAllStringSubmatch(string(b), -1)
	checked := 0
	for _, m := range blocks {
		body := m[1]
		// Only the store's own configuration: the file also shows
		// extraction-spec fragments, which are a different schema.
		if !strings.Contains(body, "data_dir:") || !strings.Contains(body, "listen:") {
			continue
		}
		checked++
		var cfg fileConfig
		dec := yaml.NewDecoder(strings.NewReader(body))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil {
			t.Fatalf("the documented configuration does not decode: %v\n---\n%s", err, body)
		}
		if cfg.Mode == "" || cfg.DataDir == "" {
			t.Fatalf("the documented configuration decoded to nothing useful: %+v", cfg)
		}
	}
	if checked == 0 {
		t.Fatalf("found no store configuration block in %s; this guard is watching nothing", doc)
	}
}
