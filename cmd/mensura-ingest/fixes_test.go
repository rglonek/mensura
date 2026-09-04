package main

import (
	"compress/gzip"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/internal/ingest"
	"github.com/rglonek/mensura/pkg/extract"
)

const gzCheckSpec = `
version: 1
profiles:
  - name: numbered
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
      anchor: prefix
    patterns:
      - set: app
        search: "n="
        extract: ['n=(?P<n>\d+)']
`

// `check --sample` has to read a file the way the import will.
//
// It opened the file raw, so a .gz sample handed the extractor deflate
// bytes: the tool whose whole job is to predict what `batch` will do
// reported "no profile matched", or a 100% unmatched rate, for a file the
// importer reads without trouble.
func TestCheckSampleReadsACompressedFile(t *testing.T) {
	dir := t.TempDir()
	plain := filepath.Join(dir, "app.log")
	body := "1756382400000 n=1\n1756382401000 n=2\n"
	if err := os.WriteFile(plain, []byte(body), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	gzPath := filepath.Join(dir, "app.log.gz")
	f, err := os.Create(gzPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	zw := gzip.NewWriter(f)
	if _, err := zw.Write([]byte(body)); err != nil {
		t.Fatalf("compress: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	specPath := filepath.Join(dir, "spec.yaml")
	if err := os.WriteFile(specPath, []byte(gzCheckSpec), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	s, err := extract.Load(specPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, path := range []string{plain, gzPath} {
		if err := checkSample(s, path, nil, 0, false); err != nil {
			t.Fatalf("checkSample %s: %v", filepath.Base(path), err)
		}
	}
}

// An archive and a binary file are declined by name here for the reason
// the importer declines them, rather than being fed to the extractor as if
// they were records.
func TestCheckSampleDeclinesWhatTheImportSkips(t *testing.T) {
	dir := t.TempDir()
	specPath := filepath.Join(dir, "spec.yaml")
	if err := os.WriteFile(specPath, []byte(gzCheckSpec), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	s, err := extract.Load(specPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	tarball := filepath.Join(dir, "logs.tar")
	if err := os.WriteFile(tarball, []byte("not really a tar\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	err = checkSample(s, tarball, nil, 0, false)
	if err == nil || !strings.Contains(err.Error(), "archive") {
		t.Fatalf("an archive was not declined: %v", err)
	}

	binary := filepath.Join(dir, "core.log")
	if err := os.WriteFile(binary, []byte("1756382400000 n=1\x00\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := checkSample(s, binary, nil, 0, false); err == nil {
		t.Fatal("binary content was not declined")
	}
}

// `check` fails a pipeline only on a spec that cannot do what it says.
//
// It used to exit non-zero on every lint alike, so declaring an operator
// label in `defaults.labels` -- which is where the documentation puts it,
// and what `--label dc=eu-west-1` needs -- failed the build, with a
// message that itself said the label needs no declaration.
func TestCheckPassesASpecWithOnlyAdvisoryLints(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spec.yaml")
	if err := os.WriteFile(path, []byte(`
version: 1
defaults:
  labels: [dc]
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    fields:
      n: {kind: gauge}
    patterns:
      - set: app
        search: "n="
        extract: ['n=(?P<n>\d+)']
`), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	spec, err := extract.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(spec.Lint()) == 0 {
		t.Fatal("expected the undeclared operator label to be reported at all")
	}
	if err := runCheck([]string{"--spec", path, "--label", "dc=eu-west-1"}); err != nil {
		t.Fatalf("check failed a spec that works exactly as written: %v", err)
	}
}

// A pattern the matcher can never select still fails, so a genuinely
// broken spec does not ship.
func TestCheckFailsAnUnreachablePattern(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spec.yaml")
	if err := os.WriteFile(path, []byte(`
version: 1
profiles:
  - name: p
    select: {}
    timestamp:
      formats: [{layout: epoch_ms, regex: '^\d{13}'}]
    fields:
      n: {kind: gauge}
      m: {kind: gauge}
    patterns:
      - set: a
        search: "n"
        extract: ['n=(?P<n>\d+)']
      - set: b
        search: "nm"
        extract: ['nm=(?P<m>\d+)']
`), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	if err := runCheck([]string{"--spec", path}); err == nil {
		t.Fatal("check passed a spec whose second pattern can never be reached")
	}
}

// The delivery knobs reach the sink. They were documented on SinkConfig,
// each with the trade-off it makes, and settable by nobody: setup() passed
// DefaultSinkConfig verbatim.
func TestDeliveryFlagsReachTheSinkConfig(t *testing.T) {
	c := commonFlags{}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	c.register(fs)
	if err := fs.Parse([]string{
		"--batch-size", "7", "--batch-bytes", "8192", "--flush-interval", "250ms",
		"--max-buffered-samples", "99", "--max-fatal-drops", "-1",
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	got := c.sinkConfig()
	if got.BatchSize != 7 || got.BatchBytes != 8192 {
		t.Fatalf("batch bounds are %d samples / %d bytes", got.BatchSize, got.BatchBytes)
	}
	if got.FlushEvery != 250*time.Millisecond {
		t.Fatalf("flush interval is %s", got.FlushEvery)
	}
	if got.MaxBufferedSamples != 99 {
		t.Fatalf("buffer cap is %d", got.MaxBufferedSamples)
	}
	// Negative is how "never give up" is expressed, so it must survive
	// rather than being read as "unset".
	if got.MaxFatalDrops != -1 {
		t.Fatalf("max fatal drops is %d, want -1", got.MaxFatalDrops)
	}
	// An unset flag leaves the built-in default.
	var unset commonFlags
	def := unset.sinkConfig()
	base := ingest.DefaultSinkConfig()
	if def.BatchSize != base.BatchSize || def.MaxFatalDrops != base.MaxFatalDrops {
		t.Fatalf("unset flags changed the defaults: %+v", def)
	}
}
