package main

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
