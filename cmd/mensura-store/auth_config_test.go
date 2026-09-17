package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rglonek/mensura/internal/store"
)

func writeAuthConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "mensura.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// Bearer mode with nothing to authenticate is a store no client can
// reach: authorise() walks an empty list and refuses every request, so
// the write API answers 401 to every batch, the ingest sink holds its
// buffer and retries forever, and the store's own startup said nothing.
func TestBearerWithNoClientsIsRefused(t *testing.T) {
	_, err := loadConfig(writeAuthConfig(t, "auth:\n  mode: bearer\n"))
	if err == nil {
		t.Fatal("want an error for auth.mode: bearer with no clients")
	}
	if !strings.Contains(err.Error(), "no auth.clients") {
		t.Fatalf("error does not name the missing clients: %v", err)
	}
}

// A hash that is not 32 bytes of hex can never match a SHA-256 of a
// bearer token, so the credential is loaded, reported as configured and
// refuses every request. A truncated paste and the secret pasted where
// the hash belongs are the two ways it happens.
func TestMalformedClientHashIsRefused(t *testing.T) {
	good := store.HashSecret("s3cret")
	for name, hash := range map[string]string{
		"truncated": good[:40],
		"plaintext": "s3cret",
		"not-hex":   strings.Repeat("z", 64),
		"doubled":   "sha256:sha256:" + good,
	} {
		body := "auth:\n  mode: bearer\n  clients:\n    - name: ing\n      hash: " + hash + "\n      scopes: [write]\n"
		if _, err := loadConfig(writeAuthConfig(t, body)); err == nil {
			t.Errorf("%s hash %q was accepted", name, hash)
		}
	}
	for _, hash := range []string{good, "sha256:" + good, strings.ToUpper(good)} {
		body := "auth:\n  mode: bearer\n  clients:\n    - name: ing\n      hash: " + hash + "\n      scopes: [write]\n"
		if _, err := loadConfig(writeAuthConfig(t, body)); err != nil {
			t.Errorf("hash %q was refused: %v", hash, err)
		}
	}
}
