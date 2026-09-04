package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rglonek/mensura/internal/store"
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

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mensura.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// The `db:` block is the one an operator reaches for to fit the store on a
// small host (09-operations.md section 5). It used to be decoded and then
// dropped on the floor, so the 1 GiB cache default stayed put with nothing
// saying so.
func TestEngineTuningReachesTheStoreConfig(t *testing.T) {
	cfg, err := loadConfig(writeConfig(t, `
data_dir: /tmp/x
db:
  cache_bytes: 268435456
  memtable_size_bytes: 67108864
  max_concurrent_compactions: 2
  compression: zstd
`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	sc, err := cfg.toStoreConfig()
	if err != nil {
		t.Fatalf("toStoreConfig: %v", err)
	}
	if sc.CacheBytes != 268435456 {
		t.Errorf("cache_bytes did not reach the store config: %d", sc.CacheBytes)
	}
	if sc.MemTableSizeBytes != 67108864 {
		t.Errorf("memtable_size_bytes did not reach the store config: %d", sc.MemTableSizeBytes)
	}
	if sc.MaxConcurrentCompactions != 2 {
		t.Errorf("max_concurrent_compactions did not reach the store config: %d", sc.MaxConcurrentCompactions)
	}
	if sc.Compression != "zstd" {
		t.Errorf("compression did not reach the store config: %q", sc.Compression)
	}
}

// Two keys in the documented example were never implemented, and
// KnownFields(true) reported them as an unmarshalling error naming the
// whole anonymous struct. A key that is refused has to say why.
func TestUnimplementedLimitKeysAreRefusedByName(t *testing.T) {
	for _, body := range []string{
		"limits:\n  max_concurrent_requests: 16\n",
		"limits:\n  write_rate_per_client: {rps: 2000, burst: 8000}\n",
	} {
		_, err := loadConfig(writeConfig(t, body))
		if err == nil {
			t.Fatalf("%q was accepted", body)
		}
		if !strings.Contains(err.Error(), "not implemented") {
			t.Errorf("refusal does not explain itself: %v", err)
		}
	}
	// The keys that are implemented still load.
	if _, err := loadConfig(writeConfig(t, "limits:\n  max_concurrent_writes: 8\n  max_concurrent_jobs: 4\n")); err != nil {
		t.Fatalf("a supported limits block was refused: %v", err)
	}
}

// A read-only address that is also the write address is not read-only.
// startListeners can only mount one handler per address, so it kept the
// full mux and skipped the query listener entirely: the address an
// operator publishes to Grafana then accepted /v1/write and DELETE
// /v1/admin/sets/ as well, silently.
//
// The check lives in checkAuthPosture rather than loadConfig because the
// write address is not settled until the flags and the default have been
// applied.
func TestQueryListenerMayNotShareTheWriteAddress(t *testing.T) {
	cfg := &fileConfig{}
	cfg.Auth.Mode = "bearer"
	cfg.Listen.Write.Addr = "0.0.0.0:9631"
	cfg.Listen.Query.Addr = "0.0.0.0:9631"
	err := checkAuthPosture(cfg)
	if err == nil {
		t.Fatal("expected the shared address to be refused")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("refusal does not explain itself: %v", err)
	}

	// A distinct address is fine, and so is leaving the query listener
	// unset to serve everything on the write address.
	cfg.Listen.Query.Addr = "0.0.0.0:9632"
	if err := checkAuthPosture(cfg); err != nil {
		t.Fatalf("distinct addresses were refused: %v", err)
	}
	cfg.Listen.Query.Addr = ""
	if err := checkAuthPosture(cfg); err != nil {
		t.Fatalf("an unset query listener was refused: %v", err)
	}
}

// A listener that cannot bind must fail startup. The bind used to happen
// inside the serving goroutine, where the error was logged and nothing
// else: an address already in use still printed "api listener on ..." and
// "mensura-store ... listening on ...", and the process then sat on its
// context serving nothing.
func TestStartListenersFailsWhenAnAddressIsTaken(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("take an address: %v", err)
	}
	defer held.Close()

	dir := t.TempDir()
	s, err := store.Open(store.Config{DataDir: filepath.Join(dir, "data"), Durability: "batch", RetentionSweep: 0})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	api := store.NewAPI(s, store.APIConfig{})

	cfg := &fileConfig{}
	cfg.Listen.Write.Addr = "127.0.0.1:0"
	cfg.Listen.Query.Addr = held.Addr().String()
	logger := log.New(io.Discard, "", 0)

	servers, err := startListeners(context.Background(), cfg, api, logger)
	if err == nil {
		shutdown(servers)
		t.Fatal("startListeners accepted an address that is already in use")
	}
	if !strings.Contains(err.Error(), "query listener") {
		t.Errorf("the error does not name the listener that failed: %v", err)
	}
}
