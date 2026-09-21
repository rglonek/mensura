package main

import (
	"os"
	"path/filepath"
	"testing"
)

// A negative value switches a gate off. Three of the six read `> 0` and so
// kept their default instead, so an operator removing a ceiling silently
// kept it -- while the three keys beside them, documented identically,
// removed it.
func TestNegativeLimitsDisableEveryGate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
data_dir: /tmp/x
limits:
  max_series_per_graph: -1
  max_datapoints_received: -1
  max_label_cardinality: -1
  max_label_keys: -1
  max_sets: -1
  max_fields_per_set: -1
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := cfg.toStoreConfig()
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]int{
		"max_series_per_graph":    sc.MaxSeriesPerGraph,
		"max_datapoints_received": sc.MaxDataPointsReceived,
		"max_label_cardinality":   sc.MaxLabelCardinality,
		"max_label_keys":          sc.MaxLabelKeys,
		"max_sets":                sc.MaxSets,
		"max_fields_per_set":      sc.MaxFieldsPerSet,
	} {
		if got > 0 {
			t.Errorf("%s = %d: a negative value must switch the gate off, not be ignored", name, got)
		}
	}
}

// Zero still means "leave the built-in default".
func TestZeroLimitsKeepTheDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("data_dir: /tmp/x\nlimits:\n  max_sets: 0\n  max_series_per_graph: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := cfg.toStoreConfig()
	if err != nil {
		t.Fatal(err)
	}
	if sc.MaxSets <= 0 || sc.MaxSeriesPerGraph <= 0 {
		t.Fatalf("zero must keep the default, got max_sets=%d max_series_per_graph=%d", sc.MaxSets, sc.MaxSeriesPerGraph)
	}
}
