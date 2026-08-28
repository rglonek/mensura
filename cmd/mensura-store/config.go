package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rglonek/mensura/internal/store"
	"gopkg.in/yaml.v3"
)

// fileConfig mirrors docs/design/09-operations.md section 1.1. Every field
// has a working default, so a config file is never required for a
// single-host setup.
type fileConfig struct {
	Mode           string `yaml:"mode"`
	DataDir        string `yaml:"data_dir"`
	StorageProfile string `yaml:"storage_profile"`
	Durability     string `yaml:"durability"`
	StoreURL       string `yaml:"store_url"` // proxy mode

	Listen struct {
		Write   listenSpec `yaml:"write"`
		Query   listenSpec `yaml:"query"`
		Debug   listenSpec `yaml:"debug"`
		Metrics listenSpec `yaml:"metrics"`
	} `yaml:"listen"`

	Auth struct {
		Mode    string `yaml:"mode"`
		Clients []struct {
			Name   string   `yaml:"name"`
			Hash   string   `yaml:"hash"`
			Secret string   `yaml:"secret"`
			Scopes []string `yaml:"scopes"`
		} `yaml:"clients"`
	} `yaml:"auth"`

	Limits struct {
		MaxRequestBytes       int64 `yaml:"max_request_bytes"`
		MaxConcurrentWrites   int   `yaml:"max_concurrent_writes"`
		MaxConcurrentJobs     int   `yaml:"max_concurrent_jobs"`
		MaxSeriesPerGraph     int   `yaml:"max_series_per_graph"`
		MaxDatapointsReceived int   `yaml:"max_datapoints_received"`
		MaxLabelCardinality   int   `yaml:"max_label_cardinality"`
	} `yaml:"limits"`

	Retention struct {
		Default string `yaml:"default"`
		Shard   string `yaml:"shard"`
		Sweep   string `yaml:"sweep"`
		Sets    map[string]struct {
			Retention string `yaml:"retention"`
			Shard     string `yaml:"shard"`
		} `yaml:"sets"`
	} `yaml:"retention"`

	DB struct {
		CacheBytes               int64  `yaml:"cache_bytes"`
		MemtableSizeBytes        uint64 `yaml:"memtable_size_bytes"`
		MaxConcurrentCompactions int    `yaml:"max_concurrent_compactions"`
		Compression              string `yaml:"compression"`
	} `yaml:"db"`
}

type listenSpec struct {
	Addr string `yaml:"addr"`
	TLS  struct {
		Cert     string `yaml:"cert"`
		Key      string `yaml:"key"`
		ClientCA string `yaml:"client_ca"`
	} `yaml:"tls"`
}

func loadConfig(path string) (*fileConfig, error) {
	cfg := &fileConfig{}
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	// A plaintext secret in a config file ends up in git and in backups.
	// Refuse it and say which key, rather than silently accepting it.
	for _, c := range cfg.Auth.Clients {
		if c.Secret != "" {
			return nil, fmt.Errorf("config %s: client %q has an inline secret; set `hash` (see `mensura-store hash-secret`) or supply it via the environment", path, c.Name)
		}
	}
	return cfg, nil
}

func (c *fileConfig) toStoreConfig() (store.Config, error) {
	sc := store.DefaultConfig()
	if c.DataDir != "" {
		sc.DataDir = c.DataDir
	}
	if c.Durability != "" {
		sc.Durability = c.Durability
	}
	if c.StorageProfile != "" {
		sc.StorageProfile = c.StorageProfile
	}
	if c.Limits.MaxSeriesPerGraph > 0 {
		sc.MaxSeriesPerGraph = c.Limits.MaxSeriesPerGraph
	}
	if c.Limits.MaxDatapointsReceived > 0 {
		sc.MaxDataPointsReceived = c.Limits.MaxDatapointsReceived
	}
	if c.Limits.MaxLabelCardinality > 0 {
		sc.MaxLabelCardinality = c.Limits.MaxLabelCardinality
	}
	if c.Limits.MaxConcurrentJobs > 0 {
		sc.MaxConcurrentJobs = c.Limits.MaxConcurrentJobs
	}
	var err error
	if sc.Retention, err = durationOr(c.Retention.Default, sc.Retention); err != nil {
		return sc, err
	}
	if sc.Shard, err = durationOr(c.Retention.Shard, sc.Shard); err != nil {
		return sc, err
	}
	if sc.RetentionSweep, err = durationOr(c.Retention.Sweep, sc.RetentionSweep); err != nil {
		return sc, err
	}
	if len(c.Retention.Sets) > 0 {
		sc.SetRetention = map[string]time.Duration{}
		sc.SetShard = map[string]time.Duration{}
		for name, s := range c.Retention.Sets {
			if s.Retention != "" {
				d, err := time.ParseDuration(expandDays(s.Retention))
				if err != nil {
					return sc, fmt.Errorf("retention for set %s: %w", name, err)
				}
				sc.SetRetention[name] = d
			}
			if s.Shard != "" {
				d, err := time.ParseDuration(expandDays(s.Shard))
				if err != nil {
					return sc, fmt.Errorf("shard for set %s: %w", name, err)
				}
				sc.SetShard[name] = d
			}
		}
	}
	return sc, nil
}

func durationOr(s string, fallback time.Duration) (time.Duration, error) {
	if s == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(expandDays(s))
	if err != nil {
		return fallback, err
	}
	return d, nil
}

// expandDays lets the config use "30d", which Go's duration parser does
// not accept, without inventing a second duration syntax elsewhere.
func expandDays(s string) string {
	if !strings.HasSuffix(s, "d") {
		return s
	}
	var n float64
	if _, err := fmt.Sscanf(strings.TrimSuffix(s, "d"), "%g", &n); err != nil {
		return s
	}
	return fmt.Sprintf("%gh", n*24)
}

func (c *fileConfig) toAPIConfig(mode string) store.APIConfig {
	api := store.APIConfig{
		AuthMode:            c.Auth.Mode,
		MaxRequestBytes:     c.Limits.MaxRequestBytes,
		MaxConcurrentWrites: c.Limits.MaxConcurrentWrites,
		Mode:                mode,
	}
	for _, cl := range c.Auth.Clients {
		scopes := make([]store.Scope, 0, len(cl.Scopes))
		for _, s := range cl.Scopes {
			scopes = append(scopes, store.Scope(s))
		}
		api.Clients = append(api.Clients, store.ClientAuth{
			Name:   cl.Name,
			Hash:   strings.TrimPrefix(cl.Hash, "sha256:"),
			Scopes: scopes,
		})
	}
	return api
}
