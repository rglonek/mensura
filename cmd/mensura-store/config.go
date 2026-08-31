package main

import (
	"fmt"
	"os"
	"strconv"
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
			return nil, fmt.Errorf("config %s: client %q has an inline secret; hash it with `mensura-store hash-secret` and set `hash` instead", path, c.Name)
		}
		if c.Hash == "" {
			return nil, fmt.Errorf("config %s: client %q has no hash", path, c.Name)
		}
		if len(c.Scopes) == 0 {
			return nil, fmt.Errorf("config %s: client %q has no scopes; it could not do anything", path, c.Name)
		}
		for _, sc := range c.Scopes {
			switch store.Scope(sc) {
			case store.ScopeWrite, store.ScopeQuery, store.ScopeAdmin:
			default:
				return nil, fmt.Errorf("config %s: client %q has unknown scope %q (write, query or admin)", path, c.Name, sc)
			}
		}
	}
	switch cfg.Auth.Mode {
	case "", "none", "bearer":
	default:
		return nil, fmt.Errorf("config %s: auth.mode %q is not supported (none or bearer)", path, cfg.Auth.Mode)
	}
	// mTLS is not implemented. Accepting the key and ignoring it would
	// leave an operator believing client certificates are being verified.
	for name, l := range map[string]listenSpec{
		"write": cfg.Listen.Write, "query": cfg.Listen.Query,
		"debug": cfg.Listen.Debug, "metrics": cfg.Listen.Metrics,
	} {
		if l.TLS.ClientCA != "" {
			return nil, fmt.Errorf("config %s: listen.%s.tls.client_ca is set, but client-certificate verification is not implemented; remove it rather than rely on it", path, name)
		}
		if (l.TLS.Cert == "") != (l.TLS.Key == "") {
			return nil, fmt.Errorf("config %s: listen.%s.tls needs both cert and key", path, name)
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
//
// Only a bare "<number>d" is rewritten. Sscanf stops at the first byte it
// cannot use and reports no error for the rest, so "1h30d" used to parse
// as 1 and expand to "24h" -- a silently wrong retention from a plausible
// typo. Anything that is not exactly a number followed by "d" is handed to
// time.ParseDuration unchanged, which rejects it with a real error.
func expandDays(s string) string {
	body, ok := strings.CutSuffix(s, "d")
	if !ok || body == "" {
		return s
	}
	n, err := strconv.ParseFloat(body, 64)
	if err != nil {
		return s
	}
	return strconv.FormatFloat(n*24, 'f', -1, 64) + "h"
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
