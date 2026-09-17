package main

import (
	"crypto/sha256"
	"encoding/hex"
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
		MaxRequestBytes int64 `yaml:"max_request_bytes"`
		// MaxBufferedRequestBytes caps the request bodies the API holds
		// in memory at once, across every connection. Defaults to four
		// times max_request_bytes.
		MaxBufferedRequestBytes int64 `yaml:"max_buffered_request_bytes"`
		MaxConcurrentWrites     int   `yaml:"max_concurrent_writes"`
		MaxConcurrentJobs       int   `yaml:"max_concurrent_jobs"`
		MaxSeriesPerGraph       int   `yaml:"max_series_per_graph"`
		MaxDatapointsReceived   int   `yaml:"max_datapoints_received"`
		MaxLabelCardinality     int   `yaml:"max_label_cardinality"`
		// MaxLabelKeys bounds the distinct label *names* the store will
		// intern; max_label_cardinality bounds the values under each one.
		MaxLabelKeys int `yaml:"max_label_keys"`
		// MaxSets and MaxFieldsPerSet bound the catalogue itself, which
		// is the other thing a sender names and the store never
		// reclaims. Negative disables, as it does above.
		MaxSets         int `yaml:"max_sets"`
		MaxFieldsPerSet int `yaml:"max_fields_per_set"`

		// Accepted by the decoder only so they can be refused by name.
		// Neither is implemented, and KnownFields(true) reported them as
		// an unmarshalling error naming the whole anonymous struct --
		// which is what an operator got for copying the example config
		// out of docs/design/09-operations.md.
		MaxConcurrentRequests *int `yaml:"max_concurrent_requests"`
		WriteRatePerClient    *struct {
			RPS   int `yaml:"rps"`
			Burst int `yaml:"burst"`
		} `yaml:"write_rate_per_client"`
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
		// The hash is compared byte for byte against a SHA-256 of the
		// presented bearer token, so anything that is not 32 bytes of hex
		// can never match. A truncated paste, a `sha256:` prefix written
		// twice, or the *secret* pasted where the hash belongs therefore
		// produced a credential that was loaded, reported as configured
		// and refused every request -- 401 forever, with the only trace a
		// WARNING per attempt on a store that looks healthy. It is
		// decidable here, which is where every other credential mistake
		// is decided.
		if err := checkSecretHash(c.Hash); err != nil {
			return nil, fmt.Errorf("config %s: client %q: %w", path, c.Name, err)
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
	// Bearer mode with nothing to authenticate is a store no client can
	// reach. authorise() walks an empty client list and answers "not
	// authorised" to every request, so the write API returns 401 to every
	// batch -- which wire.Client classifies as an auth failure, so the
	// ingest sink holds its buffer, retries every 30 seconds and never
	// makes progress, while the store's log fills with one WARNING per
	// attempt and its own startup said nothing was wrong. It is the same
	// class of mistake as `auth.mode: none` on a routable address, caught
	// at the same moment.
	if cfg.Auth.Mode == "bearer" && len(cfg.Auth.Clients) == 0 {
		return nil, fmt.Errorf("config %s: auth.mode is bearer but no auth.clients are configured; every request would be refused as unauthorised", path)
	}
	if cfg.Limits.MaxConcurrentRequests != nil {
		return nil, fmt.Errorf("config %s: limits.max_concurrent_requests is not implemented; use max_concurrent_writes for the write API and max_concurrent_jobs for queries", path)
	}
	if cfg.Limits.WriteRatePerClient != nil {
		return nil, fmt.Errorf("config %s: limits.write_rate_per_client is not implemented; back-pressure is by write-slot shedding (max_concurrent_writes)", path)
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

// checkSecretHash holds a configured credential to the shape
// HashSecret produces: the optional "sha256:" prefix this file's own
// examples use, then 64 hex characters.
func checkSecretHash(h string) error {
	body := strings.TrimPrefix(h, "sha256:")
	if len(body) != 2*sha256.Size {
		return fmt.Errorf("hash is %d characters; it must be the %d-character SHA-256 that `mensura-store hash-secret` prints, optionally prefixed with \"sha256:\"", len(body), 2*sha256.Size)
	}
	if _, err := hex.DecodeString(strings.ToLower(body)); err != nil {
		return fmt.Errorf("hash is not hexadecimal; it must be what `mensura-store hash-secret` prints, not the secret itself")
	}
	return nil
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
	if c.Limits.MaxLabelKeys != 0 {
		// Non-zero rather than positive: a negative value is how every
		// other gate here is switched off, and Open reads zero as
		// "unset, use the default".
		sc.MaxLabelKeys = c.Limits.MaxLabelKeys
	}
	if c.Limits.MaxSets != 0 {
		sc.MaxSets = c.Limits.MaxSets
	}
	if c.Limits.MaxFieldsPerSet != 0 {
		sc.MaxFieldsPerSet = c.Limits.MaxFieldsPerSet
	}
	if c.Limits.MaxConcurrentJobs > 0 {
		sc.MaxConcurrentJobs = c.Limits.MaxConcurrentJobs
	}
	// The `db:` block reaches the engine. It used to be decoded and then
	// dropped, so an operator sizing the store for a small host kept the
	// 1 GiB cache default with nothing saying otherwise.
	sc.CacheBytes = c.DB.CacheBytes
	sc.MemTableSizeBytes = c.DB.MemtableSizeBytes
	sc.MaxConcurrentCompactions = c.DB.MaxConcurrentCompactions
	sc.Compression = c.DB.Compression
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
		AuthMode:                c.Auth.Mode,
		MaxRequestBytes:         c.Limits.MaxRequestBytes,
		MaxBufferedRequestBytes: c.Limits.MaxBufferedRequestBytes,
		MaxConcurrentWrites:     c.Limits.MaxConcurrentWrites,
		Mode:                    mode,
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
