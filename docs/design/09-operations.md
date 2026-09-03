# 09 — Operations

## 1. Configuration model

Every setting has three sources, in increasing precedence: config file (YAML) →
environment variable → command-line flag. Every setting has a documented
default; a config file is never required for a working single-host setup.

Environment variables are prefixed `MENSURA_INGEST_` and `MENSURA_STORE_`, and
follow the field path (`MENSURA_STORE_DB_CACHE_BYTES`). Sensitive values
(tokens, object-store and SFTP credentials) may only come from environment
variables or from a path referenced by config — never inline in a config file
that ends up in git, and never on a command line that ends up in a process
listing. `mensura-store config check` refuses to start on an inline secret and
names the offending key.

### 1.1 Store config sketch

```yaml
mode: server                     # server | proxy | plugin
data_dir: /var/lib/mensura
storage_profile: local           # local | network-fs
durability: stream               # batch | stream | paranoid

listen:
  write:  {addr: "0.0.0.0:9631", tls: {cert: …, key: …, client_ca: …}}
  query:  {addr: "127.0.0.1:9632"}   # read surface only: no /v1/write, no /v1/admin/*
  debug:  {addr: "127.0.0.1:9633"}   # /v1/debug/*, loopback only
  metrics:{addr: "127.0.0.1:9634"}

# Every listener honours its own tls: block. A separate `query` address is a
# separate *surface*, not only a separate port: it mounts the read endpoints
# and nothing else, so publishing it to Grafana cannot expose writes or admin
# calls even with auth.mode: none. It therefore needs an address of its own:
# setting it equal to listen.write.addr is refused at startup, because one
# address can only carry one handler and the write surface would win.

auth:
  mode: bearer                   # bearer | none (loopback only); mTLS is not implemented
  clients:
    - {name: web1, hash: "sha256:…", scopes: [write]}
    - {name: grafana, hash: "sha256:…", scopes: [query]}

limits:
  max_request_bytes: 33554432
  max_buffered_request_bytes: 0   # 0 = 4x max_request_bytes; total body bytes held at once
  max_concurrent_writes: 8        # write slots; past them the store sheds with 503
  max_concurrent_jobs: 8          # concurrent queries
  max_series_per_graph: 1000
  max_datapoints_received: 34560000
  max_label_cardinality: 100000

# Per-client rate limiting is not implemented (12-implementation.md §5), so
# there is no write_rate_per_client key and no max_concurrent_requests:
# back-pressure is write-slot shedding. The loader refuses both by name
# rather than accepting a limit it would not enforce.

retention:
  default: 30d
  shard: 24h
  sets:
    http: {retention: 7d, shard: 1h}

db:                              # see 05-storage.md §9; 0 = engine default
  cache_bytes: 0
  memtable_size_bytes: 0
  max_concurrent_compactions: 0
  compression: ""
```

### 1.2 Ingest config sketch

```yaml
store:
  url: https://store.internal:9631
  token_env: MENSURA_INGEST_TOKEN
  wire: proto                    # proto | json
  compression: zstd
  batch: {size: 1024, bytes: 4194304, flush_ms: 50}

state_dir: /var/lib/mensura-ingest
spec: /etc/mensura/specs/app.yaml

labels: {dc: eu-west-1, env: prod}

inputs:
  - type: follow
    path: ['/var/log/app/*.log']
    start_at: end                # end | beginning | checkpoint (default)
    rotated_glob: '{path}{,.1,.0,-*}{,.gz,.zst}'
  - type: follow
    ssh: {host: db1.internal, user: mensura, credential_path: /etc/mensura/ssh-cred}
    path: ['/var/log/service/*.log']
  - type: receive
    listen: {tcp: "0.0.0.0:9640", udp: "0.0.0.0:9640"}
    mode: logs                   # logs | metrics

progress:
  file: /var/lib/mensura-ingest/progress.json
  print_interval: 30s
```

## 2. Deployment topologies

### 2.1 Single host, triage

Grafana + `mensura-store` in embedded (plugin) mode + one-shot
`mensura-ingest batch`. Everything on loopback; auth may be `none` on a
loopback-bound write listener. Point ingest at a bundle, open the dashboard.

### 2.2 Central store, many followers

`mensura-store --mode=server` on a collection host; `mensura-ingest follow` on
each source host writing over TLS with per-host bearer tokens or client
certificates; Grafana anywhere, with the datasource in `proxy` mode.

### 2.3 Central store, no agents

`mensura-store --mode=server`; one `mensura-ingest` doing SSH follow against N
hosts, plus a receive listener for hosts that already run a shipper. Fewer
moving parts on the fleet, weaker rotation guarantees
([02-ingest.md §6.4](02-ingest.md)).

### 2.4 Container / Kubernetes

The store is a `StatefulSet` with a `PersistentVolumeClaim` (it is a
single-writer local database; a `Deployment` with two replicas would corrupt
nothing but would fail to start the second pod, loudly). Ingest is a
`DaemonSet` mounting `/var/log`, or a sidecar per workload. The Grafana plugin
runs inside the Grafana pod in proxy mode.

## 3. Sizing

Start from the sample rate, not the log volume:

```
samples/s = (patterns matched per line) × (lines/s)
bytes/day ≈ samples/s × 86400 × 60 B     # compressed, typical row
```

- 50 hosts × 1 stats line/s × 4 fields ≈ 50 samples/s ≈ 260 MiB/day.
- A 20 GiB log bundle typically yields tens of millions of samples and a store
  of a few GiB after post-ingest compaction.

Memory floor for the store is roughly `cache_bytes + memtable_size ×
(stop_writes_threshold + 1) + 512 MiB` of headroom for query buffers; at
defaults that is ~2.5 GiB. On a small host, set `cache_bytes: 268435456` and
`memtable_size_bytes: 67108864` and expect slower scans, not failures.

CPU: ingest is regex-bound and scales nearly linearly with cores up to the
file-parallelism cap; the store is I/O-bound on ingest and scan-bound on query.

Disk: provision 2× the steady-state size to leave room for compaction. On
network filesystems, use the `network-fs` storage profile
([05-storage.md §9](05-storage.md)) — the difference there is tens of seconds
per flush, not percentages.

## 4. Security

| Surface | Posture |
| --- | --- |
| Write API | TLS required off-loopback; bearer or mTLS; per-client scopes; per-client rate limits |
| Query API | Same; `query` scope only |
| Debug API | Loopback listener only, never proxied, read-only, capped limits and per-request timeouts |
| Metrics | Separate listener, typically loopback or a private interface |
| Ingest ↔ SSH | Public-key auth only, host-key verification on by default (`--ssh-strict-host-key=yes`), no interactive prompts |
| Sensitive values | Environment or referenced path only; never logged; auth failures log the client *name* and source address, never the credential |
| Spec files | Treated as configuration, not untrusted input, but regexes compile under a size limit and `check` reports catastrophic-backtracking risk from nested quantifiers |
| Receive listeners | Optional TLS and mTLS; per-connection and per-source rate limits; UDP source allow-list |

The store never executes anything derived from ingested data, has no template
evaluation in queries, and writes no files outside `data_dir` and its log.

## 5. Observability

**Store metrics** (`/metrics`, `query` scope — a scrape against a
`bearer`-mode store needs a token): write requests by status, samples written,
rejected samples by reason, batch commit latency, engine stats (puts, scans,
open iterators), Pebble metrics (cache hit rate, L0 sublevels, compaction debt,
disk bytes), query count/latency/rows-scanned/series/points, safety-gate trips,
retention sweeps and shards dropped.

**Ingest metrics**: bytes read, records framed, samples emitted per set,
unmatched lines, timestamp parse failures, extraction errors by pattern,
batches sent/retried/dropped, in-flight requests, per-file lag bytes, UDP drops,
SSH reconnects.

**Alert on these four**, which cover the failure modes that are otherwise
silent:

1. `ingest_unmatched_lines_ratio` rising — the spec has drifted from the log
   format; data is being lost quietly.
2. `ingest_lag_bytes` rising on a followed file — the pipeline is behind, or the
   store is refusing writes.
3. `store_open_iterators` trending up — an iterator leak, which also blocks
   space reclamation.
4. `store_l0_sublevels` approaching `l0_stop_writes_threshold` — writes are
   about to stall.

**Logs**: levelled (`0=none … 6=detail`), structured, with the
rule that any dropped datum is logged at most `N` times per minute per reason
with a running count, so a broken spec produces a summary rather than a
log-flood that is itself an outage.

## 6. Runbook: common situations

**"The panel is empty."** Check in order: (1) does the set exist — `SETS`;
(2) does the field exist and is it recent — `FIELDS FROM x`; (3) does the filter
match anything — the query returns `W201` when a value is not in the
dictionary; (4) is the time range inside the data's range — `CheckHealth`
reports it; (5) is ingest still matching lines —
`ingest_unmatched_lines_ratio`.

**"The graph shows a giant negative spike."** A counter reset rendered as a
delta. Add `CLAMP MIN 0 ELSE RAW`, or declare `limits` in the spec so it becomes
the default for that field.

**"The line is continuous through an outage I know happened."** No `GAP` on
that field. Set it, or declare `max_interval` in the spec.

**"Zooming out changes which spikes I see."** Expected and correct: extremes are
per window, and windows scale with the range. Zoom in to recover detail; the
extremes of a coarse window are always the extremes of some contained window
([07-downsampling.md](07-downsampling.md)).

**"Writes are failing with 503."** The store is shedding: every write slot is
busy, so it answers `503` with `Retry-After` rather than queueing. Check
`mensura_store_l0_sublevels` and raise `max_concurrent_writes` only if the
engine is keeping up. Ingest holds the batch and retries; no data is lost
until the sink's own buffer limit is reached, which it counts and logs.

**"Store will not start: storage version mismatch."** The data directory was
written by a different layout version. For batch deployments, wipe and
re-ingest. For streaming deployments, run the dual-run recipe
([05-storage.md §8](05-storage.md)).

**"Store will not start: directory locked."** Another process owns `data_dir` —
usually a standalone server plus a Grafana-spawned embedded plugin. Pick one
topology ([01-architecture.md §2](01-architecture.md)).

## 7. Backup

There is no built-in backup, on purpose. Two supported approaches:

1. **Re-ingestible sources** (batch deployments): the logs are the backup.
2. **Filesystem snapshot** of `data_dir` while the store is stopped, or a
   storage-layer snapshot with the store quiesced (`POST /v1/admin/quiesce`
   flushes memtables and blocks writes for the duration). Copying a live
   directory without quiescing yields an LSM in an unknown state; the docs say
   this plainly rather than implying it works.

## 8. Upgrades

- Ingest and store may differ in patch version freely; the wire major version is
  checked at connect and mismatches are refused.
- Store upgrades that change the storage layout ship with a dual-run recipe; all
  others are a restart.
- Plugin upgrades are a Grafana plugin update; in embedded mode that restarts
  the store, so a follow-mode ingest retries for the duration and resumes from
  its checkpoint.
