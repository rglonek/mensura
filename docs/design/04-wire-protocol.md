# 04 — Wire protocol

Two APIs are specified here: the **write API** (ingest → store) and the
**query API** (plugin-in-proxy-mode → store). Both are HTTP/2 over TLS with the
same auth and error conventions. The ingest binary additionally accepts
inbound traffic from third parties in receive mode; that is a different,
looser protocol and is specified in §7.

```
mensura-ingest ──POST /v1/write──►  mensura-store  ◄──POST /v1/query── plugin (proxy mode)
                                          ▲
                                          └── in-process call ── plugin (embedded mode)
```

## 1. Versioning

The path carries the major version (`/v1/…`). Within a major version:

- new optional fields may be added to requests and responses;
- unknown fields in a *request* are rejected (`400`), because a silently ignored
  field produces wrong-but-plausible results;
- unknown fields in a *response* are ignored by clients.

Each side reports its build and protocol version in `/v1/hello`, and the ingest
write client refuses to start against a store with an incompatible major.

## 2. Authentication

| Mechanism | Use |
| --- | --- |
| Bearer token | Default. A shared secret per client, configured on the store as a list of `{name, hash, scopes}`. Compared in constant time. |
| mTLS | Preferred for production. Client certificate subject/SAN maps to a client name and scopes via a configured mapping. |
| None | Only permitted when the listener is bound to loopback, and even then it must be explicitly enabled (`auth: none`). Refusing to start otherwise is deliberate. |

Scopes: `write`, `query`, `admin`. Tokens are never logged; the store logs the
client *name*, never the credential.

## 3. `POST /v1/write`

### 3.1 Request

```
POST /v1/write HTTP/2
Content-Type: application/x-mensura-batch    (or application/x-ndjson)
Content-Encoding: zstd                        (or none, gzip)
Idempotency-Key: 0f9a…                        (32 hex chars, xxh3-128 of the uncompressed body)
X-Mensura-Client: web1-nginx
Authorization: Bearer …
```

Body (protobuf; the NDJSON form is field-for-field identical):

```protobuf
message WriteRequest {
  repeated FieldMeta   field_meta = 1;   // optional, see §5
  repeated SampleBatch batches    = 2;
}

message SampleBatch {
  string          set     = 1;           // destination set, e.g. "http"
  repeated Sample samples = 2;
}

message Sample {
  int64                ts_ms    = 1;     // milliseconds since epoch, required
  map<string, string>  labels   = 2;     // stream + discovered labels
  map<string, Value>   fields   = 3;     // measurements
  bytes                key_hint = 4;     // optional, see §6
}

message Value {
  oneof v { int64 i = 1; double f = 2; string s = 3; bool b = 4; bytes raw = 5; }
}
```

Notes:

- `labels` are sent as strings. Interning is the store's job (§4), because the
  dictionary must have a single writer.
- Repeating the label map per sample is intentional at v1: it is what makes the
  protocol stateless and retries trivially safe. It compresses extremely well
  (zstd sees the same map thousands of times) and is measured, not assumed —
  see [11-roadmap.md](11-roadmap.md) for the dictionary-delta optimisation that
  is deferred until the numbers justify it.
- A batch may not exceed `max_request_bytes` (default 32 MiB uncompressed);
  `413` otherwise.

### 3.2 Response

```json
{
  "accepted":  4096,
  "duplicate": false,
  "rejected":  [ {"index": 17, "reason": "label 'req_id' exceeds cardinality limit"} ],
  "catalogue_version": 128
}
```

- `200` — the batch is committed (memtable + WAL if enabled). Partial rejection
  is reported in `rejected[]`; the accepted remainder is committed.
- `200` with `duplicate: true` — this `Idempotency-Key` was already committed;
  nothing was written. Keys are remembered for `idempotency_window` (default
  10 min) in a bounded LRU.
- `400` — malformed body, unknown field, bad timestamp.
- `401`/`403` — auth.
- `413` — too large.
- `422` — unknown set while `strict_sets` is on.
- `429` — over the client's rate/burst budget. `Retry-After` set.
- `503` — the store is shedding (write queue full, or shutting down).
  `Retry-After` set.

`429` and `503` are the backpressure signals; the write client treats them as
retryable and slows down. Everything in the `4xx` range other than `429` is
fatal for that batch (§9 of [02-ingest.md](02-ingest.md)).

### 3.3 Ordering

The protocol makes **no ordering guarantee across batches**, and the store
requires none: every sample carries its own timestamp, and the index is by
timestamp, not by arrival. Ingest preserves per-stream order for its own
checkpoint accounting, not for the store's benefit.

## 4. Label interning

The store owns the dictionary. On write:

1. For each label key, look up the value in a hot in-memory map.
2. On miss, take the per-key write lock, assign the next index, append to the
   dictionary record, and publish a new immutable snapshot (copy-on-write, so
   readers never lock).
3. Store the integer index in the row's column.

Consequences:

- The wire stays string-based, so clients are stateless and dumb.
- Query-time filters become integer equality, which is what makes predicate
  pushdown cheap (see [05-storage.md §6](05-storage.md)).
- Cardinality limits are enforced at the one place that can see the whole
  picture.

`GET /v1/labels?key=host` exposes the dictionary for a key, and
`GET /v1/catalogue` exposes sets, fields, field metadata and bucket sets. The
plugin's builder is driven entirely by those two endpoints.

## 5. Field metadata

`field_meta` may be included on any write and is typically sent once per
process start and whenever the spec reloads:

```protobuf
message FieldMeta {
  string set = 1; string field = 2;
  Kind   kind = 3;                    // COUNTER | GAUGE | DELTA | STRING
  string unit = 4; string unit_hint = 5; string description = 6;
  int32  max_interval_s = 7;
  Limits limits = 8;                  // {min, max, replace_with_raw}
  string bucket_set = 9;              // membership, for heatmaps
  int32  bucket_index = 10;
  double bucket_lower_edge = 11;
}
```

The store merges by `(set, field)`, last writer wins, and bumps
`catalogue_version` so the plugin's cache can refresh cheaply
(`If-None-Match`).

Conflicting metadata from two ingesters (one says counter, one says gauge) is
resolved last-writer-wins **and recorded**: `GET /v1/catalogue` reports
`conflicts[]`, and the plugin shows a warning in the builder. Silent
disagreement is the thing to avoid; picking a winner is fine.

## 6. Primary keys and deduplication

The store assigns each row a primary key. Two schemes, selected per set (spec
`key:`, default `content`):

| Scheme | Key | Property |
| --- | --- | --- |
| `content` (default) | `xxh3-128(set ‖ ts_ms ‖ canonical(labels) ‖ canonical(fields))` | Re-ingesting identical data is a no-op. Two identical samples in the same millisecond from the same stream collapse into one row. |
| `offset` | `xxh3-128(set ‖ ts_ms ‖ canonical(labels) ‖ key_hint)` where `key_hint` is `(stream_id, byte_offset)` | Every occurrence is a distinct row. Replays after an unclean stop may duplicate rows for the replayed span. |

The idempotency win (re-running ingest does not double-count) and the known
collapse of identical lines are two faces of the same property; `offset` keying
is the opt-out for streams where occurrences must be counted individually.

Collision probability for xxh3-128 at 1 TiB of typical log-derived rows is
≈1.5 × 10⁻¹⁹ — orders of magnitude below the host's other failure modes.

## 7. Receive-side line protocol (third parties → ingest)

Deliberately simple and human-typable; it is the interface a shell script or an
existing shipper uses.

```
# metrics mode
<set> <label>=<value>[,<label>=<value>…] <field>=<num>[,<field>=<num>…] <ts_ms>
http host=web1,dc=eu1 requests_total=91823,inflight=7 1756400000000

# logs mode: the raw line, extracted by the spec exactly like a tailed file
2026-08-28 10:41:02.113 stats: pool=default reqs=91823 inflight=7 heap=8123456
```

- Timestamp is optional in metrics mode; when absent the receiver stamps
  arrival time and marks the sample `ts_source: receiver` so a dashboard can
  tell the difference.
- Escaping: `\` escapes `,`, `=`, space and `\` inside values; values may be
  double-quoted.
- Type inference: integers stay integers, anything with `.`/`e` becomes a float,
  quoted values are strings.
- Malformed lines are counted and (up to a cap) sampled into the error log; one
  bad line never kills a connection.

An HTTP equivalent (`POST /ingest/v1/samples`) takes the same content as JSON,
for callers who would rather not implement a line format.

## 8. `POST /v1/query`

Used by the plugin in proxy mode and by `mensura-ingest query`.

```json
{
  "ast":         { "…MQL AST…" },
  "from_ms":     1756396400000,
  "to_ms":       1756400000000,
  "max_points":  1200,
  "interval_ms": 15000,
  "format":      "timeseries",
  "options":     {"disable_series_safety": false, "disable_size_safety": false}
}
```

Response is a series list in the store's own frame-shaped encoding (protobuf
`QueryResponse`), which the plugin converts to `data.Frame`s without
re-deriving anything:

```protobuf
message QueryResponse {
  repeated Series series  = 1;
  Stats           stats   = 2;   // rows scanned, series, points, truncation flags
  repeated string warnings = 3;  // e.g. "counter plotted without RATE"
  string          error    = 4;  // partial-result error, see §9
}
message Series {
  string             name    = 1;
  map<string,string> labels  = 2;
  repeated int64     ts_ms   = 3;
  repeated double    values  = 4;
  repeated bool      is_null = 5;   // parallel to values; true = connect-break
  FieldMeta          meta    = 6;
}
```

`is_null` is a parallel array rather than a NaN sentinel because NaN is a
legitimate value in some sources and because "no data" must be a distinct
concept all the way to the renderer (whitepaper P2).

## 9. Partial results

A query that trips a safety gate returns `200` with both `series` (what was
collected so far) and `error` set — never a bare error: an operator narrowing
filters wants to see the
partial shape plus the reason, not an empty panel. The plugin surfaces `error`
as a panel-level notice and still draws the frames.

## 10. Admin endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/hello` | Versions, mode, uptime, data dir, catalogue version |
| `GET /v1/catalogue` | Sets, fields, metadata, bucket sets, conflicts; `ETag` |
| `GET /v1/labels?key=` | Dictionary values for a label key |
| `GET /v1/stats` | Engine stats: puts, scans, iterators, LSM metrics, disk usage |
| `POST /v1/admin/compact` | Full-keyspace compaction (used at end of batch ingest) |
| `POST /v1/admin/retention/run` | Force a retention sweep |
| `DELETE /v1/admin/sets/{name}` | Drop a set (scope `admin`) |
| `GET /v1/debug/plan` | Explain a query: shards touched, index range, pushdown expression, projection |
| `GET /metrics` | Prometheus exposition |

`/v1/debug/*` binds to the loopback listener only: the debug surface is never
reachable from whatever proxies the public path.
