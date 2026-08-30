# 12 — Implementation

This document tracks the code against the design: where things live, how to
build and run them, what is implemented, and every place the implementation
knowingly diverges from documents 01–11. It is updated with the code, not
after it.

## 1. Repository layout

```
cmd/mensura-store/      store, plugin and proxy modes; config, CLI, query client
cmd/mensura-ingest/     batch, follow, receive, check, query
internal/engine/        Pebble-backed sparse-column store (keyspace, codec, index, filters)
internal/store/         catalogue, dictionary, shard routing, write path, query engine, HTTP API
internal/ingest/        sink, batch import, follow with rotation, SSH follow, receivers, progress
internal/plugin/        Grafana backend datasource; local and proxy query services
pkg/model/              sample and value types, identifier rules, primary keys
pkg/wire/               API types and the HTTP client
pkg/mql/                lexer, parser, AST, printer, validator, diagnostics
pkg/render/             the ten-stage render walk
pkg/extract/            extraction spec, Aho-Corasick prefilter, timestamps, histograms
examples/specs/         example extraction specs
```

The dependency direction is one-way: `pkg/*` never imports `internal/*`, and
`internal/ingest` never imports `internal/engine` or `internal/store` — ingest
cannot open the database even by accident, which is ADR-001 enforced by the
compiler rather than by discipline.

## 2. Build and test

```bash
go build ./...
go test ./...            # unit and integration tests, ~10s
go vet ./...
gofmt -l ./cmd ./internal ./pkg
```

Binaries:

```bash
go build -o bin/mensura-store  ./cmd/mensura-store
go build -o bin/mensura-ingest ./cmd/mensura-ingest
```

Requires Go 1.24 or newer. The only heavyweight dependencies are Pebble
(engine) and the Grafana plugin SDK (backend datasource).

## 3. End-to-end run

```bash
# 1. Start a store. Loopback with auth off is allowed; a non-loopback bind
#    without auth is refused at startup.
mensura-store --data-dir /var/lib/mensura --listen-write 127.0.0.1:9631 \
              --durability batch --retention 0

# 2. Check a spec against a real file before importing anything.
mensura-ingest check --spec examples/specs/appserver.yaml --sample /logs/web1/app.log

# 3. Import.
mensura-ingest batch --spec examples/specs/appserver.yaml \
                     --source /logs --label dc=eu-west-1

# 4. Query.
mensura-ingest query --from 24h \
  'FROM app SELECT requests_total RATE AS "req/s" BY host, pool'

# Or follow, or receive:
mensura-ingest follow  --spec examples/specs/appserver.yaml --path '/var/log/app/*.log'
mensura-ingest receive --spec examples/specs/appserver.yaml --listen-tcp :9640 --mode metrics
```

## 4. Test coverage of the design's claims

| Claim | Where it is tested |
| --- | --- |
| Render properties C1–C8 ([07](07-downsampling.md) §7) | `pkg/render/render_test.go`, property tests over randomised series plus a brute-force differential check for C4 |
| Padding never reorders output | `pkg/render/render_test.go:TestPaddingNeverReordersAgainstAdjacentNull` |
| MQL AST round-trips losslessly ([06](06-query.md) §7) | `pkg/mql/mql_test.go:TestRoundTrip` |
| Modifier order is canonical (ADR-006) | `pkg/mql/mql_test.go:TestModifierOrderIsCanonical` |
| Diagnostics fire with the documented codes ([06](06-query.md) §12) | `pkg/mql/mql_test.go:TestValidate` |
| Strict label matching (ADR-007) | `internal/store/store_test.go:TestStrictLabelMatching` |
| An unknown value empties the result and warns | `…:TestFilterOnUnknownValueReturnsEmptyWithWarning` |
| Field metadata supplies gap and clamp defaults (ADR-010) | `…:TestFieldMetadataDrivesDefaults` |
| Time sharding and shard-drop retention (ADR-003) | `…:TestTimeShardingAndRetention` |
| Idempotent writes and content-addressed replay (ADR-005) | `…:TestIdempotentWrite` |
| Cardinality guard is loud and early | `…:TestCardinalityGuard` |
| Reserved sets rejected, ingest set exempt | `…:TestReservedSetsRejected` |
| Safety gates return partial results plus a reason | `…:TestSafetyGateReturnsPartialResults` |
| Catalogue and dictionary survive a restart | `…:TestReopenKeepsCatalogue` |
| Rotation loses nothing (rename+create, delete+create, copytruncate) | `internal/ingest/follow_test.go:TestFollowAcrossRotationStyles` |
| Follow resumes from the acknowledged offset | `…:TestFollowResumesFromCheckpoint` |
| A shedding store costs no data | `…:TestFollowSurvivesStoreShedding` |
| Line protocol parsing, quoting, escaping, rejection | `internal/ingest/receive_test.go` |
| An idempotency key is spent only by a write that committed | `internal/store/regression_test.go:TestIdempotencyKeySurvivesAFailedWrite` |
| A label named `timestamp` is rejected, not silently hidden | `…:TestTimestampLabelIsRejected` |
| `LIMIT SERIES` binds at execution, not only at validation | `…:TestLimitSeriesIsEnforced` |
| A hostile `bucket_index` is bounded, not allocated | `…:TestHostileBucketIndexIsBounded` |
| Catalogue reads and writes do not race | `…:TestConcurrentCatalogueReadAndWrite` |
| A shard suffix carries the width it was written with | `…:TestShardSuffixCarriesItsWidth` |
| Per-set retention sweeps without a global default | `…:TestPerSetRetentionSweepsWithoutAGlobalDefault` |
| The spec's `sets:` block reaches the store | `…:TestSetMetaIsApplied` |
| The debug API refuses a non-loopback peer | `…:TestDebugHandlerRefusesNonLoopback` |
| A line read before its newline arrives is delivered once, intact | `internal/ingest/regression_test.go:TestFollowDeliversASplitLineOnceAndIntact` |
| A file growing past the fingerprint window is not re-read | `…:TestFollowDoesNotReReadAGrowingShortFile` |
| A genuine rewrite in place is still detected | `…:TestFollowStillDetectsARewriteInPlace` |
| Printed MQL always re-parses (large floats, unicode, regexes) | `pkg/mql/regression_test.go:TestRoundTripSurvivesExtremeNumbersAndUnicode` |
| An unsubstituted `$variable` is an error | `…:TestUnsubstitutedVariableIsAnError` |
| Extraction: patterns, labels vs fields, multiline, routes, buckets, aggregation | `pkg/extract/extract_test.go` |

## 5. Implementation status

| Area | Status |
| --- | --- |
| Engine: keyspace, TLV codec, covering index, pushdown, projection, snapshot iterators, shard drop, compaction, stats, tunings, storage version | implemented |
| Store: catalogue, label dictionary, shard routing, retention sweep, write path, idempotency, cardinality guard | implemented |
| Query: planning, pushdown, grouping, render, safety gates, `Explain`, `timeseries`/`table`/`logs`/`heatmap`, `SETS`/`FIELDS`/`LABELS`/`LABEL KEYS` | implemented |
| Render pipeline: all ten stages, null slots, SSE, window formula, `EVERY` | implemented |
| MQL: lexer, parser, printer, validator, diagnostics, JSON AST | implemented |
| Extraction: profiles, timestamp formats and caching, multiline, replace, routes, default values, bucket sets with cumulative/tail, aggregation, identity discovery, `check` | implemented |
| Ingest: batch, follow with rotation and checkpoints, SSH follow, TCP/UDP/HTTP receive, progress reporting | implemented |
| HTTP API: write, query, catalogue, labels, stats, parse/print, admin compact/retention/quiesce/drop-set, loopback debug plan, Prometheus metrics, bearer auth | implemented |
| Plugin backend: `QueryData`, `CheckHealth`, `CallResource`, one frame per series, alert-mode `SSE OFF` | implemented |
| Plugin frontend: React builder, Monaco code mode, variable editor, packaging and signing | **not implemented** (M3/M6) |
| Protobuf wire encoding, zstd | **not implemented**: v1 speaks JSON with gzip (§6.1) |
| mTLS, per-client rate limits | **not implemented**: bearer auth and server TLS only; back-pressure is by write-slot shedding (§6.2) |
| S3, SFTP and HTTP batch sources; nested archive unpacking | **not implemented**: local files, directories, globs, and single-file gzip/bzip2 (§6.3) |
| Percentile estimation from bucket sets, `AGGREGATE … BY` | **not implemented** (M5) |
| Streaming live tail, annotations, dashboard library, dashboard converter | **not implemented** (M6) |
| Spec reload on SIGHUP | **not implemented** (M4 remainder) |

## 6. Divergences from documents 01–11

Each entry states what the design says, what the code does, and why.

### 6.1 The wire encoding is JSON, not protobuf

[04](04-wire-protocol.md) specifies protobuf by default with NDJSON as the
debuggable alternative, and zstd compression. The implementation sends JSON
with gzip.

*Why*: protobuf needs a generated-code toolchain in the build, and zstd needs
a third dependency, for a saving that has not been measured yet. The field
names, types and semantics on the wire are exactly those in
[04](04-wire-protocol.md) §3.1, so switching the encoding later is a codec
change behind `pkg/wire`, not a protocol change. The benchmark gate in
[11](11-roadmap.md) §2.6 is what should decide it.

*Consequence for operators*: request bodies are larger than the design
implies. On loopback, `--compress=false` avoids paying for compression that
buys nothing.

### 6.2 Auth is bearer-only, and rate limiting is shedding

[04](04-wire-protocol.md) §2 and [09](09-operations.md) §4 describe mTLS and
per-client rate limits. The implementation has bearer tokens (SHA-256 hashes,
constant-time compare, per-scope), server TLS, and a bounded write-slot pool
that returns `503` with `Retry-After` when full.

*Why*: the shedding path is what actually protects the store, and it is
implemented. mTLS and token-bucket limits are configuration surface that can
be added without changing any semantics.

*Consequence*: `auth.mode: mtls` is not accepted yet, and
`listen.*.tls.client_ca` is *refused* at startup rather than accepted and
ignored — an operator must not be able to believe client certificates are
being verified when they are not. A non-loopback listener with auth
disabled is refused at startup rather than quietly serving an open write
API, and a hostname that is not a loopback literal counts as non-loopback
because it may resolve anywhere. The debug listener has no credential of
its own in any mode, so it is refused on a non-loopback bind and
additionally rejects any non-loopback peer at request time.

### 6.3 Batch sources are local only

[02](02-ingest.md) §4.1 lists S3, SFTP, HTTP and nested-archive sources. The
implementation resolves local files, directories and globs, and decompresses
single-file `.gz`, `.tgz` and `.bz2` streams. Nested `tar`/`zip` walking and
remote object stores are not implemented.

*Why*: the acquisition interface takes a list of readable paths, so a source
is a small adapter in front of it. Getting the pipeline, rotation and delivery
semantics right mattered more than the number of download adapters.

*Consequence*: fetch a bundle with the operator's usual tool and point
`--source` at the unpacked directory. Note that binary sniffing runs on
*decompressed* bytes: sniffing the compressed stream would classify every
`.bz2` as binary and skip it, which is what happened before.

### 6.4 Heatmaps sum, they do not run the render walk

[06](06-query.md) §6 describes `FORMAT heatmap` without saying how bucket
counts are reduced. The implementation aggregates each bucket's counts by
**sum per window**, anchored on a wall-clock-aligned window boundary, rather
than running the min/max walk.

*Why*: min and max are the right summary for a line, where the question is
"what was the extreme"; a heatmap column asks "how many fell in this bucket",
which is a sum. Running the extremes walk over bucket counts would understate
totals at every zoom level.

*Status*: this is a deliberate refinement of the design; [06](06-query.md) §6
should be read with it.

### 6.5 SSE padding is clamped into the available gap

[07](07-downsampling.md) §5 and the whitepaper use a flat ±500 ms offset for
singular-series padding. The implementation clamps each padding point into the
space between the real point and its neighbours.

*Why*: gap detection injects its null at `ts − 1`. A flat −500 ms padding
point next to that null lands *before* it, which breaks the
strictly-increasing-time guarantee (C1) that the same specification asserts.
Clamping keeps the padding as wide as it can be without reordering anything;
where there is no room, the padding point is omitted rather than emitted out
of order. Covered by `TestPaddingNeverReordersAgainstAdjacentNull`.

### 6.6 Field names may start with a digit

[05](05-storage.md) §12 originally gave field names the same charset as set
names and label keys. Histogram bucket columns are conventionally `00`…`23`
plus `03plus`, which that rule rejected — the store rejected every histogram
sample the example spec produced. Field names now allow a leading digit; set
names and label keys are unchanged, and such a field must be quoted in MQL
(`SELECT "00"`). The document has been corrected.

### 6.7 Checkpoint acknowledgement is per flush, not per stream

[02](02-ingest.md) §5 describes `acked_offset` advancing to the highest byte
offset covered by acknowledged samples. The implementation advances every
followed file's acknowledged offset to its read offset after a successful
flush, because one flush carries samples from many streams.

*Why*: exact per-stream accounting inside a shared batch would need per-sample
provenance on the wire. The effect on the contract is a slightly larger replay
window after an unclean stop (at most one flush interval), which
at-least-once delivery and content-addressed keys already absorb.

The rule is still "only acknowledged bytes move the resume point", and that
now holds on both follow paths: a tailer's read offset is promoted to
`pending` only once every sample from those bytes has reached the sink, and
`pending` becomes `acked` only in the flush callback. The SSH follower
follows the same rule rather than adding its consumed byte count
unconditionally, and counts bytes from the raw framing so a CRLF stream or
an unterminated final line cannot drift the offset across a reconnect.
Checkpoints are fsynced before the rename and the directory after it.

### 6.8 Compression, storage profiles and Pebble

`Compression: balanced` is implemented directly against the Pebble version in
use: the cheapest codec on the upper levels and zstd on the bottom two, rather
than a named Pebble profile. `network-fs` sets the file-size, sync, LBase, L0
and bloom knobs described in [05](05-storage.md) §9.

### 6.9 Idempotency window is a bounded LRU

[04](04-wire-protocol.md) §3.2 describes a 10-minute idempotency window. The
implementation remembers the most recent 4 096 request keys. Under steady load
that is a shorter window; when idle it is a longer one. Content-addressed row
keys make an expired key harmless, which is why the bound is on memory rather
than on time.

### 6.10 Shard suffixes carry their own width

[05](05-storage.md) §5 describes a configurable shard width. The suffix
originally encoded only the timestamp, and the width was inferred from the
suffix *length* — so every width below a day behaved as one hour and every
width at or above a day behaved as one day. A `shard: 6h` silently produced
hourly shards, and retention then measured them as an hour wide.

The suffix now carries the width for anything but the two original
granularities: `set@2026082812-6h`, `set@20260828-7d`. The bare
`set@2026082812` and `set@20260828` forms still parse as one hour and one
day, so a directory written by an earlier build opens unchanged.

Widths are snapped to what the encoding can express — whole days, or an
hour count that divides a day — and a width that is not exactly
representable is reported at startup rather than rounded in silence.

### 6.11 Label keys may not be named `timestamp`

Labels and fields share one column namespace on a stored row.
`ValidateFieldName` already reserved `timestamp`; `ValidateLabelKey` did
not, so a label with that name overwrote the indexed column with a
dictionary index and put the row at a time no range scan could reach — the
sample was accepted and then invisible. The name is now reserved on both
sides and such a sample is rejected by name.

### 6.12 Set-level spec options travel on the write API

[02](02-ingest.md) §3 has the spec declare per-set retention, shard width
and key scheme. Nothing carried them to the store, so the `sets:` block was
parsed and ignored — `key: offset` in particular could never take effect,
and every set was content-keyed.

`WriteRequest` now carries `set_meta` alongside `field_meta`, on the same
"sent once per process start, not per sample" footing. Note that the store
decodes write bodies with `DisallowUnknownFields`, so an ingest of this
version against an older store is refused rather than silently dropping the
declarations.

### 6.13 The rotation fingerprint uses a fixed window

Rotation is detected by hashing the head of the file. The hash was taken
over "however many bytes exist", which for a file shorter than the window
changes on *every append* — so a young log was diagnosed as rewritten on
each poll and re-read from the start.

The window is now a fixed 256 bytes, and a fingerprint taken over a
shorter file is marked provisional and not compared. Below the window only
a shrinking file counts as a rewrite; once the file is long enough the
fingerprint is adopted and rewrites are detected as before.

### 6.14 An unsubstituted `$variable` is an error

A `$name` that reaches execution is compared against the dictionary as a
literal, matches nothing, and draws an empty panel with a warning. Since
the whole point of the project is that a plot may not mislead, validation
now rejects it with `E010`: an unsubstituted variable is a configuration
fault, not a value.

## 7. Known gaps worth naming

- **No frontend.** The plugin backend answers Grafana correctly, but until the
  React editor exists a panel must carry the AST in its query model. The
  backend's `parse`/`print` resources exist precisely so the frontend never
  implements a second parser.
- **`route:` can create unbounded sets.** Open question 3 in
  [11](11-roadmap.md) is unresolved; there is no `max_sets` guard yet.
- **`store_stream_label:` is refused, not ignored.** The key was accepted by
  the spec decoder and acted on nowhere. Compiling a spec that sets it now
  fails, on the same principle as `tls.client_ca`: a declaration that does
  nothing is worse than one that is rejected.
- **Sub-millisecond timestamps are truncated**, per open question 1.
- **No `AGGREGATE` clause**, so the `W301` warning about interleaved streams
  is the only mitigation for a query with no `BY`.
