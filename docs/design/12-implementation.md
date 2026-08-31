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
| S3, SFTP and HTTP batch sources; nested archive unpacking | **not implemented**: local files, directories, globs, and single-file gzip/bzip2 (§6.3). A `.tar`/`.tgz`/`.zip` source is refused with an explanation rather than read as one stream |
| Percentile estimation from bucket sets, `AGGREGATE … BY` | **not implemented** (M5) |
| Streaming live tail, annotations, dashboard library, dashboard converter | **not implemented** (M6) |
| Spec reload on SIGHUP | **not implemented** (M4 remainder) |

## 6. Divergences from documents 01–11

Each entry states what the design says, what the code does, and why.

### 6.0 The label dictionary is global per key, not per set

[05](05-storage.md) §8 describes `_mensura_labels` as "one row per label key:
the ordered dictionary of its values", and [ADR-004](10-decisions.md) makes
the store the single writer of that dictionary. The implementation matches
that: `store.dict` is keyed on the label key alone, across every set.

This is a design property rather than a defect, but it has consequences worth
stating, because they are visible to operators:

- `MaxLabelCardinality` is one budget per label key for the whole store, so a
  high-cardinality set can consume the budget another set relies on.
- `GET /v1/labels?key=host` and the builder's label-value autocomplete return
  the values of `host` seen anywhere, not only in the set being queried.
  `LABELS host WHERE …` narrows this by scanning, and is the form to use when
  the answer must be set-scoped.
- A `WHERE` clause lowered against the dictionary may resolve values that
  cannot occur in the queried set. Results stay correct — a row can only carry
  the index it was written with — but a "matches no value" warning is
  store-wide rather than set-wide.

Making the dictionary per set would change the meaning of every index already
written to disk, so it is a storage-version migration and not a local fix.

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
single-file `.gz` and `.bz2` streams. Nested `tar`/`zip` walking and remote
object stores are not implemented.

A tar-bearing extension (`.tar`, `.tgz`, `.tar.gz`, `.tar.bz2`, `.tar.zst`,
`.tar.xz`, `.zip`) is **refused with an explanation**. It used to be
decompressed and handed to the line scanner, which fed tar headers — member
names, modes, padding — to the extractor as if they were log records. That
produces samples rather than an error, so an unpacked-looking import quietly
carried garbage and a misleading match rate. Refusing is the honest
behaviour until the walker exists.

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

### 6.13 The rotation fingerprint covers consumed bytes

Rotation is detected by hashing the head of the file. The hash was first
taken over "however many bytes exist", which for a file shorter than the
window changes on *every append* — so a young log was diagnosed as
rewritten on each poll and re-read from the start. A fixed 256-byte window
fixed that and introduced two problems of its own: rotated logs routinely
share their first 256 bytes (a startup banner, a templated first line), and
below the window there was no rotation detection but a size comparison,
which a copytruncate that restores the size defeats.

The window now covers the bytes *already consumed*, capped at 4 KiB, and
the checkpoint records the width it covers. Appending cannot change bytes
behind the read offset, so the hash is stable on a growing file, while a
truncate, a copytruncate or a rewrite in place changes it immediately at
any file size. The hash carries a `v2:` prefix; a checkpoint written by an
earlier build is still recognised against the legacy 256-byte window, so an
upgrade does not re-read every followed file.

### 6.14 An unsubstituted `$variable` is an error

A `$name` that reaches execution is compared against the dictionary as a
literal, matches nothing, and draws an empty panel with a warning. Since
the whole point of the project is that a plot may not mislead, validation
now rejects it with `E010`: an unsubstituted variable is a configuration
fault, not a value.

### 6.15 `key: offset` carries a key hint

[04](04-wire-protocol.md) §6 defines the `offset` row key as
`xxh3-128(set ‖ ts_ms ‖ canonical(labels) ‖ key_hint)`, where `key_hint` is
the stream identity and the byte offset. §6.12 carried the `key: offset`
declaration to the store; nothing ever supplied the hint itself, so the key
hashed the set, the timestamp and the labels and *nothing else*. That is
strictly less than content keying, which at least hashes the field values:
two records sharing a millisecond and a label set overwrote each other, the
write API answered `accepted: 3`, and one row was stored. The scheme whose
purpose is "every occurrence is a distinct row" did the opposite.

Every acquisition path now attaches a hint — stream plus the byte offset
the record started at, plus an index when one record yields several samples.
Batch import keys on the file path, follow on the stream id, remote follow
on `host:path`, and the receive listeners on their own arrival sequence,
which is the closest thing a datagram has to an offset.

The store refuses an offset-keyed sample that carries no hint, naming the
omission on the rejection rather than accepting it and storing one row for
two records. Any writer, not only this ingest, gets told.

### 6.16 Set-level spec options are persisted

The declarations of §6.12 travel *once per ingest process*, which means a
store that only held them in memory lost them on restart and the ingester,
whose `metaSent` map still said "sent", never repeated them. The set then
resolved to no retention, which makes `shardWidth` return zero, which routes
every subsequent write to the `@all` shard that retention skips by
construction. A `--retention 0` store with per-set retention declared by the
spec — the shape the flag exists for — silently stopped enforcing it and
grew without bound.

`retention_ms` and `shard_ms` are now recorded on the catalogue entry
alongside `key_scheme` and restored into the live overrides on open.

### 6.17 Auxiliary query forms are validated

`SETS`, `FIELDS`, `LABEL KEYS` and `LABELS` returned from a switch that ran
*before* `Validate`. For `LABELS … WHERE` that meant its predicate was never
shape-checked, and the tagged-union check in `arms` exists precisely
because the lowering in the store takes the first arm of a priority switch:
a node carrying both `and` and `eq` executed as the `and` and dropped the
`eq`, so a dashboard variable filtered by datacentre returned every
datacentre's hosts. Validation now runs first for every kind. A consequence
worth naming: `FIELDS FROM` and `LABEL KEYS FROM` an unknown set are now an
`E002` rather than an empty result.

Validation also gained the checks whose absence let an unrunnable query
render a plausible panel: an unknown `FORMAT` (the executor defaults to
timeseries), a non-positive `EVERY`, a negative `GAP`, an unknown `SSE`
mode (the render layer defaults to the constant), and an inverted or
mis-spelled `CLAMP`.

### 6.18 Remote follow detects rotation by size

An SSH follow counts bytes as they stream past. `tail -F` follows the file
across a rotation while that counter keeps climbing on the old one, so the
checkpoint ends up describing a file that never held those bytes and the
next reconnect seeks past the whole head of the new one. There was no
truncation detection on this path at all.

`ProbeInterval` — declared, defaulted and until now unused — is what closes
it: the remote length is read with `wc -c` before each connection and every
probe interval during one. A file shorter than the bytes already read has
been truncated, copytruncated or replaced, so the tail is dropped and the
stream restarts from the beginning. The same probe makes `--start-at end`
work on this path, which was accepted on the command line and ignored.

### 6.19 One framing for every acquisition path

`readRecord` bounds a record on the batch and local-follow paths. The
remote tail still used an unbounded `ReadBytes`, so one newline-free file
was pulled into memory whole, and the TCP listener still used a
`bufio.Scanner` capped at a megabyte — one longer line returned `ErrTooLong`,
which ended the read loop, closed the connection and discarded the rest of
the stream in silence. Both now frame with `readRecord`, and the follower
extracts the truncated prefix of an over-long record instead of dropping it,
so the same file yields the same samples however it was acquired.

### 6.20 Buffered extractor state is flushed on every path

`extract.Stream` holds open multiline records and half-filled aggregation
windows. The receive path never flushed either: a peer's state was dropped
outright when it was evicted, eviction only ran when a *new* peer arrived,
and nothing closed a window on a shutdown. The local follower had the same
hole at a rotation boundary — a truncate reset the reader without draining
the extractor, so the old file's tail was concatenated onto the new file's
first lines. Receive now runs an idle flush of its own and drains every peer
on the way out, and the follower drains before it re-reads.

A backwards timestamp inside a multiline record now *emits* the buffered
record rather than deleting it. Interleaved writers produce that ordering
routinely, and the loss was reported only as a counter.

### 6.21 A lost batch freezes only the streams it took bytes from

A dropped batch freezes a checkpoint, because a resume offset that moved
past a hole would bury the records in it. That freeze applied to every live
tailer and every followed remote path, and `holed` is never cleared, so one
bad record from one file stopped checkpoint progress for the whole process
until it was restarted. A stream whose in-flight mark had not moved past its
acknowledged offset cannot have contributed to the lost batch, and is now
left alone.

### 6.22 Metrics carry the same authorisation as the rest of the API

`/metrics` had no authentication in any mode, and the startup posture check
only forces that listener onto loopback when authentication is switched off
entirely. A bearer-mode store therefore published its set names, write rates
and disk usage to anyone who could reach the port. The metrics handler now
requires the `query` scope like every other read. **Operational note:** a
scrape against a `bearer`-mode store now needs a token with that scope.

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
- **Retention does not reclaim label dictionary entries.** There is one
  dictionary per label key for the whole store ([05](05-storage.md) §8,
  ADR-004) and every stored row holds an index into it, so a value dropped
  because one set aged out would relabel the rows of every other set that
  still carries it. A key's cardinality budget therefore only ever grows;
  recovering it needs the per-set dictionaries ADR-004 rejected.
- **No `AGGREGATE` clause**, so the `W301` warning about interleaved streams
  is the only mitigation for a query with no `BY`.
