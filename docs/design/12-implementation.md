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
| A non-finite field value is refused, not encoded | `pkg/model/regression_test.go:TestNonFiniteFieldValueIsRejectedByName` |
| Two label sets that differ only in framing bytes stay two rows | `…:TestPrimaryKeySeparatesAmbiguousLabelSets`, `internal/store/regression_test.go:TestAmbiguousLabelSetsStayDistinctRows` |
| `field_meta` cannot forge a set name | `internal/store/regression_test.go:TestFieldMetaCannotForgeASetName` |
| Re-declared metadata does not churn the catalogue version | `…:TestRedeclaringFieldMetaDoesNotChurnTheCatalogueVersion` |
| One unencodable sample does not lose its batch | `internal/ingest/regression_test.go:TestUnencodableSampleDoesNotPoisonItsBatch` |
| A rejected credential holds the batch rather than dropping it | `…:TestRejectedCredentialHoldsTheBatchInsteadOfDroppingIt` |
| The read offset stays before an undelivered record | `…:TestFollowLeavesTheOffsetBeforeAnUndeliveredRecord` |
| Every received sample carries a key hint | `…:TestReceivedSamplesCarryAKeyHint` |
| An evicted peer is drained, not dropped | `…:TestEvictedPeerIsFlushedNotDropped` |
| Unimplemented spec keys are refused | `pkg/extract/regression_test.go:TestUnimplementedFramingOptionsAreRefused`, `…:TestUnknownAggregateModeIsRefused` |
| Closed windows survive an aggregation error | `…:TestExpiredWindowsSurviveAnAggregationError` |
| An absent `FORMAT` validates as a timeseries | `pkg/mql/regression_test.go:TestAbsentFormatIsValidatedAsTimeseries` |
| A client-input fault is a 400, an engine failure is still a 500 | `internal/store/regression_test.go:TestClientInputFaultsAreNotServerErrors`, `…:TestEngineFailuresAreStillServerErrors` |
| Retry exhaustion requeues; only a full buffer drops | `internal/ingest/regression_test.go:TestRetryExhaustionRequeuesRatherThanDropping`, `…:TestAFullBufferStillDropsAndSaysSo` |
| A frozen checkpoint thaws when delivery recovers | `…:TestAFrozenCheckpointThawsWhenDeliveryRecovers`, `…:TestRotationClearsAFrozenCheckpoint` |
| Overlapping `--source` entries are read once | `…:TestResolveDeduplicatesOverlappingSources` |
| A negative duration is a spec error, sub-second cadence survives | `pkg/extract/regression_test.go:TestNegativeSetDurationsAreASpecError`, `…:TestSubSecondMaxIntervalIsKept` |
| `field_meta` carries a sub-second cadence | `internal/store/regression_test.go:TestFieldMetaCarriesSubSecondCadence` |
| The query listener carries no write or admin surface | `…:TestQueryHandlerCarriesNoWriteOrAdminSurface` |
| `LABELS … WHERE` is gated, and table truncation is visible | `…:TestLabelsFilterScanIsBounded`, `…:TestTabularTruncationIsVisible` |
| A variable inside a regex is diagnosed | `pkg/mql/regression_test.go:TestUnsubstitutedVariableInARegexIsDiagnosed`, `…:TestVariablesFindsRegexReferences` |
| `HAS`/`MISSING` check the label name | `…:TestHasAndMissingCheckTheLabelName` |
| A fractional integer is a positioned parse error | `…:TestFractionalIntegerIsAPositionedParseError` |
| A partial histogram is not a row of zeros | `pkg/extract/regression_test.go:TestPartialHistogramDoesNotInventZeroBuckets`, `…:TestPartialHistogramSkipsDerivedColumns` |
| A trailing gap draws a connect-break | `pkg/render/render_test.go:TestTrailingGapDrawsAConnectBreak`, `…:TestNoTrailingBreakWhenTheCadenceIsHonoured` |

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

The freeze also ends without a restart. The first delivery that succeeds
afterwards clears `holed`, pulls the stream's pending and in-flight marks
back to its acknowledged offset, and asks the reader to seek there: locally
the poll goroutine re-seeks the file, remotely the tail is dropped so the
reconnect re-issues `tail -c` from the frozen offset. A successful flush is
the signal rather than a timer, because it proves the store is accepting
writes and cannot fire while the sink is still failing. What this does is
exactly what a restart did — including discarding the extractor's buffered
state, which was built from the bytes about to be read again — so the
duplicates it produces are the ones a content-addressed key already
collapses.

### 6.22 Metrics carry the same authorisation as the rest of the API

`/metrics` had no authentication in any mode, and the startup posture check
only forces that listener onto loopback when authentication is switched off
entirely. A bearer-mode store therefore published its set names, write rates
and disk usage to anyone who could reach the port. The metrics handler now
requires the `query` scope like every other read. **Operational note:** a
scrape against a `bearer`-mode store now needs a token with that scope.

### 6.23 A non-finite field value is a rejection, not a poisoned batch

`strconv.ParseFloat` accepts `NaN`, `Inf` and `+Infinity`, so `Coerce`
turned any of those tokens in a log line into a `float64` that
`encoding/json` refuses to marshal. The failure landed on the *request*
rather than the sample: the whole batch became undeliverable, every retry
failed the same way, and because an undeliverable batch is reported to the
delivery observers as a lost one, it froze every followed file's
checkpoint for the life of the process — after which a restart re-read the
same line and did it again. One log line stopped a pipeline permanently.

`Sample.Validate` now rejects a non-finite value by name, and the sink
screens a sample for encodability before it enters a buffer, counting what
it refuses in `SinkStats.Unencodable`. `Value.MarshalJSON` names the value
rather than leaving `encoding/json` to say `unsupported value: NaN` with
no field attached. A batch that cannot be encoded is also classed as
fatal rather than as an exhausted retry, so a caller that builds one by
hand is told it is malformed.

### 6.24 Row-key components are length-prefixed

[04](04-wire-protocol.md) §6 derives the content key from the set, the
timestamp, the canonical labels and the field values. The implementation
hashed each label as `key '=' value 0x00`, with no length prefix — and a
label value is any valid UTF-8, so it may contain both of those framing
bytes. `{a: "b", c: "d"}` and `{a: "b\x00c=d"}` therefore produced the
same 16 bytes. `PutBatch` assumes a key is either new or carries the same
indexed value, so the second sample silently overwrote the first while the
write API reported both as accepted. The same held for string field values
on the content path.

Every variable-length component now carries a uvarint length, which cannot
be forged from content. *Consequence*: content keys computed by this build
differ from those computed by an earlier one. Nothing on the read path
compares them, so existing rows stay queryable and existing shards open
unchanged; the only effect is that re-ingesting data first written by an
older build creates a second row rather than collapsing onto the first.

### 6.25 `field_meta` is validated exactly as a batch is

`applySetMeta` checked the set name and the reserved prefix;
`applyFieldMeta` checked neither, and `entryLocked` creates whatever it is
given. Any client holding the `write` scope could therefore put a reserved
name — or one carrying the `@` that separates a set from its shard suffix
— into the catalogue. The entry was persisted, served from
`/v1/catalogue`, accepted by the MQL validator as a real set, and could
not be removed through `DELETE /v1/admin/sets/`, which does validate. A
name containing `@` additionally broke the logical/shard split in
`shardsByLogical`.

Set names, the reserved prefix (with the documented `_mensura_ingest`
exception) and field names are now checked before anything is recorded,
and the request is refused rather than partially applied.

The same function also bumped `catalogue_version` unconditionally. Since
metadata travels on every ingest process start, that reintroduced exactly
the ETag churn `CatalogueVersion` exists to avoid. It now moves only when
the schema actually changed.

### 6.26 A rejected credential holds the batch instead of dropping it

Every non-retryable status was `ErrFatal`, which the sink treats as a
malformed batch: dropped, counted, and reported to the observers as a
hole — so a rotated or mistyped token destroyed up to `MaxFatalDrops`
batches *and* froze every checkpoint permanently. The batch was never the
problem.

`401` and `403` are now `wire.ErrAuth`, which is neither fatal nor
retryable: the client returns at once (retrying the same rejected token in
the same request is pointless), and the sink puts the batch back in its
buffer, tells no observer anything, and holds delivery for 30 seconds
before trying again. `Close` lifts the hold for one final attempt.

*Consequence*: a long authentication outage grows the sink's buffer rather
than shedding it, which is the same trade the cancelled-context path
already makes. §7 names it.

### 6.27 Declared-but-unimplemented spec keys are refused

`framing.record: json` was accepted and every record was still framed by
line. `timestamp.on_parse_error: fail` and `drop-stream` were accepted and
the error was still merely counted. Both are the shape §6.2 and §7 already
refuse for `store_stream_label` and `listen.*.tls.client_ca`: a
declaration that does nothing is worse than one that is rejected, because
an operator cannot tell the difference from the outside. They are now
compile errors naming what is and is not implemented, as is an unknown
`timestamp.anchor`.

`aggregate.mode` was validated nowhere. The accumulator's update switch
has no default, so an unrecognised mode — `avg` is the obvious typo — kept
whichever value the first record of a window carried and ignored every
later one, drawing a flat and entirely plausible series. The four
implemented modes are now the only ones that compile, and a non-positive
`every` is refused with them.

### 6.28 `check` frames the way the acquisition paths do

§6.19 put every acquisition path on one framing. `mensura-ingest check`
was not one of them: it still used a `bufio.Scanner` capped at a
megabyte, so a single longer line failed it with `ErrTooLong` and it
reported nothing, on a file the import would have read to the end with the
record truncated and counted. The tool whose purpose is to predict the
import now uses the same `ReadRecord`, and reports the unjoined-
continuation count alongside the other rates.

### 6.29 A record's bytes are consumed only once its samples are queued

The local follower advanced `tailer.offset` past a record before handing
that record's samples to the sink. A delivery failure part-way through
therefore left the remaining samples unqueued while the read offset had
already moved past the bytes that produced them, and a later record's own
advance carried the skipped range into the acknowledged one. The offset is
now moved after the last sample of the record reaches the sink, so a
failure re-reads the record whole — redelivering the samples that did get
through, which is the at-least-once contract and what content-addressed
row keys collapse back to one row.

### 6.30 Receiver state and hints

Two holes on the receive path, both of the kind §6.20 and §6.15 closed
elsewhere:

- `evictLocked` returns the peers it retires precisely so the caller can
  drain them. `stream()` discarded that return value, so a peer evicted on
  the arrival of a new sender lost its open multiline record and its
  half-filled aggregation window. It is now drained, outside `r.mu`,
  because delivering into the sink from under that lock would invert two
  locks.
- The `key_hint` of §6.15 was supplied only in `logs` mode. A set declared
  `key: offset` and fed by the line protocol, or by
  `POST /ingest/v1/samples`, had every sample refused by the store for
  carrying no hint — a listener that could never write to the sets the
  scheme exists for. Both paths now attach the listener's arrival
  sequence, and a caller-supplied hint still wins.

### 6.31 A client-input fault is a 4xx, and retry exhaustion is back-pressure

Three separate places on the write path turned a recoverable condition into
permanent loss, and each was survivable alone.

`Store.Write` returned `applySetMeta`/`applyFieldMeta` validation failures as
bare Go errors and `handleWrite` mapped every non-`Diag` error to `500`.
`wire.Client` classifies `>= 500` as retryable, so the sink retried six times
and then dropped the batch — and `requeueMeta` put the offending metadata
straight back on the queue for the next flush. One bad field in a spec was
therefore 100 % data loss, at full rate, for the life of the process, with
every checkpoint frozen alongside it. Those failures now carry
`store.ErrBadRequest` and come back as `400`, which the client treats as
fatal on the first attempt.

The same fault was reachable from an ordinary spec typo. `mql.ParseDuration`
accepts a leading sign so that `Print` → `Parse` round-trips a negative
duration out of an unvalidated AST, and `Spec.Compile` never range-checked
the result, so `sets: {app: {retention: "-5s"}}` compiled cleanly. `Compile`
now refuses a negative retention, a non-positive shard width and a
non-positive `max_interval`.

Retry exhaustion is no longer a drop. A store that sheds load answers `503`
with `Retry-After` — its own request to slow down — and the client's six
retries are a few seconds of that, so discarding at the end of them threw
good data away during exactly the condition the shedding exists to survive.
The batch goes back at the head of its buffer and delivery is held for the
interval the store asked for, which is what the `401` path already did. Only
a full buffer drops (see §7).

### 6.32 `max_interval` travels in milliseconds

The spec accepts any duration for a field's cadence, and the wire carried
`max_interval_s` as whole seconds. `500ms` truncated to `0`, which switched
gap detection off and then had the query emit `W103` saying the field has no
declared cadence — which was false; `1500ms` rounded to `1000ms`, a *tighter*
gap than declared, so a series arriving exactly on its declared cadence drew
a connect-break at every single point. `FieldMeta.MaxIntervalMs` carries it
now. `max_interval_s` is still read, so an older ingester's metadata still
lands, and is never written.

### 6.33 The query listener is a read surface

`listen.query` exists so an operator can hand a separate address to Grafana,
and it mounted the full mux: that address also accepted `/v1/write` and
`/v1/admin/*`, separated only by bearer scopes, and by nothing at all under
`auth.mode: none`. `API.QueryHandler` mounts the read endpoints and nothing
else. Relatedly, `startListeners` passed a zero `listenSpec` for the debug
and metrics listeners, so their `tls:` blocks were validated at startup and
then ignored — two of four listeners served plaintext whatever the config
said. Every listener now uses its own spec.

### 6.34 `LABELS <key> WHERE …` is gated like a graph

`Validate` returns early for `KindLabels`, so neither datasource ceiling was
computed, and the filter scan walked every set carrying the label across the
whole time range with no series or points bound, accumulating into an
unbounded map. That is the path a dashboard hits on every variable refresh.
It is now bounded by the same two ceilings the timeseries and heatmap paths
use, and reports a tripped gate the same way: partial results, `Error`, and
`W401`.

Table and logs truncation is reported the same way too. `runTabular` set
`Stats.Truncated` and nothing else, and the plugin renders `Warnings` and
`Error`, so a table silently showed the first 1 000 rows of a range.

### 6.35 A trailing gap is drawn

`render.Series` injected a null only when a *later* sample arrived, so a
series that stopped mid-range ended at its last point: a source that went
away drew as a line that simply stops, which is the false continuity the
whole walk exists to prevent. `Spec.EndMs` carries the end of the requested
range, and a null is emitted at `last + gapMs` when the range ends more than
`gapMs` after the last sample. It is left at zero for alert evaluation,
where no synthetic point may contribute.

### 6.36 A partially parsed histogram is not a row of zeros

`expand` refuses to invent a row when the whole `buckets` group is missing,
but wrote `Int(0)` for every declared bucket the payload did not contain —
which draws on a heatmap as a measured zero, the same invention one bucket
at a time. Absent buckets are now absent. The derived `tail` and
`<bucket>plus` columns read every bucket, so they are skipped rather than
computed wrong when the payload carried only some.

### 6.37 Interning is linear

`intern` scanned the whole entry slice for a reusable hole on every new
value, under the exclusive dictionary lock and on the write path: 10 k
values in 25 ms, 20 k in 92 ms, 40 k in 362 ms — a clean 4× per doubling,
which at the default 100 k cardinality limit is seconds of lock-held CPU per
label key with every write serialised behind it. The free positions are held
in a list built once at load, so the cost no longer depends on cardinality.

### 6.38 Smaller corrections

- The remote follower shared one `Checkpoint` between its tail loop, which
  rewinds it on a rotation, and the sink's flush goroutine, which advances
  it — and `CheckpointStore`'s lock protects the file, not the struct. It
  now goes through a `checkpointBox` that hands `Save` a copy, which is
  what the local follower already did and for the same reason.
- `lazyRow.get` recorded a structural payload error but swallowed a
  per-value decode error, so a corrupt column read as absent and a
  pushdown filter quietly excluded the row — the asymmetry `decodeRow` was
  changed to remove. Both are reported now.
- The forward pointer stored under a `D/` key carries a tag byte. It was
  recognised by its length alone, and a small row legitimately stored
  there — one column, a three-character name, a one-byte value — encodes
  to exactly the same eight bytes. The untagged form written by an earlier
  build is still read.
- `Validate` normalises an absent `FORMAT` to `timeseries` before its
  format-dependent checks. An AST with no `format` key executes as a
  timeseries but skipped `W103`, the warning that says outages will be
  drawn as continuous lines — and a hand-authored panel model, which §7
  says is the only kind there is until the frontend exists, is exactly
  that shape.
- `extract.Stream.process` returns the aggregation windows it closed
  *alongside* an error rather than instead of them. They have already been
  removed from the stream, so a caller that dropped them on the error lost
  every window that happened to expire on the same record as a spec fault.
  `Flush` and `FlushIdle` no longer double-count those results in
  `Stats.Samples`.
- A continuation line that matched `continue_regex` but no `join` rule
  used to vanish from both the samples and the unmatched tally. It is
  counted, reported as `ErrNoJoin`, and surfaced by `check` and by the
  progress document.
- An over-long record is truncated onto a rune boundary. Cutting mid-rune
  produced an invalid-UTF-8 label value, which the store rejects by name —
  so the record lost its whole sample rather than its tail.
- `mensura-store query --explain` prints the plan from `/v1/debug/plan`
  rather than the request body. It needs `--debug-store`, because the plan
  is served by the loopback debug listener and not by the API.
- `mensura-ingest follow --poll-interval` is local-only and
  `--ssh-probe-interval` is its remote counterpart; supplying one to the
  other path now says so instead of being ignored. A local poll is a
  `stat`, a remote probe is an SSH round trip, so they cannot share a
  default.
- `startReporting`'s stop function waits for its goroutine, so a progress
  sample can no longer be queued into a sink whose final flush has already
  happened.
- A variable inside a regex is diagnosed. `host = "$env"` was a clear
  `E010` while `host =~ /$env/` compiled as a literal regex, matched
  nothing, and produced only a `W201` — the same mistake with two very
  different answers. `Variables()` walks `match`/`noMatch` too, so the
  plugin reports the dependency. `$` is the end-of-line anchor far more
  often than it is a variable, so only a `$` immediately followed by an
  identifier counts, and an escaped `\$` is left alone.
- `validateExpr` applies the unknown-label check (`E004`) to `has` and
  `missing`, not only to the comparisons. `MISSING nosuchlabel` used to
  validate and match every row.
- `Checkpoint.SpecHash` and `LastTSMs` were serialised into every
  checkpoint file and read by nothing. The feature they were for —
  spec-change invalidation — is not implemented, so the fields are gone
  rather than left looking like one that works.
- Batch import keys its hint on `StreamID(path)`, as follow does. Keying
  on the raw path gave one record two different keys depending on how the
  file was read, so under `key: offset` a file that was imported and later
  followed produced two rows per record.
- `Ingest.resolve` de-duplicates. Overlapping sources are ordinary —
  `/logs` and `/logs/*.log` name the same files — and every record in the
  overlap was extracted, delivered and counted twice.
- `lexNumber` decodes the unit rather than byte-casting it, which is the
  conversion the identifier scanner was explicitly moved away from; only
  the rewind kept it harmless.
- `p.integer()` reports a fractional token as a positioned `ParseError`.
  The lexer emits `1.5` as a number, so `ParseInt` surfaced a bare
  `strconv` error with no position in it.
- `remoteService.Parse` returns an error when the catalogue fetch fails.
  Returning `q, nil, nil` reported a query as valid when the
  catalogue-dependent half of validation never ran.
- `CheckHealth` no longer prints `data from 1970-01-01` for sets that carry
  a last timestamp but no first one.
- `receiver` collapses a repeated per-record warning onto the powers of
  ten. A listener that matches no profile fails every record, and a line
  each was a log flood at line rate.

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
- **A label value that is literally `$name` cannot be queried.** The AST
  stores a variable reference as the plain string `"$name"`, so it is
  indistinguishable from a literal of the same text, and §6.14's `E010`
  refuses both. Separating them needs a tagged value on `Compare` and
  `InList`, which changes the JSON shape of every stored panel; the
  round-trip is otherwise lossless.
- **The sink's buffer is bounded by a count, not by a spill.** A cancelled
  context (§ shutdown), a rejected credential (§6.26) and now retry
  exhaustion (§6.31) all requeue rather than shed, so a long outage grows
  memory until the store comes back. `SinkConfig.MaxBufferedSamples`
  (100 000) is where that ends: past it the oldest batch is dropped,
  counted and logged with the reason, because an unbounded buffer turns a
  store outage into an out-of-memory kill that loses everything rather
  than the tail. Losing nothing at all still needs a spill-to-disk queue.
