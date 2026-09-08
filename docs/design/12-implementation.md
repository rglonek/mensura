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
| A receive listener that cannot bind fails the command | `internal/ingest/fixes_test.go:TestReceiveRefusesToStartWhenTheFirstListenerCannotBind`, `…:TestReceiveReleasesBoundListenersWhenALaterOneFails` |
| A record the extractor is still holding holds the checkpoint | `…:TestFollowHoldsTheCheckpointBehindABufferedRecord`, `pkg/extract/fixes_test.go:TestHeldFromNamesTheOldestBufferedRecord`, `…:TestHeldFromCoversAnOpenAggregationWindow` |
| An empty `IN` list is an error, not an empty panel | `pkg/mql/empty_in_test.go:TestEmptyInListIsRefused`, `…:TestEmptyInListIsRefusedInsideAnAndArm` |
| `check --sample` reads compressed and skipped sources as the import does | `cmd/mensura-ingest/fixes_test.go:TestCheckSampleReadsACompressedFile`, `…:TestCheckSampleDeclinesWhatTheImportSkips` |
| An epoch unit that cannot describe the value is a timestamp failure | `pkg/extract/fixes_test.go:TestEpochSecondsRefusesAValueThatCannotBeSeconds` |
| An excluded sender is refused before its body is read | `internal/ingest/fixes_test.go:TestReceiveHTTPSamplesRefusesTheSenderBeforeReadingTheBody` |
| An out-of-range `bucket_index` is named, and the version still moves | `internal/store/fixes_test.go:TestFieldMetaRefusesAnOutOfRangeBucketIndex`, `…:TestFieldMetaVersionTracksRealChanges` |

## 5. Implementation status

| Area | Status |
| --- | --- |
| Engine: keyspace, TLV codec, covering index, pushdown, projection, snapshot iterators, shard drop, compaction, stats, tunings, storage version | implemented |
| Store: catalogue, label dictionary, shard routing, retention sweep, write path, idempotency, cardinality guard | implemented |
| Query: planning, pushdown, grouping, render, safety gates, `Explain`, `timeseries`/`table`/`logs`/`heatmap`, `SETS`/`FIELDS`/`LABELS`/`LABEL KEYS` | implemented |
| Render pipeline: all ten stages, null slots, SSE, window formula, `EVERY` | implemented |
| MQL: lexer, parser, printer, validator, diagnostics, JSON AST | implemented |
| Extraction: profiles, timestamp formats and caching, multiline, replace, routes, default values, bucket sets with cumulative/tail, aggregation, identity discovery, `check` including the unreachable-pattern and capture-resolution analyses (§6.39) | implemented |
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

It also needs an address of its own. `startListeners` can only mount one
handler per address, so it skipped the query listener when its address
equalled the write listener's and kept the full mux there — which inverts
exactly what the operator asked for. `checkAuthPosture` refuses the pair
at startup, before the data directory is opened.

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
  build is still read, and `Get` falls back to decoding the payload as a
  row when the index key it names does not exist, which is what keeps the
  legacy ambiguity harmless. A regression test pins that fallback.
- The engine's set-id high-water mark is persisted rather than re-derived
  from the sets that survive. `DropSet` removes the meta record, so
  deriving it from `max(ID)+1` made it *regress* across a restart:
  dropping the highest-numbered shard — which retention does on every
  sweep — let the next set created afterwards take that id back, and set
  ids are what the `D/` and `I/` key prefixes are built from.
- The cardinality budget counts live dictionary values, not positions. A
  lost record leaves a hole that `takeHole` exists to reuse, and counting
  it against `MaxLabelCardinality` refused writes on a key that was under
  its budget while charging for the same position twice.
- `Sink.enforceBufferCap` takes its cut from the oldest end of every
  buffered set in proportion to its size. Draining the sorted set list in
  order meant that with sets `access` and `zzz` over the cap, the whole of
  `access` was discarded before `zzz` lost one sample: one stream went
  dark on the dashboard while another was untouched.
- A flush that could only take part of the buffer now reports neither half
  to the delivery observers. It announced no `BeginFlush` — correctly,
  since the mark would cover samples the batch does not carry — but still
  called `EndFlush`, which was safe only because `commitInflight` happens
  to no-op when `inflight` has not moved. No observer contract stated
  that, so the next implementation of one would have advanced its
  checkpoint over undelivered data.
- `follower.retire` clears the checkpoint fingerprint along with the
  offsets. It described the file that had just been rotated away, so the
  record on disk named offset zero in the *new* file beside a content hash
  of the old one.
- The remote follower honours `--start-at` where the flag is applied, not
  where the loop reaches the bottom. Both cases lived inside the `else`
  arm of the size probe, so one transient SSH failure on the first pass —
  which the loop logs and carries on from — silently dropped the flag for
  the life of the process.
- A joined multiline record is bounded by `max_record_bytes` like a single
  one, and the truncation is counted. The cap bounded each input line and
  the buffer a join appends into was bounded by nothing, so a stream of
  continuation lines grew one string for as long as the idle timeout
  allowed — per rule, and on the receive path per peer.
- `Store.Close` is idempotent. `runWithEngine` defers it while an error
  path may already have taken it, and the second call re-ran
  `saveCatalogue` against a closed engine and logged an error about a
  shutdown that had already succeeded.
- `Explain` clamps a negative `EVERY` the way `runTimeseries` does, so the
  plan it reports is the plan that would run.
- An empty quoted name is a parse error. `WHERE HAS ""` produced
  `Expr{Has: ""}`, which `Expr.Empty` reports as carrying no predicate at
  all: `Print` dropped the whole clause and the store's lowering produced
  a nil expression, so a query that asked for one thing silently widened
  to every row in the set.
- A number that will not parse comes back as a positioned `ParseError`
  like every other syntax fault, instead of a bare `strconv` error with
  the cursor already moved past the token.
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

### 6.39 `check` performs the analyses it advertises

[03](03-extraction.md) §1 says `mensura-ingest check` "checks that captures
resolve, detects unreachable patterns (a `search` literal shadowed by an
earlier pattern)". It did neither: it compiled the spec and, given
`--sample`, reported match rates. Everything else in this codebase refuses
an unimplemented documented feature by name — `on_parse_error:
drop-stream`, `framing record: json`, `store_stream_label`,
`limits.write_rate_per_client` — and this was the one that silently did
nothing, on the tool whose entire job is to predict what an import will do.

`acMatcher.FirstIndex` returns the *lowest* pattern index whose literal
occurs in the record, and `process()` uses that one pattern and no other.
So a pattern whose `search` contains an earlier pattern's `search` is
dead: its destination set is never written, and nothing at run time says
so, because the record did match something. An empty `search` is the
extreme case and shadows every pattern after it.

`Spec.Lint` reports four codes, split by `Lint.Fatal()` into the one that
describes a broken spec and the three that describe a working one that
declares more than it uses. `check` prints both, under separate headings,
and exits non-zero only on the fatal one — so a spec with a dead pattern
fails the pipeline that runs it instead of shipping, while a spec that
merely names its operator labels does not:

| Code | Fatal | Meaning |
| --- | --- | --- |
| `L001` | yes | a pattern that can never be reached, naming the one that shadows it |
| `L002` | no | a capture that is neither a declared label nor a `fields:` entry, so it lands as an untyped gauge |
| `L003` | no | a `fields:` declaration no pattern captures, so its metadata reaches no column |
| `L004` | no | a declared label no pattern captures and that is not a stream label |

Failing on all four alike was wrong in a way the L004 text said out loud:
its own message ends "if it comes from `identity:` or --label it is
attached by the stream and needs no declaration here", and declaring such
a label in `defaults.labels` — which is where the documentation puts it —
then failed the build. The stream labels L004 exempts are also derived
from the `identity:` rules' own capture names rather than being the two
hard-coded `host` and `source`.

Three related wirings are decidable from the spec alone and are now
compile errors rather than per-record failures: an `aggregate.on` key that
no regex captures or that is not classified as a label (the accumulator
reports "aggregation key is not a declared label" for every record and the
pattern produces nothing); a `bucket_set` pattern with no `buckets` or
`histogram` capture group (`process()` refuses each record); and
`parse: json_object`, which `expand()` has no case for and which is listed
in [03](03-extraction.md) §8 as if it worked.

### 6.40 `HISTOGRAM()` is refused outside `FORMAT heatmap`

Only one direction used to be checked. `FORMAT heatmap` without a
`HISTOGRAM()` was `E008`; the mirror image validated with no diagnostic at
all and then drew nothing, because the planner resolves no field for a
bucket set, so `runTimeseries` iterates an empty list. The panel came back
empty with no error and no warning — the failure the validator exists to
prevent — and an AST with no `format` key at all took the same path.

### 6.41 The retention sweep forgets the whole set

A set whose every shard has aged out has its catalogue entry removed. The
sweep removed only that, leaving the spec-supplied retention and shard
width behind in `setRetention`/`setShard` — which is exactly what
`ForgetSet` exists to prevent, and what its comment describes. The
consequences were both halves of the same bug: until a restart, a set
later created with the same name silently inherited the dead set's policy;
and after a restart the persisted `retention_ms`/`shard_ms` were gone with
the entry, while an ingester that is already running never repeats the
declaration. Under the documented "`--retention 0` globally, per-set
retention from the spec" deployment the set then came back with no
retention, was routed to the unsharded shard the sweep skips, and was kept
forever with nothing saying so. The sweep calls `ForgetSet`.

### 6.42 An identity rule's `match_path` scopes its `regex`

An `identity:` entry that declares both keys is one rule: `match_path`
selects which files it applies to and `regex` says what to pull out of
their heads. The two halves ran independently, so a path-scoped rule
scanned the head of every file in the sweep and attached its labels to
files its own `match_path` had just declined. Rules that want the two
independent write them as two list entries, which is how
[03](03-extraction.md) §2 shows them.

### 6.43 A regex prints as it was written

The printer escaped a regex by doubling every backslash, because
`lexQuoted` collapses `\\` to `\`. That round-tripped, and it printed
`/\\d+/` for the pattern `\d+` — so the canonical text of a query, which
is what the builder shows and what Grafana reports as the executed query,
read as a different regex from the one that was written. Only the
sequences the lexer actually decodes are escaped now, and a backslash only
where leaving it bare would be ambiguous: before the delimiter, before
another backslash, or at the end of the pattern.

Relatedly, `printFloat` emitted `'f'` unconditionally, so a `CLAMP` bound
near `MaxFloat64` printed as a 310-digit literal. Past 24 characters it
uses the exponent form, and the number lexer reads an exponent back.

### 6.44 A drop and a write to the same set cannot interleave

`DropSet` took the set out of the maps, released the lock and only then
applied the batch that ends with `Delete(metaKey("set", name))`. A
`PutBatch` for the same name landing in that window re-created the set
under a fresh id — so its rows went outside the range deletes and
survived — persisted its meta record, and then had that record deleted by
name. After the next restart the shard was absent from `Sets()`: invisible
to every query, to the catalogue, to `shardsFor` and to the retention
sweep, while its rows still occupied disk with no way to reclaim them,
because dropping needs the name to be in `d.sets`. Reachable from the
hourly sweep and from `DELETE /v1/admin/sets/`, either of which can
coincide with a backfilled write.

The mirror case is a write that resolved its set id before the drop and
applied its rows after the range deletes went in. Neither half of a write
happens under `mu` end to end, so the exclusion is a second lock:
`dropMu`, held shared by `PutBatch` and `RegisterSet` for their whole
duration and exclusively by `DropSet`.

### 6.45 Set metadata moves the catalogue version

`SetRetentionFor` calls `entryLocked`, which *creates* the catalogue
entry, and nothing bumped `catVer` — so a write carrying only `SetMeta`
added a set to `/v1/catalogue` while the version stood still, and
`handleCatalogue` answered `304` to every client still holding the old
ETag. A datasource that caches it never saw the set. Only a real change
moves the version, which is the rule `applyFieldMeta` already follows: an
ingester that re-declares the same retention on every start must not wake
every client watching `catalogue_version`.

### 6.46 Every acquisition path counts its samples

`Progress.Samples` was written by `MergeStream` alone, and `MergeStream`
is called from one place: `processFile`. Follow, SSH follow and receive
never called it, so they reported `0 samples` forever — on the console,
in the progress document, and in the `samples` field the `_mensura_ingest`
set publishes for dashboards ([02](02-ingest.md) §5). Results are counted
where they are handed to the sink now, on every path; `MergeStream` keeps
only the sample of unmatched lines, and `processFile` defers it so a read
error part-way through a file no longer discards that file's
`FirstUnmatched` — the one output that explains why the import failed.

### 6.47 The UDP consumer is joined before the final flush

`serveUDP` closed its queue and returned, and nothing waited for the
goroutine draining it. `Receive` went on to `flushAll` and `Sink.Flush`,
and `runReceive`'s deferred `sink.Close` ran, while up to `UDPQueue` (4096)
datagrams were still becoming samples behind them: reported as "still
buffered at shutdown" and lost, with no checkpoint to re-read them from.
The consumer is a `WaitGroup` of one, joined before the listener returns.

### 6.48 A listener that cannot bind fails startup

`start()` ran `ListenAndServe` inside the serving goroutine and logged
whatever came back. An address already in use — the exact two-owners
mistake this binary's package doc is about — still printed `api listener
on …` and `mensura-store … listening on …`, and the process then sat on
`<-ctx.Done()` serving nothing. Every address is bound before any of them
is served, whatever did bind is closed again, and the error names the
listener that failed.

### 6.49 Request bodies are bounded in aggregate

A body is read before a write slot is taken, deliberately: holding a slot
across the read would let `MaxConcurrentWrites` slow clients occupy the
pool without ever presenting a batch. The consequence was that peak
write-path memory was concurrent connections × `max_request_bytes`
(32 MiB), with nothing capping the connections. `max_buffered_request_bytes`
(default 4× `max_request_bytes`) is the ceiling on what every handler
holds at once; past it a body is shed with `503` and `Retry-After`, which
is the same honest signal the write pool gives.

### 6.50 The wire catalogue carries the stale flag

`Schema.Field` computed `Stale` from `LastSeenMs`; `Store.Catalogue()`,
which is the form that travels over the wire, did not. So the same query
against the same data answered `W203 field … has not been seen recently`
under `mode: plugin` and nothing at all under `mode: proxy`, because a
proxy validates against the catalogue it fetched.

### 6.51 `check` selects on the operator labels

`checkSample` passed `nil` labels to `SelectProfile` while
`processFile` passes `i.cfg.Labels`, and `check` registered no `--label`
flag — so a profile chosen by `select.label_equals` could never match and
the tool whose job is to predict the import answered `no profile matched`.
Same flag, same validation as the acquisition modes.

### 6.52 Aggregation windows expire from a heap

`process()` calls `closeExpiredAggregators` on every matching record, and
that walked the whole `aggs` map. With a high-cardinality `aggregate.on`
key the map holds an entry per key per window, so the per-record cost grew
with cardinality, on the hot path, per stream. The windows sit in a
min-heap ordered by end time (then by key, so windows ending together
close in a stable order), and expiry touches only what has expired.

### 6.53 Remote follow sees a rotation it grew past

The size probe detects a file *shorter* than the bytes already read. That
is a truncate; a rename-and-create rotation is not. `tail -F` follows the
replacement while `consumed` keeps climbing from the file it left, and by
the next probe — 15 s later by default — the new file has usually grown
past the acknowledged offset, so the length test finds nothing wrong and
the reconnect resumes at a byte position that means nothing in it. That is
a silent skip, not a replay. The probe reads `ls -Li` alongside `wc -c`
(both POSIX, still no remote install) and the identity is kept in the
checkpoint's `fingerprint`, so a rotation is seen both mid-connection and
across a restart. A far end whose `ls` cannot answer falls back to the
length test alone.

### 6.54 A backlog is drained within one flush

A take bounded by `BatchBytes` reports `partial`, and a partial take
deliberately tells the observers nothing: their high-water mark covers
samples this batch does not carry. That rule is right per request, but it
meant no checkpoint advanced for the *whole duration* of a backlog, so a
crash while recovering from a store outage replayed all of it. `Flush`
sends rounds until the buffer is empty and announces only the take that
empties it — the acknowledgement still covers exactly what the store
holds, but it happens when the backlog drains rather than when a later
flush happens to find the buffer small. A cap of 64 rounds keeps one call
from running forever against a source that outruns the store, and says so
when it stops.

### 6.55 Smaller corrections

- A series whose only sample is consumed by `DELTA` or `RATE` renders as
  nothing. `lastPointTime` is set before those stages drop the sample, so
  the trailing-gap rule fired on a series that had emitted no value: a
  connect-break drawn just after a moment when data did arrive, whose real
  cause is that a rate needs two samples.
- A row with no indexed column is refused rather than stored under `D/`.
  Every scan of an indexed set is bounded to the index prefix, so such a
  row was write-only. `Get` still reads the ones an earlier build wrote.
- An empty node inside `and`/`or`/`not` is `E001`. It executed fine — the
  store lowers it to a constant true — but printed as `( AND host = "x")`,
  which does not parse, and the AST and its canonical text round-trip
  losslessly by contract. An absent predicate still means "no predicate".
- A hole in a legacy packed label dictionary is not indexed as the empty
  string, so `label = ""` lowers to the constant-false plus `W201` an
  unknown value gets rather than to an equality against a free position.
- `pebbleLogger.Fatalf` flushes before it exits. It forwarded to
  `log.Fatalf`, so `os.Exit` skipped every deferred `Close` — and `Close`
  is what performs the explicit `Flush` that makes "a graceful stop is
  durable in every profile" true. Under `durability: batch` there is no
  WAL behind it. The flush is time-bounded, because pebble may be holding
  its own locks on the way out.
- `POST /ingest/v1/lines` answers `403` to a sender outside
  `allowed_sources` and reports `refused` alongside `accepted`. It used to
  answer `200 {"accepted": 0}`, which is indistinguishable from an empty
  body.
- `FollowOptions.IdleFlush`/`MaxRecordBytes` and
  `ReceiveOptions.MaxConnections`/`PeerIdle` are reachable:
  `--idle-flush`, `--max-record-bytes` (shared by every path),
  `--max-connections`, `--peer-idle`. A real knob an operator cannot reach
  is the mirror image of the config keys this codebase refuses by name.
- `SinkConfig` is reachable for the same reason: `--batch-size`,
  `--batch-bytes`, `--flush-interval`, `--max-buffered-samples` and
  `--max-fatal-drops`. These decide how large a body the ingester
  presents and how much of a store outage it survives before it starts
  dropping, and `setup()` used to pass `DefaultSinkConfig()` verbatim, so
  every documented trade-off was the library's to make and not the
  operator's. Zero leaves the built-in, as it does for
  `--max-record-bytes`; a negative `--max-fatal-drops` never gives up.
- `Value.UnmarshalJSON` decodes strictly. A custom unmarshaller replaces
  the outer decoder's settings, so `handleWrite`'s `DisallowUnknownFields`
  stopped at the edge of a field value and `{"i":1,"flaot":2}` was
  accepted with the typo dropped.

### 6.56 The ingest listeners bind before they serve

`Receive` started `serveTCP`, `serveUDP` and `serveHTTP` on goroutines and
each of them called `net.Listen` itself. The error went into a channel
nothing read until every *other* listener had exited, and they only exit on
cancellation — so a port already in use printed nothing at all, the
surviving listeners logged `receiving on …`, and every record sent to the
dead address was refused by the kernel with nothing on this side recording
it. It is the same shape `startListeners` was moved away from in §6.48.

Every address is now bound in `Receive` before any of them serves,
whatever did bind is closed again on the way out, and the error names the
listener that failed. One listener failing mid-flight also cancels the
others, the way `FollowRemote` cancels its siblings on the first error: a
receiver serving two of its three addresses looks healthy and is not.

### 6.57 A record the extractor is still holding holds the checkpoint

02-ingest.md section 5 says `acked_offset` advances "to the highest byte
offset fully covered by acked samples". The read loop advanced it per
*record consumed* instead, and those are not the same thing. A line that
matched a `start_contains` opened a multiline buffer and returned no
samples; a line folded into an `aggregate:` window returned none until the
window closed. Both advanced the offset, so bytes whose sample existed only
inside `extract.Stream` were recorded as delivered. A crash inside the
idle-flush window lost them silently, and `applyRewind` — which discards
the extractor and re-reads from the frozen offset — threw away whatever an
open window had already absorbed.

`extract.Stream` now carries a caller-supplied `Mark` per record and
answers `HeldFrom`: the mark of the oldest record it is still holding. The
follow and SSH-follow loops pull `pending` back to it, and release it when
the buffer flushes — on the next block marker, on the idle flush, on
rotation, on shutdown. Checkpoints therefore lag an open aggregation window
by at most its own width, which is the honest answer; they do not freeze,
because every path that empties the extractor republishes the offset.

### 6.58 An empty `IN` list is an error, not an empty panel

`{"in": {"label": "host", "values": []}}` validated clean. The lowering
turns it into a constant false — correctly, nothing can match — but with no
diagnostic, so the panel came back empty with nothing saying why: the one
outcome the validator exists to prevent. `Print` also emitted
`host IN ()`, which does not parse, so an AST and its canonical text
stopped round-tripping losslessly. The parser cannot produce such a node;
only a hand-authored or builder-generated AST can. It is now `E007`.

### 6.59 `check` reads a sample the way the import does

§6.28 gave `check` the acquisition paths' framing. It still opened the file
with a bare `os.Open`, while `processFile` goes through `peek` and
`openRecords`, which decompress single-file gzip and bzip2 and decline
archives and binary content by name. So `check --sample app.log.gz` handed
the extractor deflate bytes and reported "no profile matched" — or a 100%
unmatched rate — for a file `batch` imports without trouble, from the one
tool whose job is to predict what the import will do. It now uses the
importer's own `OpenSource`/`SniffSource`, and takes `--max-record-bytes`
so a record is truncated at the same point on both paths.

### 6.60 The hot paths are off the shared locks

Three places did work under a lock, or repeated work, that the rest of the
codebase had already been moved away from:

- `engine.PutBatch` derived its column set — every column of every record,
  ten thousand map operations for a default batch — while holding `DB.mu`
  exclusively. It touches no shared state, so it serialised every writer in
  the process behind it and contended with every query's `setRef`. It now
  runs before the lock is taken.
- `queryLabelValues` called `shardsFor` once per set carrying the label,
  and `shardsFor` walks and sorts every set name in the store. With hourly
  shards that is quadratic in the shard count, on the path a dashboard hits
  on every variable refresh. `shardsFor` is now a thin wrapper over
  `shardsInRange`, which takes a list the caller already has, and the scan
  builds one with `shardsByLogical` — the same fix §6 applied to the
  catalogue.
- A regex label filter copied and sorted the whole value list and then took
  the dictionary lock again for each match: at the default 100k-value
  budget, a hundred thousand lock acquisitions to lower one clause. It is
  one pass under one lock (`labelIndicesMatching`), which also matches a
  value that appears at two dictionary positions at both of them.

### 6.61 Smaller corrections

- `/ingest/v1/samples` decoded up to 32 MiB of JSON and validated the set
  name before checking `allowed_sources`. Its sibling `/ingest/v1/lines`
  was changed to answer first (§6 above); an excluded sender got a `400`
  about a set name rather than the `403` that was true.
- A TCP connection cut mid-record fed the fragment to the extractor. Half a
  line does not fail cleanly — a prefix-anchored pattern matches it and
  invents a sample from a truncated number — and there is no re-read on a
  socket. A record with no terminator is now extracted only when it is
  complete as far as the sender is concerned: it hit the size cap, or the
  sender closed cleanly after it. A cut leaves a counted warning instead.
- `intern` popped a hole from the free list and, if the dictionary write
  then failed, dropped it: the position stayed empty and was never listed
  again, leaking the cardinality budget this function defends.
- An out-of-range `bucket_index` was silently skipped with a `continue`
  that also jumped over the change detection at the end of
  `applyFieldMeta`'s loop — so a kind or unit change carried in the same
  declaration was written with `CatalogueVersion` standing still, and every
  ETag-caching client was answered `304` for a catalogue that had moved. It
  is now refused by name in the validating pass, like every other field of
  a declaration.
- `applySetMeta` refused every reserved set while `applyFieldMeta` exempts
  `_mensura_ingest`, so the one set every ingester writes was also the one
  set no spec could give a retention.
- `Validate` returned a literal `nil` warning slice on the `LABELS` path
  while `validateExpr` was accumulating into a pointer. Nothing can produce
  a warning there today, only because the schema is passed as `nil`.
- `--start-at` accepted any string. Both followers take "from the
  beginning" as their default arm, so `--start-at END` silently re-read
  every source in full on every restart.
- `epoch_s` multiplied by 1000 on trust. A nanosecond value declared as
  seconds overflows `int64` and wraps to a timestamp that is not the one in
  the record — sometimes back inside `model.MaxTSMs`, the bound that exists
  to catch exactly that unit mismatch. Out-of-range values are now a
  timestamp failure, which is counted and dropped. `epoch_us` and
  `epoch_ns` floor rather than truncate towards zero, so a pre-epoch
  timestamp lands in the millisecond it belongs to.
- `runProxy` ignored the signal context `runServer` had built for it, so
  `SIGTERM` reached nothing in proxy mode.
- `Progress.AddBytes` was the only thing that incremented the record
  counter, which its name did not say. It is `AddRecord`.

### 6.62 A deleted followed file is retired

`poll` builds its work list from `filepath.Glob` and visits only the paths
that match now, and `retire` — which closes the handle and forgets the
tailer — was reachable only from `checkRotation`, which only runs for a
path in that list. A deleted file is not in it, because `Glob` lists a
directory. So `checkRotation`'s `os.IsNotExist` branch, and the "drain to
EOF, close, wait for the path to reappear" row in [02](02-ingest.md) §9,
only ever fired on the race between the glob and the stat.

An ordinary deletion instead left the tailer in the map for the life of
the process: its descriptor kept the unlinked inode's blocks allocated —
the `df` full / `du` empty incident — and its extractor and buffers leaked
with it. On a glob whose members come and go (a file per day, a file per
instance) that accumulates. `poll` now retires every tailer whose path the
sweep no longer matched, draining the open handle first, because an
unlinked file still holds whatever was written before it went away.

### 6.63 A rewind freezes the offset the checkpoint may reach

`clearHole` runs on whichever goroutine flushed — the sink's own flush
loop as well as the poll goroutine — and pulls `pending`/`inflight` back
to `acked`, but the seek itself waits for the poll goroutine's next
`applyRewind`. `read` tests `rewindOwed` only at the top of an iteration,
so a hole that cleared mid-record left it free to finish that record and
publish a position past the frozen offset; a flush landing in that window
snapshotted the advanced value and committed it, writing a checkpoint past
the very bytes the freeze exists to re-read. It self-heals in memory —
`applyRewind` resets the offsets and the re-read re-advances them — so the
exposure was a crash in the window, which is the likeliest moment for one,
since the hole was caused by the store being unavailable in the first
place. `setPending` and `markInflight` now do nothing while a rewind is
owed.

### 6.64 The store stamps a client name only when it verified one

`Write` overwrites the ingester's own `client` label on `_mensura_ingest`
with the HTTP layer's client name. With `auth.mode: none` — the documented
loopback posture, and what the quick start runs — `authorise` answers
`"anonymous"` for every caller, so every ingester reported under one name,
`--client-name` did nothing, and two ingesters against one store collapsed
into a single series whose counters interleave and read as a counter reset
on every scrape. The name is passed through only under `auth.mode: bearer`,
where something actually checked it; otherwise the sample's own label
stands, which is what [05](05-storage.md) §7 describes.

### 6.65 The HTTP `lines` listener frames like every other path

`POST /ingest/v1/lines` split its body on `"\n"` instead of going through
`readRecord`, so it was the one acquisition path that ignored the record
cap: `--max-record-bytes` did not reach it, no counter moved for a
truncation, and a newline-free body became a single record of up to 32 MiB
handed whole to the profile's regexes. It reads through `readRecord` now,
against the same cap, and counts oversize records like the rest.

### 6.66 Smaller corrections

- A set that already holds rows cannot gain its first indexed column.
  Every scan of an indexed set is bounded to the `I/` prefix, so promoting
  one made the rows already under `D/` unreachable by any query and
  invisible to retention — the same silent orphaning `PutBatch` refuses a
  timestamp-less row to avoid. A set with nothing in it may still be
  promoted, so "declare, then write" is untouched.
- `PutBatch` derives a column's type from the widest value in the batch,
  not from whichever row the map walk reached first. A column arriving as
  an int in one row and a float in the next was registered as `int64`, so
  the schema disagreed with the payloads stored under it and `setLocked`'s
  own int-to-float widening never fired.
- An empty `OR` evaluates to false. False is the identity of the operator;
  returning true *widened* the query rather than narrowing it, which is
  the one failure mode a predicate must never have.
- `RunRetention` persists the catalogue as soon as it forgets a set, as
  the admin drop already did. Waiting for the 30-second tick meant a crash
  in that window brought the entry back, advertising fields and a time
  range whose shards had just been range-deleted.
- `seriesName` renders every `BY` slot, including one whose label the row
  did not carry. Dropping those collapsed the legend onto fewer slots than
  the grouping key has, so `{host: "a"}` and `{pool: "a"}` under
  `BY host, pool` were two series by `seriesKey` and drew as two lines
  both labelled `a`. An absent slot renders as `<label>=`.
- `peek` reports a `.gz` it cannot open instead of answering "no head, no
  error", which sent the caller on to select a profile and discover
  identity against an empty head before failing on the same file a moment
  later for the same reason.
- `--ssh-strict-host-key=false` sets `StrictHostKeyChecking=no` rather
  than merely omitting the strict setting, which fell back to a client
  default that the `BatchMode=yes` alongside it still refuses — so the
  flag named for turning verification off turned nothing off.
- `ingest.Config.Log` is defaulted like every other optional field. It was
  the one that was not, and the acquisition paths call it unconditionally,
  so a caller that filled in everything else got a nil-interface panic on
  the first skipped file.

### 6.67 A value that cannot be plotted reads as an absent one

`Value.AsFloat` coerces a numeric string, because extraction may
legitimately leave a value as a string. `strconv.ParseFloat` accepts the
literal text `NaN`, `Inf` and `+Infinity`, so a *string*-typed field
carrying one of those tokens coerced to a non-finite float, landed in
`wire.Series.Values` — a `[]float64` — and made `encoding/json` fail on
the response *after* `writeJSON` had already sent the `200`. The panel
received a truncated body with no status and no diagnostic to explain it.
`ValidateFieldValue` refuses such a value on the write path; nothing
refused it on the read path.

`AsFloat` now reports `false` for a non-finite result whichever type it
came from, so the query paths treat it exactly as they treat a column the
row does not carry. `AsInt` does the same, because `int64(NaN)` is
implementation-defined and that function is how the engine reads a row's
indexed timestamp. The tabular path honours the verdict rather than
appending whatever `AsFloat` returned, which is where a non-finite float
already on disk — written before the validation existed — used to poison
a table response.

### 6.68 A remote rewind freezes the offset the checkpoint may reach

6.63 gave the local follower a rewind guard: between the moment a hole
clears and the moment the reader actually seeks back, the read head is
stale, so publishing it let a flush landing in that window commit a
checkpoint past the very records the freeze exists to re-read. The remote
follower had the same hole and none of the guard. `clearHole` only signals
a reconnect, and the reader is very likely mid-record when it is asked, so
`advance` published a position the tail was about to abandon, `BeginFlush`
snapshotted it and the commit acknowledged it — and the reconnect, which
starts at the acknowledged offset, then skipped those records for good.

`remoteProgress` now carries `rewindOwed`. `advance` and `markInflight`
are no-ops while it is set, and `startFrom` — which is what a fresh tail
connection asks for its starting offset — discharges it. It also drains
the buffered rewind signal, which otherwise survived a hole that cleared
while the loop was between reconnects and killed the *next* connection
immediately, one that was already starting from the right place.

### 6.69 `--start-at end` is answered once

`end` means "only what arrives from now on". The local follower applied it
whenever the stored fingerprint failed to match, and a rotation is exactly
that: `retire` rewinds the checkpoint to zero and clears the fingerprint,
so every rename-and-create started the replacement at its *current size*
and silently skipped whatever it had accumulated between the rotation and
the next poll. 02-ingest.md section 6.1 says the new file starts at offset
0, and the remote follower already gated the same flag on "there is no
checkpoint". The local one now does too.

### 6.70 A remote tail has a clock of its own

A local follow flushes idle multiline records and half-filled aggregation
windows on `--idle-flush`; a receiving listener does the same on its own
ticker. A remote follow did neither. It has no end of file to flush at and
a connection can stay up for days, so a stream that went quiet held its
last partial record until the connection dropped — and, because 6.57 pulls
the published offset back to the oldest byte the extractor is still
holding, its checkpoint with it. `--idle-flush` was accepted on the
command line and reached only the local path.

`RemoteOptions.IdleFlush` now carries it, and the connection watcher
drives the flush alongside the size probe. `extract.Stream` is
single-threaded state and the watcher runs on its own goroutine, so the
reader and the flusher both go through `remoteStream`, which holds the
extractor, the read position the flush releases, and the flush sequence
that is half of a flushed record's key hint — kept across connections,
because restarting it per connection would hand two different flushes the
same hint.

### 6.71 Smaller corrections

- The catalogue `ETag` folds in the number of stale fields. `Stale` is a
  function of wall-clock time, so the body changed while
  `CatalogueVersion` — which is the *schema* version, deliberately — stood
  still, and a client caching on the validator was answered `304` for the
  rest of the process's life: it never saw a field go quiet, and the W203
  that says so never reached it.
- A request body that could not be read or decompressed is a `400`, not a
  `413`. Every body failure used to come back "too large", which is both
  untrue and expensive: `wire.Client` classifies `413` as fatal, so the
  sink drops the batch, reports it to the delivery observers as a hole,
  and freezes every followed file's checkpoint. A body that really is
  oversized still says so.
- `POST /ingest/v1/samples` reports the denominator the way
  `/ingest/v1/lines` does. It answered `"accepted": <everything in the
  body>` without looking at any of it, so a sender whose samples the store
  would refuse — one carrying no fields, one with a timestamp in the wrong
  unit — was told they had all landed, and the only trace of the loss was
  the store's own log on the far side of the sink.

### 6.72 A `route:` target is a destination set

`route:` fans one pattern out to several sets (03-extraction.md §7), and
its `set:` was the one destination name nothing validated. `set:` on the
pattern is checked against the charset and the reserved prefix; the route's
was only defaulted to it when absent and otherwise taken on trust.

A spec routing to a name carrying the `@` that separates a set from its
shard suffix, or to the store's own reserved prefix, therefore compiled
cleanly — `check` reported it good — and then failed at run time twice
over. `Declarations()` derives field metadata per destination set, so the
first write carrying that declaration comes back `400`, which
`wire.Client` classifies as fatal: the batch is dropped outright, reported
to the delivery observers as a hole, and every followed file's checkpoint
freezes until a later flush thaws it. And every sample the route ever
produces is rejected by the store, per sample, for the life of the
process, with nothing anywhere pointing back at the spec.

Both halves are decidable from the spec, so `Profile.compile` decides
them.

### 6.73 A spec may age out the ingest-progress set

`applySetMeta` exempts `_mensura_ingest` by name — it is the one reserved
set a client may write, so refusing to let it carry a retention meant the
one set every ingester produces was also the one set no spec could age out
(§6.55). The compiler still refused it, which made that exemption
unreachable: the declaration could never leave the ingester. With no
retention the set is routed to the unsharded `@all` shard, which the sweep
skips by construction, and then kept forever with nothing saying so.

The `sets:` block now makes the same single exemption the store does.

### 6.74 The sink's clock is not a lock-free field

`Sink.now` is the seam a test uses to step over a delivery hold without
sleeping through it. It was a bare field, and its only reader is
`holding()` — which the background flush goroutine calls on every tick. So
replacing it raced with that goroutine, and `go test -race` failed
intermittently in whichever test happened to be running rather than in the
one that swapped the clock. It is written through `setClock`, under the
lock `holding()` already takes.

### 6.75 Smaller corrections

- A dictionary hole is not a label value. `labelValue` reported a position
  whose record was lost as the empty string, while `LabelValues` and the
  regex lowering both skip one — so `LABELS host WHERE …`, the filtered
  form, which is the one that translates indices off rows, listed an empty
  value among the hosts and a dashboard variable grew a blank option that
  the unfiltered form never showed.
- A failing read on a followed file is reported. "No terminator, nothing
  consumed" is the shape of a partial line *and* the shape of a read that
  failed, and the failure was dropped along with the record: an unreadable
  file — a disk fault, a revoked permission, a network mount that went
  away — was polled forever, five times a second, with no error from the
  sweep and no log line. `io.EOF` is the one that really means "nothing
  more yet".
- `lag_bytes` covers every followed file. `SetLag` overwrites, and the
  local follower called it once per tailer at the end of that tailer's
  read, so with a glob matching several files the published backlog was
  whichever one the sweep visited last: a file megabytes behind was hidden
  by any other file that happened to be caught up. The sweep now publishes
  the sum once, after the retirement pass, so a file that has left the
  glob stops contributing.
- The auxiliary query forms answer `"series": []`. `Series` carries no
  `omitempty`, so `SETS`, `FIELDS`, `LABEL KEYS` and `LABELS` put
  `"series": null` on the wire — for exactly the queries a dashboard
  variable runs — while every other query answers a list, which is the
  contract the data path states in its own comment.
- `Get` reports a missing indexed row as absent. A tagged forward pointer
  whose index key is gone means the row is gone; falling through decoded
  the pointer's own nine bytes as a row, and `encodeRow`'s leading column
  count read as zero, so `Get` answered "found" with a row carrying no
  columns. The untagged eight-byte form an earlier build wrote still falls
  through, because there those bytes really may be a small row.
- The plugin fetches the catalogue only where it reads it. `CallResource`
  fetched it for every path, and in proxy mode that is a network round
  trip to the store — on `parse`, which the query editor calls on every
  keystroke, and on `print`, neither of which looks at the result.

### 6.76 A `CLAMP` bound list does not swallow the `SELECT` separator

Two comma-separated lists nest in the grammar: `SELECT` separates its
fields with a comma, and a field's `CLAMP` separates its bounds with one.
The `CLAMP` parser consumed a comma unconditionally and then demanded
another bound, so

```
FROM app SELECT cpu CLAMP MIN 0, mem
```

— an ordinary two-field query — was refused with *expected MIN or MAX
after CLAMP*, and the same text is exactly what `Print` emits for that
AST. So a query the builder produced could not be read back, the executed
query string a panel reports did not parse, and the round trip
[06](06-query.md) states as a property was broken for every clamped field
followed by another one. One token of lookahead separates the two cases
without changing the language: a comma only continues the `CLAMP` when
`MIN` or `MAX` follows it. A field whose name collides with either has to
be quoted to be a name at all, and a quoted name lexes as a string rather
than as the keyword the lookahead tests.

### 6.77 An identifier the grammar cannot express is refused

`Validate` checked the shape of a predicate and the sense of every
modifier, and let an *empty* identifier through: a selected field, a `BY`
slot or a comparison whose name is `""`. Such a query executed — the
planner projected a column called `""`, which no row carries, so the panel
came back empty with only a `W203` to explain it — and `Print` emitted
`SELECT ""`, which does not parse, because `p.name` refuses an empty name
for precisely this reason. The parser cannot build one; a builder with a
half-filled row can, and that is the shape a panel is stored in. An empty
name is now `E001` wherever it appears, including inside a `LABELS`
filter, which validates with no schema at all and so had nothing else
that would have seen it.

### 6.78 A refused batch answers in a body the client can read

Every sample a write refuses was named individually in the response, and
the reasons are sentences. A batch the store refuses wholesale — a spec
declaring the wrong epoch unit does exactly that — therefore produced a
body proportional to the batch, and at a configured `--batch-size` of a
few thousand it passed the megabyte `wire.Client` reads. The client got
truncated JSON and reported *unexpected end of JSON input*, which the sink
classifies as neither fatal nor an auth failure: it requeued the batch and
retried it forever, the store re-committed whatever it accepted on every
attempt, and no checkpoint advanced again for the life of the process.

`WriteResponse` now names at most `wire.MaxReportedRejections` of them and
carries `rejected_count` for the total, so the body is bounded by the cap
rather than by the batch. `WriteResponse.Refused()` reads the count where
there is one and falls back to the named list, so a response from a store
built before the count existed still reports the right number; the metrics
counter and the sink's log line both go through it. The client's own read
limit was raised as well, because truncating a write response is not a
recoverable condition — the store has already committed — and the headroom
covers a store that does not cap.

### 6.79 One junk value costs its record, not its window

`model.Coerce` turns the literal text `NaN`, `Inf` or `+Infinity` in a log
line into exactly that float: `strconv.ParseFloat` accepts all three. The
aggregation accumulator discarded the verdict `AsFloat` returns and folded
the value in anyway, so `sum` stayed `NaN` for every later record and a
window seeded with one never moved again — `incoming > NaN` is false. The
window's sample is then refused outright by the sink as unencodable
(§6.30), so one bad line silently erased every record that shared its
window rather than only itself. A non-numeric string was the same failure
one magnitude smaller: it summed as zero.

A value the accumulator cannot use is now reported as an extraction error,
which is counted and shows up on the progress document and in the
`refused` count the HTTP `lines` listener answers with. Absence is still
not junk: a record that simply does not carry the field contributes
nothing and is not an error, which is what `increment` relies on and what
a sparse `sum` source produces — and `increment` counts the occurrence
whatever the field says, because the value is not what it is measuring.

### 6.80 A record is queued whole before a delivery failure is reported

`Sink.Add` buffers the sample and only then flushes, so the error it
returns is the flush's verdict and never a refusal to take the sample. The
follow loop treated it as the latter and returned on the first one, which
left two things wrong at once: the rest of that record's samples were
never queued, and the read offset stayed before a record the *extractor*
had already consumed.

`extract.Stream` is stateful and feeding a record into it is not
idempotent. So the next poll handed the same line to it a second time — an
aggregation window counted the occurrence twice, a multiline join appended
its capture twice — and the window that came back reported more than the
file held. This is the same hazard `applyRewind` exists for: it discards
the extractor precisely because the bytes behind it are about to be read
again.

The whole record is now queued before the failure is reported, so the
extractor stays in step with the offset and nothing is left unqueued. The
samples wait in the sink's buffer, which is where an undelivered batch
belongs: `pending` means *handed to the sink*, and `acked` only ever moves
on a committed flush. The receive path had the same shape without the
double-processing — it has no offsets to re-read from, so its half was a
plain partial loss on the one acquisition path with no way back to the
bytes.

### 6.81 Smaller corrections

- The no-profile backoff is pruned with the tailers. Its entries are only
  removed when the path is looked at again, and a path that has left the
  glob never is, so a pattern whose members come and go — a file per day,
  a file per instance — grew the map by one path string per file for the
  life of the process. `retireUnmatched` already has the set of matched
  paths and now drops what is not in it.
- The aggregate's integer narrowing is range-checked. `emit` asked whether
  the accumulated float is an exact integer by converting it to `int64`
  and back, which the Go specification leaves undefined past the int64
  range — amd64 yields the indefinite value and arm64 saturates. Both
  answered "not an integer" for a value that far out, so the behaviour was
  right by accident on both; it is now right by construction.

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
  (100 000) is where that ends: past it the excess is dropped from the
  oldest end of every buffered set in proportion to its size, counted and
  logged with the reason, because an unbounded buffer turns a store outage
  into an out-of-memory kill that loses everything rather than the tail.
  Losing nothing at all still needs a spill-to-disk queue.
