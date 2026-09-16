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
| Declared field limits are finite and ordered | `pkg/extract/zero_retention_test.go:TestFieldLimitsAreValidated` |
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
| Interleaved streams with no `BY` are reported, not merged in silence | `internal/store/review_test.go:TestNoByInterleaveWarns` |
| A declaration is on disk before the next catalogue tick | `…:TestDeclarationsArePersistedImmediately`, `…:TestRepeatedDeclarationDoesNotResave` |
| Modifiers written out of canonical order are linted, not reinterpreted | `pkg/mql/review_test.go:TestModifierOrderLint`, `…:TestModifierOrderLintNamesTheField` |
| Every acquisition path reports its backlog and its unmatched lines | `internal/ingest/review_test.go:TestRemoteFollowPublishesLag`, `…:TestFollowReportsUnmatchedLines`, `…:TestMergeStreamIsIdempotent` |
| `follow` skips a binary file, as `batch` does | `…:TestFollowSkipsBinaryFiles` |
| A framing bound that would remove itself is refused | `pkg/extract/review_test.go:TestFramingBoundsAreValidated` |
| A replay from `HeldFrom` reproduces exactly the undelivered results | `…:TestAggregationReplayFromHeldFromIsLossless`, `…:TestAggregationReplayIsLosslessOverRandomStreams`, `…:TestMultilineReplayFromHeldFromIsLossless`, `…:TestMultilineReplayIsLosslessOverRandomStreams` |
| Holding a span keeps the list bounded and the floor advancing | `…:TestHoldListStaysBoundedWithInterleavedKeys`, `…:TestHoldFloorStillAdvances` |
| A bare zero is a duration, and `retention: 0` compiles | `pkg/mql/zero_duration_test.go:TestParseDurationAcceptsABareZero`, `…:TestPrintDurationRoundTripsThroughParseDuration`, `pkg/extract/zero_retention_test.go:TestSetRetentionZeroCompiles` |
| Label *keys* are bounded, and a known key still works | `internal/store/label_keys_test.go:TestNewLabelKeysAreBounded`, `…:TestAKnownLabelKeyStillWorksAtTheLimit` |
| Closing waits for the writes as well as the queries | `internal/store/close_drain_test.go:TestCloseWaitsForAnInFlightEngineOperation`, `…:TestEngineOperationsRefuseAfterClose` |
| `Print` carries the nesting bound the parser and validator carry | `pkg/mql/print_depth_test.go:TestCheckPredicateDepthRefusesWhatParseRefuses`, `internal/store/label_keys_test.go:TestPrintRefusesAPredicateDeeperThanTheGrammar` |

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
| Ingest config file, environment-variable configuration layer, `mensura-store config check` | **not implemented**: `mensura-ingest` takes flags only, and only the two bearer tokens come from the environment (§7) |

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
| `L005` | no | a capture that is *not* declared as a label but whose name is one the acquisition layer attaches as a stream label; the sample would carry the name as a label and as a field, which the store refuses outright |

Failing on all of them alike was wrong in a way the L004 text said out loud:
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

### 6.82 A table column's declared type matches its cells

`FORMAT table` and `FORMAT logs` take a column's type from the catalogue
and a cell's value from the row, and the two part company the moment a
field the catalogue calls a gauge carries a string. That is ordinary
rather than exotic: extraction coerces per value, so a `status` field that
is usually numeric holds `"-"` on the lines that have none, and a field
absent from the catalogue is a gauge by default.

The response then advertised a `"number"` column with a string in it. Every
consumer that reads a column by its type dropped the cell — the plugin's
table frame asserts `float64` and leaves the cell nil — so the value
travelled the whole way to the panel and was rendered as an empty box, with
nothing anywhere saying a value had been discarded.

Which cells are strings is not knowable until the rows have been walked, so
`runTabular` records it during the scan and reconciles afterwards: a column
that carried even one string is declared a string column and every cell in
it is rendered as one, and a column the catalogue calls a string is one
whatever the rows held. The plugin's own reader was made tolerant to match,
because a cell arrives there as `any` — straight from the engine in
embedded mode and through `encoding/json` in proxy mode, where every number
is a `float64` whatever it was.

### 6.83 A path that left the glob keeps its checkpoint

`filepath.Glob` reports an unreadable directory as "no matches" rather than
as an error, so one NFS blip, permission change or mount flap makes a sweep
see nothing at all. `retireUnmatched` then retired every tailer, and
`retire` rewinds the resume record to offset zero — so the next sweep
re-read every followed file in full: a duplicate row per record under
`key: offset`, and a re-import of the whole backlog under content keying.

Rewinding is right for the rotation paths, where the path now holds a
different file. It is wrong for a path that has merely left the followed
set, and the two are now separate: `retireKeepingCheckpoint` closes the
tailer and leaves the record alone. Nothing is lost either way, because
`ensure` compares the stored fingerprint against the file it finds — the
same file resumes, a different one still starts at zero.

`ensure` also clears the fingerprint when it decides to restart at offset
zero. A start at zero is what a failed comparison produces, so the hash
still on the loaded record describes a file this tailer has just decided it
is not reading; leaving it there let every commit until the first widening
persist it beside the new file's offsets, which is a checkpoint whose two
halves describe different files.

### 6.84 A comparison matches every position of a duplicated label value

There is one dictionary per label key (05-storage.md section 8, ADR-004),
and `intern` never places a value twice — but an older build repaired a
hole by re-interning a value it had already placed, so a data directory can
hold one value at two positions. `dictionary.index` is one-to-one and
cannot express that, so `host = "a"` resolved to a single position and
silently skipped every row written under the other, while `host =~ /^a$/`
— which walks the entries rather than the map — found both. One predicate,
two answers, with nothing to tell them apart.

The extra positions are collected once at load, where the entries are
already being walked, and `=`, `!=` and `IN` lower to the whole set. It
costs nothing in the ordinary case, where the list has one element and the
lowering is the same `Eq` it always was. `LabelValues` deduplicates for the
same reason: one value listed twice put a repeated option in every
dashboard variable built on that key.

### 6.85 Smaller corrections

- `mode: max` no longer treats an empty window's zero as a reading. A
  window opened by a record that does not carry the aggregated field
  started at zero, and zero is a floor no negative value can beat, so a
  window of only negative readings reported 0 — a number nothing measured,
  on exactly the metrics where negative values are the point.
- `extract.Parse` refuses a document that declares `include:`. There is no
  file to resolve the paths against, so the key was decoded and dropped:
  the spec compiled without every profile, identity rule and set option the
  base contributed, and then reported "no profile matched" for files the
  same spec loaded through `Load` handles.
- Every shed request body carries `Retry-After`, not only a shed write. A
  503 without it leaves the client to invent an interval, and `wire.Client`
  only honours the header — so the API's one "come back shortly" signal was
  sent on `/v1/write` and withheld on the four endpoints that share the
  same in-memory budget.

### 6.86 A fingerprint covers bytes the moment they are read

`checkRotation` runs before `read`, so the content hash covering a poll's
bytes did not exist until the poll after it — and a tailer that had just
been opened had none at all, because `fingerprintWidth(0)` is 0. A rewrite
in place landing in that window was invisible from both directions: the
size test does not fire when the replacement is at least as long as the
old read offset, and there was no stored hash to compare content against.
The widening step then ran on the *new* content and recorded it as the
fingerprint of bytes the tailer had never read, so the head of the
replacement was skipped permanently and silently.

`widenFingerprint` is now called where the read head stops as well as in
`checkRotation`, which leaves no interval between consuming bytes and
covering them. It is still only ever called *after* the comparison, never
before: widening first would hash whatever the file holds now and call it
what we read.

### 6.87 A dropped remote connection checkpoints what it flushed

`followRemotePath` published its resume offset only when the tail exited
cleanly — and a tail that exits cleanly is the rare case, because
`tail -F` does not return. So the ordinary path (the connection drops, the
extractor is flushed into the sink, the loop reconnects) delivered a
window and then resumed from *before* the records it was built from,
rebuilt it and delivered it again. Content keying collapses the two
copies; under `key: offset` the second carries a different `flush:N` hint
— a per-connection sequence number, not a byte offset — and lands as a
duplicate row. Every clean shutdown did the same thing.

The offset is now published on every exit, and it is read off
`remoteStream` rather than from the tail's own byte counter. That is what
keeps the delivery-failure path honest: there the tail's counter has
already moved past a record whose samples were not all queued, while
`remoteStream.consumed` — which only advances once delivery has succeeded
— has not.

### 6.88 A receiver's TCP connections are closed and joined

Cancelling a receiver closed its listener, which stops new connections and
nothing else. The ones already accepted stayed inside a blocking read: the
loop only tests the context between records, and `connIdleTimeout` is
fifteen minutes. So `Receive` went on to `flushAll` and the final
`Sink.Flush` while those goroutines were still turning records into
samples behind it, and whatever they read in that window was buffered into
a sink that had already flushed for the last time — with no checkpoint to
re-read it from, because a socket has none. The UDP drain goroutine was
joined for exactly this reason; the TCP half was left.

Each connection now has a watcher that pulls its read deadline into the
past when the context is cancelled, and `serveTCP` waits for its handlers
before returning. The deadline the loop resets after each record cannot
undo this, because the very next thing the loop does is test the context.

### 6.89 A field kind is validated where it is declared and where it arrives

A kind is a plain string all the way from a spec's `fields:` block,
through the write API, into the catalogue and out to `mql.Validate` —
which compares it against the four it knows and ignores anything else.
Nothing checked it anywhere. So `kind: couter` compiled, was accepted, was
persisted, and silently withdrew every behaviour it was written to switch
on: no `W102` "counter is plotted raw; consider RATE", no `RATE`
pre-selection in the builder, and for a mistyped `string` no `E005`
refusal of numeric modifiers and no string column under `FORMAT table`. A
typo that changes nothing visible except the diagnostics is the one an
operator never finds.

`model.ValidateKind` is now applied in `Profile.compile`, where the spec
author sees it, and in `applyFieldMeta`, where it is client input and
comes back as a 400 — the same treatment set and field names already get,
and for the same reason: a fault in what was sent has to be fatal on the
first attempt rather than retried at full rate.

### 6.90 Smaller corrections

- A profile's, a pattern's and `defaults.labels`' declared label keys are
  validated at compile time. A declared label is a column name on every
  row the profile writes, and it was the one name in a spec nothing
  checked: the store validates it per sample, so `labels: [timestamp]` —
  which names the indexed column — compiled cleanly and then had every
  sample the profile produced rejected, one at a time, for the life of the
  process, with nothing pointing back at the spec. This is the check
  `route:` targets were given in section 6.72, at the same place.
- `runHeatmap` records a group only where a cell exists to render it.
  `groups[]` was written per scanned row, before any bucket column was
  looked at, so a row that produced no cell still cost an entry — and
  neither ceiling ever moved, because both count cells. A set holding more
  than the histogram, grouped by a high-cardinality label, therefore grew
  a map bounded by nothing at all — the unbounded accumulation the
  heatmap's two ceilings were added to stop, one map along from where they
  were put.

### 6.91 `HAS` and `MISSING` name a field or a label

Both lower to the engine's `Exists` predicate, which reads a column off
the row, and a row's columns are its labels *and* its fields — which is
what [06](06-query.md) §4.4 says and what `buildExpr` has always done.
The validator sent them through the label check instead, so `HAS
requests_total` came back `E004: unknown label "requests_total"` — the
worked example in §2 of that same document, refused by the tool that is
supposed to explain it, on a query the executor would have run correctly.
A name that is neither a field nor a label is still `E004`: that check
exists because `MISSING nosuchlabel` otherwise matched every row.

### 6.92 A stream's hold list is pruned where it grows

`extract.Stream.holds` records where each open aggregation window began,
so `HeldFrom` can keep a checkpoint behind bytes whose only copy is
inside the extractor. It was pruned only inside `HeldFrom` — and only a
driver that checkpoints byte offsets ever calls that. The receive path
does not: it has no offsets, it runs for the life of the process, and it
holds one stream per peer. There the list grew by one entry, plus the
aggregation key string it retains, for every window ever opened. A
one-minute `every` over a thousand keys is a million entries a day that
nothing would ever read. Pruning now happens where the entries are added
as well, on the same amortised threshold.

### 6.93 A buffered multiline record holds its own offset

A multiline block is flushed by a *later* line — the next start marker,
the idle timeout, a rotation — and it was then processed under `st.mark`,
which by that point names the flushing line. An aggregation window the
flushed record opens records that mark, so `HeldFrom` reported a position
past the bytes the window was actually built from: a checkpoint could be
acknowledged over records whose only copy was the still-open window, and
a crash or a rewind lost them silently. `processBuffered` restores the
opening line's mark for the duration, and `holdWindow` keeps the hold
list in mark order, because `HeldFrom` reads only its first live entry.

### 6.94 The remote idle flush queues before it releases

Emptying the extractor raises the held offset to the read head.
`remoteStream.flushIdle` used to return its results and let the caller
queue them afterwards, and on the SSH path the reader is a different
goroutine: between the two it could process the next record and publish a
position covering the flushed window's bytes, and a sink flush landing
there acknowledged them while their samples were still on their way to
the sink. The flush and the delivery now happen as one step under the
stream lock, which is what `remoteStream.process` already does for the
reader's own records and for the same reason.

### 6.95 A value the render walk overflowed is not a value

`model.Value.AsFloat` refuses a non-finite *input*, which is what stops a
log line spelling a field `NaN` from reaching a response (§6.23, §6.67).
The walk itself is arithmetic, and arithmetic over the finite floats is
not closed: `DELTA` across two values of opposing sign near the float64
limit overflows to an infinity, `PER SECOND` divides by a raw interval
that may be a single millisecond, and a heatmap cell is a running sum.
One such point used to travel into `wire.Series.Values`, where
`encoding/json` refuses it — on a response whose `200` header has already
been written, so the panel received a truncated body with no status and
no diagnostic, and the whole query was lost to one point.
`runTimeseries` and `runHeatmap` now screen at the wire boundary: a value
that cannot be plotted is emitted as a null, which is what that parallel
array already means by absence.

### 6.96 `AsInt` refuses a float it cannot represent

Converting a float outside the int64 range is undefined by the Go spec —
amd64 yields the indefinite value, arm64 saturates — and `AsInt` is how a
row's indexed timestamp and a bucket set's `total_field` are read. That
field is an ordinary capture, so `model.Coerce` turns `1e300` in a log
line into a float far past the range, and the `tail` and `<bucket>plus`
columns derived from it were then counts nothing had measured.
`aggregator.emit` already range-checked before narrowing; `AsInt` now
does too, and an unrepresentable total falls back to the declared bucket
sum, which is the same answer an absent total gives.

### 6.97 `--max-fatal-drops` reaches the modes it is for

The flag is documented as "unretryable batches dropped before the process
gives up, so a supervisor notices a spec the store rejects". It was
enforced by returning an error from `Sink.Flush`, and only batch import
propagates one: follow, SSH follow and receive all fold a delivery error
into a log line, which is correct for every *other* delivery error and
wrong for this one. So an ingester whose every batch the store refuses
ran at full rate, storing nothing, for as long as it was left alone — in
exactly the three modes that run unattended. The sink now closes a
`GaveUp()` channel when the limit is reached, each continuous mode
selects on it, drains and returns `ingest.ErrGaveUp`, and the command
exits non-zero.

### 6.98 A multiline rule needs both halves of its contract

[03](03-extraction.md) §4 pairs `start_contains` with `continue_regex`:
lines matching the second have the nominated capture appended to the
record the first opened. `continue_regex` is the only test `Process`
applies to a candidate continuation, so a rule without one joins nothing
at all — while still opening a buffer on every start marker. The record
that opened it is then held until the next start marker or the idle
timeout, and on a followed file `HeldFrom` pins the checkpoint to its
offset for just as long. A spec that reads as "assemble these lines"
silently became "delay every one of them", with no join, no error and no
counter anywhere. It is refused at compile time now, on the same
principle as the empty `start_contains` beside it.

### 6.99 The metrics listener validates what it accepts

A line-protocol record carries label keys, field names and a timestamp
that `ParseLineProtocol` does not look at, and nothing else did either:
a sample the store will refuse — a label key outside the charset, epoch
nanoseconds declared as milliseconds — was reported to an HTTP sender as
accepted and passed in silence on TCP and UDP, with the only trace of the
loss in the store's own log on the far side of the sink. It now runs
`Sample.Validate` where `POST /ingest/v1/samples` already did (§6.30).
`--from` and `--to` reach it as well: both flags are honoured on every
path that goes through the extractor, and the line-protocol path and the
sample-posting endpoint are the two that do not, so they read the flags
and stored everything anyway.

### 6.100 Smaller corrections

- **A table query reports its rows as points.** `runTabular` never set
  `Stats.PointsOut`, so a table that returned a thousand rows carried the
  same statistics as one that returned none, on the field an operator
  reads to tell those two apart.
- **`LABELS` of an unknown key is an empty list.** `Store.LabelValues`
  returned nil for a key the store has never seen, and
  `wire.LabelValues.Values` carries no `omitempty`, so `/v1/labels`
  answered `"values": null` there and `[]` everywhere else — the same
  wart `QueryResponse.Series` was fixed for (§6.17).
- **An empty table still draws its columns.** `toFrames` emitted a table
  frame only when rows came back, so a `FORMAT table` or `FORMAT logs`
  query that matched nothing produced no frame at all and Grafana drew
  "No data". It is keyed on the declared columns now: an empty table says
  the query ran, and "No data" says nothing.

### 6.101 An aggregation window is identified by what it produces

`extract.Stream.aggregate` keyed a window on the destination set plus the
`aggregate.on` label values and nothing else, and a window keeps the
field name and the mode of whichever pattern opened it. So a profile with
two aggregating patterns writing one set on the same keys —

```yaml
patterns:
  - {set: s, search: COUNT, aggregate: {every: 1m, on: [op], field: hits, mode: increment}}
  - {set: s, search: LAT,   aggregate: {every: 1m, on: [op], field: lat,  mode: max}}
```

— folded the second pattern's records into the first pattern's
accumulator. The emitted row carried `hits`, whose value was a latency,
and `lat` never appeared at all. Both patterns matched, both records were
counted, and the series that vanished looks exactly like one the source
never emitted. The key now carries the set, the synthesised column, the
mode and the `on` values, so two patterns share a window only when they
really do declare the same column with the same semantics; the fold reads
its mode off the accumulator rather than off the record, so the two
cannot drift apart again.

### 6.102 A flushed multiline record reports its verdict

`Process` returns `(results, err)`, and the drivers turn that error into
the pipeline's counters — `Progress.UnmatchedLines`, `TSParseErrors`,
`ExtractErrors`. A buffered multiline record is almost always flushed by
the next start marker, and that branch discarded the flushed record's
error and reported success. A profile whose *joined* records matched no
pattern therefore reported "0 unmatched" on the console, in the progress
document and in the `_mensura_ingest` set, while `extract.Stats` — which
only `check --sample` reads — counted every one of them. The line in hand
has been buffered rather than judged, so it is owed no verdict of its
own, and the flushed record's is the only one there is to give.

### 6.103 A flush hint outlives the tailer that produced it

A flush of buffered extractor state has no byte offset to key on, so its
key hint is a sequence number — and under `key: offset` that hint *is*
the row's identity. The counter lived on the tailer, and a tailer is
rebuilt whenever its path is retired and comes back: a rotation, or a
sweep in which `filepath.Glob` missed it, which is what an unreadable
directory reports (§6.83). On the retire-and-resume path the byte offsets
carry on from the checkpoint while the flush numbers restarted at one, so
two different windows of one stream were handed the same hint and the
later silently overwrote the earlier. The remote follower keeps its
counter for the life of the path for exactly this reason (§6.87); the
local follower and the receiver now hold one counter each, which is the
same guarantee without a map that grows with every path ever seen.

### 6.104 A schema write that failed is retried, and a failed drop is undone

Two halves of one rule: the in-memory maps may not get ahead of the
records on disk.

`setLocked` persisted only when *that call* had changed something, so a
failed meta write returned its error and left the widened schema in the
map — the next call saw nothing to change, skipped the write, and handed
`PutBatch` a set id whose record on disk does not describe it. Rows
written under that id belong, after a restart, to no set at all:
unreachable by any query, any drop and any retention sweep, which is the
silent orphaning the indexed-column promotion (§6.66) and the
timestamp-less row are refused to avoid. A `dirty` flag now marks a
schema that is ahead of disk and the next call rewrites it; a set whose
id could not be persisted is removed from the maps rather than left
behind.

`DropSet` had the mirror shape: it removed the set from the maps before
applying its range deletes. A pebble batch applies whole or not at all,
so a failure left every row and the meta record on disk while
`db.Sets()` no longer listed the shard — no query scanned it and no later
retention sweep tried again, until a restart brought it all back. The
maps are restored on every failure path; nothing can race the restore,
because `DropSet` owns `dropMu` exclusively for its whole duration
(§6.44).

### 6.105 Smaller corrections

- **An impossible predicate still declares its columns.** `Query`
  returned before the format's own executor ran when the planner folded
  the predicate to constant-false, and `runTabular` builds its column
  declaration before it scans. So `FORMAT table WHERE host = "typo"` came
  back with no columns — and a response with no columns is exactly what
  the plugin renders as Grafana's "No data", which is the case §6.100
  was changed to stop reporting. The work is skipped in `scan` instead,
  so an impossible query still opens no shard, and every format answers
  with the shape it promises.

### 6.106 The interleaving warning the aggregation caveat depends on

Section 8 of [06](06-query.md) states the caveat plainly: with no `BY`,
rows from many streams land in one series, and where two of them report in
the same millisecond the walk's duplicate-timestamp rule (stage 2) keeps
the first and drops the rest. It names `W301` as the mitigation — "until
then the language tells the truth about what it does" — and section 7 of
this document called `W301` the only mitigation there is. Nothing ever
emitted it. A panel selecting `cpu` with no `BY` over ten hosts reporting
on the same second drew five points out of fifty, each one whichever
host's row the scan reached first, with no diagnostic anywhere: the
plausible-looking wrong graph the whole diagnostics table exists to
prevent.

`runTimeseries` now counts what the walk actually dropped. The count is
taken after `render.Series`, which sorts the input slice in place, so
adjacent equal timestamps in that slice *are* the dropped samples rather
than an estimate that depends on scan order. It is worth saying with a
`BY` clause too — two rows becoming one point is a loss either way — so
the code is emitted whenever collapsing happened and only the suggestion
differs: with no `BY`, the warning names the set's own label keys.

### 6.107 The modifier-order lint has to be raised where the text is

`W101` is documented in [06](06-query.md) §4.3 and listed in §12, and
nothing produced it either. It could not have: modifiers are a *set*, so
the order they were written in does not survive into the AST, and
`Validate` — which sees only the AST — has no evidence left to look at.

`mql.ParseDiags` is `Parse` plus the diagnostics that belong to the text
alone. The parser ranks each modifier by its position in the order `Print`
emits, and a written sequence whose ranks do not increase raises `W101`
naming the field. Nothing about execution changes: the flags are set the
same way whichever order they arrive in, which is the property §4.3
promises. `Parse` keeps its signature; the parse endpoints —
`/v1/parse`, and the plugin's `parse` resource in both embedded and proxy
mode — merge the lint into the warnings they already return, which is
where the query editor reads them.

The canonical order the lint measures against is the printer's:
`DELTA`/`PER SECOND`, `NEGATE`, `CLAMP`, `GAP`, `SSE`, `REQUIRED`. §4.3's
parenthetical named the *execution* stage order instead, which is a
different sequence and not the one the canonical text uses; it has been
corrected there.

### 6.108 An unknown `BY` label is an error, as the table always said

§12 of [06](06-query.md) lists `E004` as "unknown label key referenced in
`WHERE` **or `BY`**". The `WHERE` half was enforced; the `BY` half
produced a `W203` — a code whose documented meaning is a field the
catalogue has not seen recently — and carried on.

The catalogue is a superset of what the rows carry: `observeSet` records
every label of every accepted sample, so a key it does not hold is a key
no row in the set has. Grouping by one therefore does not group at all.
Every row falls into the single slot whose value is absent, and a
dashboard that asked for a line per host draws one line over all of them —
the same silently widened query the `WHERE` check refuses. It is now
`E004`, and the message says why. `W203`'s entry has been widened to
cover the other thing it is used for, a field that is not in the
catalogue at all.

### 6.109 A declaration is persisted when it arrives

Everything else in the catalogue is rediscovered by the next write:
`observeSet` re-learns a set's labels, its fields and its time range from
the samples themselves, so losing the last interval of it to an unclean
stop costs nothing. A *declaration* is not rediscovered. `Sink.DeclareFields`
and `DeclareSets` mark it sent and never repeat it, so it travels once per
ingest process — and it was only ever persisted by the thirty-second
catalogue timer.

An unclean stop inside that window therefore lost it for good, with the
ingester still running and never sending it again. For a `sets:` block
that means exactly the failure `SetRetentionFor`'s own comment describes
preventing: the set comes back with no retention and no shard width, is
routed to the unsharded `@all` shard that the sweep skips, and is then
kept forever with nothing saying so. `Write` now saves the catalogue as
soon as a declaration has changed it — measured by `CatalogueVersion`, so
a declaration repeated on every ingest start still costs nothing — and
logs rather than fails, because a metadata write that could not land is
no reason to make the ingester resend data the store is about to hold.

### 6.110 A checkpoint may not land inside a record that has been emitted

`HeldFrom` names the oldest position the stream still needs a replay to
start at, and it named the oldest *open* multiline buffer or aggregation
window. For a profile with one aggregation key that is the whole answer.
With several it is not, and several is the ordinary case: `on: [op]` opens
one window per value of `op`, and those windows interleave.

Key `a`'s window closes while key `b`'s, opened later, is still open. The
floor rises to `b`'s mark — correctly as far as `a` goes, whose sample has
already been handed to the sink — but `b`'s mark sits in the middle of the
records that fed `a`. A restart, or the rewind that a delivery hole
performs, re-reads from there. Those records do not rebuild `a`'s window:
they open a *new* one, starting at the timestamp of whichever record came
first after the resume point, and the record that in the original run
opened the *next* real window is absorbed into it instead. So the store
gains a row nobody measured and loses one that was, both in silence, and
`mode: increment` counters are exactly what this is for.

An aggregator now remembers the position of the last record folded into
it, so a window has a span rather than a point, and the hold list keeps an
entry after its window closes. The floor is pulled back past any emitted
window whose span contains it, to that window's own start, where the
replay rebuilds it whole and the duplicate collapses under a
content-addressed key. One backward pass does it: the list is in mark
order, the floor only falls, and an entry at or past the floor cannot
contain it.

The floor never falls between calls either — a window's span only grows
while it is open, and while it is open it pins the floor to its own mark —
which is what lets the list be pruned without losing a constraint that
matters later: an emitted window goes once a position at or past the
current floor can no longer land strictly inside its span. A window that
absorbed only the record that opened it spans a single position and can
never contain anything, which is every window on a driver that does not
mark its records at all.

A multiline record has the same shape and needed the same treatment. It
spans the line that opened it and every line joined into it, and an
aggregation window opened between two of them pins the floor there. A
replay from inside the span meets each continuation with no buffer open,
so instead of belonging to the record it is judged on its own — and a
continuation a pattern happens to match becomes a whole sample the source
never reported, while the record it should have joined comes back
shorter. A record's span is held the same way once it has been emitted,
including the line that flushed it when that line was itself a
continuation (the timestamp-regression path) and a continuation that
matched no join rule, because both of those have a different fate
depending on whether a buffer is open.

`pkg/extract/review_test.go` checks the property directly: for every crash
point in a stream, what was delivered plus what a replay from `HeldFrom`
produces has to be exactly what an uninterrupted run produced — a missing
result is data loss and an extra one is a row nobody measured — over two
worked interleavings and a hundred and twenty randomised streams, half of
them mixing multiline records with aggregation across several keys. The
trade is only worth making if the floor still moves, so
`TestHoldFloorStillAdvances` drives five keys sharing one window with
heavily overlapping spans and asserts the lag stays in the order of a
window width.

### 6.111 Smaller corrections

- **`lag_bytes` on an SSH follow.** The remote follower learns the file's
  size on every probe and knows its own read position, and published
  neither, so `lag_bytes` read 0 for the whole life of a remote follow —
  which is exactly what a caught-up tail reads like, on the one number an
  operator watches to find out that it is not. Both the between-connection
  probe and the in-connection watcher now publish it. The figure is summed
  across paths first, for the reason `publishLag` sums across tailers:
  `Progress.SetLag` overwrites, so a per-path publish would report
  whichever path probed last.
- **`first_unmatched` on the continuous modes.** `Progress.MergeStream`
  was called from `processFile` and nowhere else, so the single most
  useful spec-debugging output there is was empty forever on follow, SSH
  follow and receive — the three modes that run unattended, where "the
  spec matches nothing" is hardest to notice. It could not be called from
  them as it stood: `extract.Stats` keeps its sample for the life of the
  stream, and a stream that lives for the life of the process can only be
  merged repeatedly, which appended ten copies of one line. It merges by
  value now, so it is idempotent, and the follower (per sweep and on
  retirement), the receiver (per idle tick and per drained peer) and the
  remote follower (per idle tick and per connection) all call it.
- **`follow` skips a binary file, as `batch` does.** §4.2 of
  [02](02-ingest.md) classifies a file holding a NUL byte in its first
  block as binary and ignores it, and says nothing about that depending on
  how the file was opened. Only `processFile` did it, so a glob like
  `/var/log/*` — which matches `wtmp`, `lastlog` and every journal
  fragment — handed those bytes to the profile's regexes on every poll,
  and any pattern loose enough to match invented samples out of them. The
  sniff is now in `ensure` too, counted on the same counter, and the path
  goes on the same slow retry the no-profile case uses.
- **A framing bound that is declared may not be ignored.** Every reader of
  `framing.max_record_bytes` tests `n > 0`, so a negative value *removed*
  the cap the operator was trying to set — the failure the cap exists to
  prevent, reached by declaring the cap. A `multiline.idle_timeout` that
  is negative or zero is never *not* elapsed, so every buffered record was
  flushed on the next idle tick and the rule joined nothing. Both are
  refused at compile time now, on the same grounds as
  `identity.scan_lines` (§6.51) and a negative `sets:` retention.
- **`engine.indexKeyValue` is gone.** It recovered the indexed value from
  an index key and nothing has ever called it.

### 6.112 An aggregation key nobody bounded

An aggregation window is one accumulator plus a copy of the opening
record's labels and fields, and `aggregate.on` opens one per distinct
tuple per window period. Nothing bounded that set. `every: 1h` over a key
whose cardinality the spec author misjudged — a request id, a path with
parameters, anything the `labels:` list happens to admit — grew the
ingester's memory for an hour at a time, with no cap, no counter and no
diagnostic, in a process that is otherwise careful to bound every buffer
it holds: the store's `max_label_cardinality`, the receiver's `max_peers`,
the sink's `max_buffered_samples`, and the heatmap group map that §6.90
was written about.

`maxOpenWindows` (100 000, the same order as a label key's budget) is
where it ends. Past it the oldest-*ending* windows are emitted early,
handed back to the caller with the record's own results, and counted in
`Stats.WindowsForcedClosed`, which `check` prints. Emitting early is the
least-lossy answer available: the row still reaches the store carrying a
window shorter than the spec declared, which is a visible loss of
resolution, while refusing the record drops a measurement outright and
holding on is how the process dies. The hold list is unaffected — a
force-closed window leaves its span behind exactly as an expired one
does, so §6.110's floor still pulls a checkpoint back past it.

### 6.113 `check` reports how lossy an aggregate is

§9 of [03](03-extraction.md) says aggregation is lossy on purpose and
that `check` prints, for each aggregating pattern, an estimate of the
reduction, "so the trade is visible". So does the `Aggregate` type's own
doc comment. Nothing measured it, so the one number that makes the trade
visible was never printed and the decision to aggregate had to be made
blind.

`Stats.Aggregates` now holds one entry per aggregating pattern, in
pattern order, counting the records it absorbed and the rows those
became; a window is counted against the pattern that opened it wherever
it is emitted (expiry, flush, or the cap above). It is a measurement
rather than an estimate, and `check --sample` prints it as
`N record(s) -> M row(s), Kx reduction`.

### 6.114 A flush reports the verdict on what it flushed

§6.102 gave `Process` back the verdict on the multiline record that a
start marker flushes, because the drivers are what turn a verdict into
`Progress.UnmatchedLines`, `TSParseErrors` and `ExtractErrors`. The other
three flush paths — a rotation, a retirement, the idle tick — still
discarded it, and on a stream quiet enough to need an idle flush those
are *all* of them: a multiline profile whose joined records match no
pattern reported "0 unmatched" on the console, in the progress document
and in the `_mensura_ingest` set, exactly as before the earlier fix.

`Flush` and `FlushIdle` return `([]Result, []error)` now, one verdict per
record they judge, and every driver reports them. `Flush` also walks the
multiline rules in the profile's declared order rather than in map order:
the samples one flush produces are keyed by a flush sequence number plus
their position in the batch, so an order that varies from run to run keys
the same record differently under `key: offset`.

### 6.115 Every column name a pattern can write is validated

A capture becomes a column on every row the pattern writes, and the store
validates a column name per sample. A name it refuses therefore cost one
rejection per record for the life of the process, with nothing anywhere
pointing back at the spec — the failure mode that `route:` targets
(§6.72), `kind:` (§6.88), profile label keys (§6.89) and bucket columns
were each given a compile-time check for.

The names that had none were the regex group names themselves, the field
an `aggregate:` synthesises and the keys of `default_values:`. All three
are now held to the same rule the store holds them to, classified the way
`Stream.isLabel` classifies them: a declared label goes through
`ValidateLabelKey`, anything else through `ValidateFieldName`. The two
reserved histogram payload groups, `buckets` and `histogram`, are skipped
because `expand()` consumes them and they never reach a row.

### 6.116 A truncated record is not a record

Two acquisition paths cut a record in half and then extracted it, which
is the failure `handleConn` refuses a connection-cut fragment for: a
prefix-anchored pattern matches half a line and invents a sample from a
number that was cut in two.

- **UDP.** `recvfrom` hands back `min(len(buf), datagram)` with no error
  and discards the rest, so a socket buffer of exactly
  `--max-datagram-bytes` truncated an over-long datagram in silence —
  no counter, no log. The buffer is one byte larger than the cap now, so
  the overrun is detectable; such a datagram is counted on
  `oversize_records`, reported through the collapsing per-record warning,
  and dropped rather than extracted. There is no reassembly and no
  re-read, so a prefix is not the record the sender meant.
- **`POST /ingest/v1/lines`.** The body was read through an
  `io.LimitReader` at `maxHTTPBodyBytes`, so a longer one was cut
  part-way through a record, that half was extracted, and the response
  was `200` — telling the sender a body it never finished sending had
  landed whole. The reader now carries one byte of headroom and the
  handler answers `413` when the body overruns, which is what the store's
  own `readBody` does with the same condition (§6.71).

### 6.117 Smaller corrections

- **A declined input is named once.** `Progress.NoProfile` and
  `SkipArchive` append to a twenty-entry list, and the follower
  reconsiders a path that matched no profile every `noProfileRetry`,
  forever — so within twenty minutes the list held twenty copies of the
  first such path, and every other unmatched path, which is what an
  operator opens the list to find, could never appear.
- **W102 is only offered where it can be taken.** "This field is a counter
  and is plotted raw; consider `RATE`" was emitted for every counter
  column of every `FORMAT table` and `FORMAT logs` query — where `E008`
  refuses `RATE` and `DELTA` outright. A warning that recommends the one
  thing the same validator will reject teaches an operator to ignore the
  warnings that matter. W103 was already gated on the format; W102 is now
  too.
- **The `LABELS` filter says each thing once.** The predicate is lowered
  once per set carrying the label and resolves against one dictionary for
  the whole store, so `LABELS host WHERE pool = "typo"` came back with one
  identical `W201` per set — a dozen copies of one sentence on a
  dashboard variable.

### 6.118 A predicate that nests deeper than the stack

The MQL predicate parser is recursive descent, and nothing bounded its
recursion. Go's stack overflow is not a recoverable panic: it is a fatal
runtime error, so `net/http`'s per-request recovery cannot catch it and
the process exits. `FROM app SELECT cpu WHERE ((((…` with a million
parentheses is 1 MB of text — well inside the store's default
`max_request_bytes` of 32 MiB — and `POST /v1/parse` is the endpoint the
query editor calls on every keystroke. In `mode: plugin` the process it
took down is the one holding the data directory.

Three bounds, all in `pkg/mql`:

- **`MaxPredicateDepth` (64) in the parser.** Counted in `parseUnary`,
  which is the one function every nesting construct passes through — a
  parenthesised group and `NOT` alike — so `a AND b AND c`, which is a
  loop rather than a nesting, stays at depth one. The deepest predicate
  anywhere in these documents is three.
- **The same bound in `Validate`.** An AST arrives as JSON, whose decoder
  allows far deeper nesting than the printer, the store's lowering or the
  engine's evaluator are written for, and the AST is the canonical form:
  a panel stores it directly. Refusing it at the depth the text grammar
  refuses also keeps `Print` → `Parse` total, which [06](06-query.md) §7
  promises.
- **`MaxQueryBytes` (1 MiB) in the lexer.** Lexing materialises one token
  per punctuation character before the parser sees any of them, and a
  token is several times the size of the byte it came from, so a 32 MiB
  body of parentheses is over a gigabyte of tokens per concurrent
  request. Every other buffer in this system is bounded; this one was
  reached through the query editor's own endpoint.

### 6.119 A join capture is a slice index

`multiline.join[].capture` names a regex group, and `Process` reads
`g[j.Capture]` after testing `len(g) > j.Capture` — which is true for
every negative value. `capture: -1` therefore compiled cleanly, passed
`check`, and then panicked with an index out of range on the first
continuation line that matched, on whichever goroutine happened to be
extracting: a batch worker, the follow poll loop, or a receive
connection. A panic on any of them takes the ingester down.

The other end fails open instead of loudly, which is the failure an
absent `continue_regex` is already refused for (§6.98): an index past the
last group makes the same test false for every line, so the rule joins
nothing while still opening a buffer on every start marker and holding
the checkpoint behind it. `compile` now requires
`0 <= capture <= re.NumSubexp()` and names the valid range.

### 6.120 Two spec numbers that were parsed leniently

`fmt.Sscanf` with `%g` stops at the first byte it cannot use and reports
no error for the rest, which is how `retention: 1h30d` used to expand to
`24h` before the config loader was moved off it. The same call was still
reading bucket-set edges: `edges: linear:5x` scanned as a step of 5 and
`explicit:[3zzz, 1, 2]` as an edge of 3 — a typo silently becoming a
different histogram axis. Both go through `strconv.ParseFloat` now, which
consumes the whole string or fails.

Their *values* are checked as well, because [03](03-extraction.md) §8
defines an edge as a bucket's numeric lower bound and the heatmap draws
its y-axis from them in order. A `linear:` step of zero gives every
bucket the same bound and a negative one runs the axis backwards; either
way `heatmapSeriesName`, which names a series after its edge, hands the
panel several series with one name. Explicit edges must ascend for the
same reason.

### 6.121 Everything a spec declares is decided at compile time

Three declarations were still resolved at run time, so a typo compiled,
passed `check`, and then failed once per stream for the life of the
process:

- **`select.path_glob`.** `filepath.Match` reports a malformed pattern as
  an error *alongside* "did not match", and `matches()` discarded it — so
  `path_glob: ['app[.log']` matched no file at all and surfaced at import
  time as "no profile matched", which points at the file rather than at
  the pattern.
- **`defaults.timestamp.timezone`** and **`assume_year`.** Both are
  resolved in `NewStream`, which runs once per file on a batch import,
  once per connection on a receiver, and on *every poll* of a followed
  path — so an unknown zone name logged "cannot follow x" four times a
  second, forever, from a spec `check` had called good. `assume_year` is
  range-checked as well as parsed: a year-less layout parses into year 0
  and is shifted by this number, so a value outside 1970..9999 produces a
  timestamp the store refuses per sample.

### 6.122 A heatmap column is one value, not a pair

The window formula in [07](07-downsampling.md) §2 doubles its width, and
says why: each window emits both its min and its max, so sizing it to two
points' worth of the render budget lands the output on what the panel
asked for. A heatmap column is a single summed value (§6.4), and it was
charged the doubled width anyway — so a panel asking for 100 data points
drew 50 columns, with nothing saying so and no way to recover the
resolution short of an explicit `EVERY`. `render.SingleWindow` is the
undoubled width; `runHeatmap` and `Explain` both read the heatmap's width
from one place, so the plan and the executor cannot disagree.

### 6.123 A checkpoint is fsynced on a timer, as it always said

[02](02-ingest.md) §5 describes a resume record as "written atomically
(temp + rename) and fsynced on a timer and on clean shutdown". The timer
was not there: `EndFlush` persisted every acked commit, and a commit
happens whenever the batch fills or the 50 ms flush tick finds samples.
Each one is a write, an fsync, a rename and an fsync of the directory,
serialised on `CheckpointStore`'s own mutex, on the goroutine that also
holds the sink's delivery lock -- so a busy follow of fifty files was
thousands of fsync pairs a second and the state directory became the
pipeline's throughput ceiling. On a network-mounted one it is a hard stop.

`checkpointSaveInterval` (one second) is now the floor between writes for
one stream, in both followers -- `tailer` for local files, `checkpointBox`
for SSH paths. The in-memory acked offset still advances on every commit,
because that is what `BeginFlush` and `EndFlush` reason about; only the
disk copy is coalesced, and a deferred record is *owed* rather than
dropped, so the latest offset is what eventually lands. It is written by
whatever clock the path has -- the poll sweep locally, the probe and idle
ticks over SSH -- and forced where deferring it would lose it: a
retirement that removes the tailer from the map, the end of an SSH
connection, and both shutdown exits, after the final flush.

Two writes are never deferred, because the offsets on disk stop describing
the file the moment they happen: the rewind a rotation performs, and the
record that carries a new remote file identity.

What it costs is that an unclean stop resumes from a record up to an
interval behind, so at most a second of records is re-read. That is the
guarantee this pipeline already offers everywhere else -- at-least-once,
made exactly-once by content-addressed row keys (§6.5).

### 6.124 Smaller corrections

- **An aggregation key is length-prefixed.** The window key joined its
  components with `\x00` and paired each `on` key to its value with `=`,
  both of which a label value may itself contain — so `{a: "x", b: "y"}`
  and `{a: "x\x00b=y"}` built the same key and two windows measuring
  different things folded into one accumulator. This is the ambiguity
  `model.PrimaryKey` and the store's `seriesKey` were each rewritten to
  remove, and it is removed the same way: a length cannot be forged from
  content.
- **A spec-declared shard width says when it is rounded.** A shard suffix
  can express whole days and the hour counts that divide one, and nothing
  else. `Open` warns about an inexact width from the config file;
  `warnInexactShardWidths` reads `cfg`, and a width from a spec's `sets:`
  block never goes there — which is the documented way to declare per-set
  sharding and the only way an ingester can. `shard: 30m` became hourly
  shards in silence. `SetRetentionFor` now says so, once, on the call
  that changes it: the declaration arrives again on every ingest process
  start.

### 6.125 Retention drops a set's data, not its declaration

A set whose every shard has aged out was forgotten completely: the
catalogue entry went, and with it the `retention`, `shard` and `key`
that a spec's `sets:` block had declared for it.

The two halves of a catalogue entry are rediscovered on very different
schedules. The observed schema — which fields, which labels, what time
range — comes back with the next sample, because `observeSet` re-learns
it from the samples themselves. The declaration does not:
`Sink.DeclareSets` marks it sent with the first write and never repeats
it, so an ingester that is still running when its set ages out will never
say it again. Dropping the declaration therefore dropped it for good, and
the next record re-created the set with the store's own defaults. Under
the deployment [09](09-operations.md) documents — `--retention 0`
globally, per-set retention from the spec — that means no retention at
all, so the set was routed to the `@all` shard the sweep skips and then
kept forever. The key scheme went with it: a set declared `key: offset`
silently reverted to content keying, so two records sharing a millisecond
and a label set collapsed into one row, and the store's own "this set
needs a `key_hint`" refusal stopped firing.

`RunRetention` now calls `forgetAgedSet`, which clears the fields, the
labels, the bucket sets and the observed time range — what the shards
really did take with them — and keeps what was declared, in the live
override maps and on the persisted entry. A set with nothing declared has
nothing to keep and is still removed outright, which is what this sweep
has always done and what `DELETE /v1/admin/sets/` still does through
`ForgetSet`.

### 6.126 A capture that did not participate is an absent field

Go reports a regex group that did not participate as the empty string,
which is indistinguishable from one that matched nothing. The label
branch of the pattern stage has always treated an empty capture as
absent; the field branch stored it, so an ordinary optional capture —
`op=(?P<op>\S+)(?: lat=(?P<lat>\d+))?` — wrote `lat: ""` on every record
that omitted it.

Three things went wrong with that at once. `default_values`, whose
documented job ([03](03-extraction.md) §7.5) is to fill "the captures the
matching regex did not produce", never fired, because the key was
present. `FORMAT table` saw one string cell in a numeric column and
retyped the whole column. And an `aggregate:` pattern refused the record
outright — *aggregate field "lat" is , which is not a finite number* — so
a spec with one optional numeric capture lost **every record that omitted
it**, not just that column.

An empty capture is now absent for fields as well as for labels. A record
whose every capture is empty is an extraction error rather than a row of
empty strings, which is what `default_values` exists to prevent.

### 6.127 A window that measured nothing writes no row

`aggregator.emit` wrote the accumulator unconditionally, so a `max` or
`last` window in which no record ever carried `aggregate.field` reported
`0` — a latency maximum nobody measured. `hasValue` was added to stop the
seed from beating a negative reading; this is the case where there is no
reading at all.

The synthesised column is now written only where the accumulator holds
one, or where zero is the identity of the fold: `sum` of nothing is zero
and `increment` counts the record that opened the window, so both always
have one. A window left with no columns writes no row, because the row it
would write is refused by the store for carrying no fields, and
`Stats.WindowsEmpty` counts it so `check` can say how often a spec
aggregates on a field its own pattern does not reliably capture. It is
the rule `BucketSet.expand` already follows when it writes an absent
bucket as absent rather than as a measured zero.

### 6.128 The request-body budget covers the body, not the read

`limits.max_buffered_request_bytes` bounds the request-body bytes the
whole API holds in memory at once. It did not: the reservation was
released the moment the read finished, while the caller still held the
whole body, decoded it into a request several times its size and then
queued for a write slot with both alive. So the budget only ever saw the
bodies that happened to be *arriving* at the same instant — for fast
clients, almost none of them — every request found the counter back at
zero, every request was admitted, and the peak footprint was still one
`max_request_bytes` per connection.

`API.readBody` now returns the release the handler owes, and every
body-reading handler defers it. The reservation is also corrected
downwards once the size is known: admission has to be pessimistic,
because a gzipped body may decompress to anything up to the limit, but
charging every 4 MiB batch for the 32 MiB it might have been would shed
at a small fraction of the configured budget.

### 6.129 Proxy-mode validation is not a round trip per keystroke

The query editor calls the `parse` resource on every character typed, and
proxy-mode validation needs the upstream catalogue and the upstream
ceilings — so each keystroke cost two HTTP requests to the store, and
rendering a catalogue walks every shard on the far side. That is the same
cost `CallResource` stopped paying by not fetching the catalogue for
`parse` itself, one layer further in. `remoteService` now caches both
behind a one-second TTL: long enough to collapse a burst of typing into
one fetch, short enough that a set appearing upstream shows up while the
author is still looking at the editor. Nothing correctness-critical rides
on it, because the store re-validates every query it executes against its
own live catalogue.

### 6.130 Smaller corrections

- **Two multiline rules may not share a `start_contains`.** The stream
  keys its open buffers by the marker and `Process` returns at the first
  rule whose marker the line carries, so the second rule's `join` list,
  its `idle_timeout` and its buffer are unreachable — and `Flush` walks
  the declared order and deletes the shared buffer on the first of them,
  so the second never finds one either. It is the dead configuration an
  empty `start_contains` and a missing `continue_regex` are already
  refused for, arriving through a duplicate instead of an omission.
- **Two bucket sets may not share a name.** They are resolved by name, so
  a repeated one silently replaces the earlier declaration and its
  columns, edges and unit are never written by anything. Duplicate
  profile names were already refused on the same grounds.
- **The auxiliary query forms report their statistics.** `SETS`,
  `FIELDS`, `LABEL KEYS` and `LABELS` answer with the same envelope as a
  data query and left `duration_ms` at zero — the one field an operator
  reads to tell a dashboard variable that takes seconds from one that is
  instant — and the `LABELS … WHERE` filter scan left `points` at zero
  too, which is the omission `runTabular` was fixed for.
- **An aggregation key that a record did not carry says so.** Compile
  refuses a key no regex captures and one that is not declared as a
  label, so at run time the only way to reach that error is a record
  whose group did not participate. Reporting it as "not a declared label"
  sent the operator to a spec that is correct.

### 6.131 A LIMIT keeps the rows the format promises, not the rows the scan met first

`FORMAT logs` walks the range newest-first and stops as soon as it holds
`LIMIT POINTS` rows; `FORMAT table` does the same walking forwards. Both
rely on the scan meeting rows in time order, which needs the selected
shards to be disjoint — and two ordinary configuration changes make them
overlap.

Withdrawing a set's retention (`sets: {app: {retention: 0}}`, or the
documented "`--retention 0` globally" posture) routes later writes to the
unsharded `@all` shard while the dated ones are still on disk, and `@all`
covers every instant. Changing a set's `shard:` width leaves a wide shard
straddling narrow ones. `@all` sorts first, because it is the shard that
starts earliest, so a `FORMAT logs` query stopped after the *oldest*
shard and answered with the oldest rows — under an error string saying
"the newest were kept". `FORMAT table` did the mirror image.

`shardsInRange` now reports whether the shards it selected can hold a row
at the same instant, and `runTabular` keeps the early stop only where
they cannot. Where they can, the walk sees every row and the buffer is
compacted to the best `LIMIT` rows as it goes, so the memory it holds is
still bounded by the limit rather than by the range.

### 6.132 Closing the store waits for the queries that are running

A query holds a pebble iterator over a snapshot for the whole of its
scan, and `DB.Close` releases the sstable readers and the cached blocks
that iterator is reading out of; pebble's own contract is that every
iterator is closed first. Nothing guaranteed the ordering. In server mode
the HTTP listeners are shut down before `Store.Close`, but
`http.Server.Shutdown` gives up after its own timeout and returns while a
long scan is still running. In plugin mode Grafana's queries never go
through an `http.Server` at all: `ServeLocal` keeps answering until the
process exits, so a `SIGTERM` during a panel refresh closed the engine
under a live iterator every time.

Every query path already takes a slot from `MaxConcurrentJobs` for the
whole of its scan, so `Close` now takes all of them before it closes the
engine, bounded by 30 seconds because a query that will not finish must
not stop a store from shutting down. The slots are never given back:
`Query` refuses outright once the closing flag is set, with a `503` and a
`Retry-After` rather than a blocked semaphore.

### 6.133 Every column a bucket set writes is a distinct name

The buckets of a bucket set are fields on one row, and `expand()` derives
`<bucket>plus` and `tail` from them onto the same row, so two of them
carrying one name is not a duplicate declaration — it is a corrupted
histogram. `expand()` adds each occurrence of a bucket into the running
sum it derives the tail from, so a bucket listed twice made the tail
short by that bucket's own count (often clamped to zero) and every
cumulative column below it wrong with it. A bucket named after another
bucket's cumulative column, or called `tail` alongside `tail: true`, had
the count the source really reported overwritten by a derived number. A
`total_field` naming one of the buckets read that bucket's count as the
total. None of it is visible at run time: the row is well formed and the
heatmap draws. `Profile.compile` now claims each column name once and
names both declarations when two collide — the check a pattern
*capturing* `tail` was already given, from the other side.

### 6.134 Smaller corrections

- **A query with no AST is a `400`.** `Store.Query` returned a bare error
  for it, and `handleQuery` turns anything that is not a diagnostic into
  a `500` — so a request that simply forgot its AST was reported as a
  fault in the store, on the status `wire.Client` reads as "come back
  later". It is `E002` now, like every other malformed query.
- **`NoBlockCache` allocates a cache.** Leaving `pebble.Options.Cache`
  nil does not disable the block cache: pebble's `EnsureDefaults`
  allocates an 8 MiB one. So `db: {cache_bytes: -1}` — the setting an
  operator reaches for precisely to reclaim that memory on a small host —
  silently handed them 8 MiB of it. Pebble has no "no cache" mode, so the
  sentinel is translated into the smallest cache it will build, one that
  holds nothing.

### 6.135 A bare zero is a duration

`printDuration` emits `0` for a zero — there is no unit to pick, because
every unit gives the same answer — and the MQL parser accepts that token
wherever a duration is expected, so `GAP 0` has always round-tripped.
`mql.ParseDuration` did not: it required a unit, so `ParseDuration("0")`
failed on text this package itself prints.

That function is also how the extraction spec reads every duration it
declares, which made the disagreement visible to operators rather than
only to the printer. `sets: {app: {retention: 0}}` is the documented way
to withdraw a set's retention — [05](05-storage.md) §7.1 and §6.131 above
both name that exact form, and the store's own `--retention 0` accepts it
— and it failed to compile with `invalid duration unit in "0"`. One
spelling of one setting worked on the store and not in the spec that
configures it.

A bare zero is now accepted, and only a bare zero: a unit-less number
other than zero still fails, because there is no default unit to guess
and guessing would silently change what the spec asks for.

### 6.136 The number of label *keys* is bounded, not only their values

`max_label_cardinality` caps how many distinct values one label key may
hold. Nothing capped how many keys there are.

Every key allocates its own dictionary, is recorded in the catalogue —
which is persisted as a single JSON record — and is never reclaimed,
because §6.0 makes the dictionary global per key and a value dropped from
one would relabel the rows of every set that still carries it. A sender
chooses its own label names on three paths that reach the store: the
line protocol, `POST /ingest/v1/samples` and `/v1/write` itself. So the
one dimension of the dictionary nobody bounded was the one an
unauthenticated loopback deployment hands to whoever can reach the port,
in a store that otherwise bounds every buffer it holds.

`limits.max_label_keys` (default 1000, far past any spec in these
documents) refuses a *new* key past the budget, by name and per sample,
exactly as the value budget already does. A key the store already holds
always works, so the bound is on growth and never on use, and a negative
value switches it off the way the other gates are switched off.

### 6.137 Closing the store waits for the writes as well as the queries

§6.132 made `Close` take every job slot before tearing the engine down,
because pebble's contract is that `Close` may not run concurrently with
any other DB method. That covered the read half only.

A write is the operation that runs long here: `PutBatch` blocks on the
engine's own back-pressure when L0 is stalled, which is exactly the
condition under which a shutdown overtakes it. In server mode the
listeners are shut down first, but `http.Server.Shutdown` gives up after
its own timeout and returns while a handler is still inside the engine.
So a `SIGTERM` during a compaction stall closed the engine underneath a
batch that was still applying — the same unsound teardown §6.132
describes, through the other door.

`Store.enterEngine`/`leaveEngine` now count every operation that reaches
the engine — `Write`, `Compact`, `Flush`, `RunRetention`,
`SaveCatalogue` and the admin drop — and `Close` waits for them under the
same 30-second bound `drainJobs` uses, saying so if it expires. Nothing
new starts once `Close` has begun: each of those answers `ErrClosed`,
which the HTTP layer already renders as a `503` with `Retry-After`.

### 6.139 A `default_values` entry for a declared label is a label

`default_values:` fills "the captures the matching regex did not produce"
([03](03-extraction.md) §7.5), and §5 of that document says a named capture
becomes a label or a field according to how the profile classifies it. The
defaults did not follow the classification: `process()` wrote every one of
them into the field map, unconditionally, whatever the spec had declared.

That broke a default for a declared label in both directions at once, and a
profile as ordinary as

```yaml
labels: [status]
patterns:
  - set: app
    search: 'req'
    extract: ['req(?: status=(?P<status>[a-z]+))? ms=(?P<ms>\d+)']
    default_values: {status: unknown}
```

hit both on alternating records. On a line where the capture *did*
participate, the value went to the labels — so the field slot was still
empty, the default fired as well, and the sample carried `status` as a label
**and** as a field. That is the one shape `store.rowFor` refuses by name
(`"status" is both a label and a field`), so every such record came back as a
named rejection: the write API reported `accepted: 0`, the ingester logged a
collapsed warning, and nothing anywhere pointed at the spec. On a line where
it did not participate, the default landed as a column instead of the label
it was declared to be — so the row fell outside its own `BY` group and the
catalogue grew a `status` field nobody declared.

The defaults are now classified exactly as the captures they stand in for
are, through the same `Stream.isLabel` the capture loop uses, and an empty
default is an absent one for a label just as an empty capture is (the store
refuses an empty label value outright). The compile-time name check already
read these keys as labels — `isDeclaredLabel` validates them against the
label charset — so this is the classification the spec was already being held
to.

The mirror case is refused rather than reclassified: an `aggregate:` block
synthesises its `field` as a column by construction, so a spec that also
declares that name as a label is a contradiction the pipeline cannot honour,
and `Profile.compile` now says so instead of emitting the same
label-and-field collision from the other side.

`Lint` gained `L005` for the third way into it, which no rule covered: a
capture the profile does **not** declare as a label, whose name is one the
acquisition layer attaches as a stream label anyway — `host` and `source`
always, plus every name an `identity:` rule can produce. Such a pattern's
every sample carries the name twice and is refused by the store, and the only
trace was a line in the store's log on the far side of the sink. It is
advisory rather than fatal for the reason L004 is: an `identity:` rule scoped
by `match_path` does not apply to every stream, so a pattern in another
profile may legitimately own the name.

### 6.140 A clause that is read by nothing is refused, not dropped

Three declarations were accepted by the validator and then read by no
executor, which is the failure the unknown-`FORMAT` check
([§6.40](12-implementation.md)) and the timeseries-only-modifier rule were
both added to prevent: the panel looks right and is the wrong shape.

- **Per-field modifiers on a `HISTOGRAM()` selection.** A bucket set
  resolves to no rendered field: `plan()` expands it into bucket columns and
  appends nothing to `p.fields`, and `runHeatmap` sums counts per window
  without ever building a `render.Spec`. So `RATE`, `DELTA`, `NEGATE`,
  `CLAMP`, `GAP`, `SSE` and `REQUIRED` were parsed, validated, stored in the
  panel and then discarded — `HISTOGRAM(hdr24) RATE` drew raw counts under a
  legend the author read as a rate. Now `E008`, naming the modifiers it
  refuses. `AS` is not a modifier and still names the series.
- **`EVERY` and `LIMIT SERIES` under `FORMAT table` or `FORMAT logs`.**
  `EVERY` is a downsample window and a tabular format does no downsampling
  ([06](06-query.md) §4.7); `LIMIT SERIES` bounds a series count and a
  tabular format produces rows. `runTabular` reads neither. Both are now
  `E008`, which is the same rule the per-field modifiers on those formats
  already follow. The *grammar* still accepts both, so `Print` → `Parse`
  stays total for an AST stored before the rule existed.
- **`Explain` reported a `downsample_window` for a tabular query.** The
  number described a stage that does not run, on the one endpoint whose
  contract is that it describes the plan the executor runs. It now reports
  zero for `FORMAT table` and `FORMAT logs`.

An unrecognised `kind` was the fourth: `Store.Query` switches on it with a
data-query default, so `{"kind": "Labels"}` executed as a timeseries query
and came back as "query has no FROM set", naming the wrong clause. It is now
`E008` naming the kind, the way an unrecognised `format` is. An absent kind
is still a data query — that is what `MarshalJSON` writes and what every
hand-authored panel omits. `E010`, which the validator has emitted since
§6.14, was also missing from [06](06-query.md) §12's table of codes; the
table's own claim is that every diagnostic has one.

### 6.141 Two smaller store corrections

- **A label key whose first value could not be written is not left
  behind.** `intern` allocates the dictionary before it persists the value,
  so a `PutDict` that failed — a full disk, a transient fault — left an
  empty dictionary in the map. Nothing ever removes one, and it is not
  recoverable from disk either, so it spent one of `max_label_keys`
  ([§6.136](12-implementation.md)) on a key that can never answer a lookup:
  the same slow leak of a budget the hole put back on the free list beside
  it exists to avoid, one level up.
- **`applySetMeta` validates every declaration before it applies any.**
  `applyFieldMeta` has always had that shape. `applySetMeta` validated and
  applied in one pass, so an entry that the *next* one made the request fail
  landed anyway: the caller got a 400 and the store kept half of what it
  refused, which for `sets:` means a retention and a shard width nobody's
  spec asked for, persisted in the catalogue.

### 6.142 A rotated file's last record survives even without its newline

`follower.read` holds an unterminated record back: the offset stays before
the incomplete bytes and the next pass re-reads them whole. That is right
while the file is live — a line read mid-write, handed to a prefix-anchored
pattern, invents a sample from a number that was cut in two, which is the
failure the TCP listener discards a cut-short record for.

It is wrong at the one moment there is no next pass. `checkRotation` retires
a handle when the path no longer resolves to it (rename-and-create) or when
the inode has been unlinked, and those bytes can never grow again: they are
as complete as they will ever be. The handle was closed over them anyway, so
a writer killed mid-line, or a rotation that landed between a record and its
newline, lost that record silently — while batch import processed the same
trailing bytes (`len(rec.Line) > 0 || rec.Terminated`) and `handleConn`
processed them when a sender closed cleanly. "The same file produces
different data depending on how it was read" is the reason `read` gives for
consuming an over-long record rather than discarding it; this is the same
principle one case along.

`drainFinalRecord` runs from `retire()` and from nowhere else. It is
deliberately not called by `retireKeepingCheckpoint` — the path that leaves
the followed set, which `filepath.Glob` reports for an unreadable directory,
where the file is very likely still being written — nor by `closeAll` at
shutdown. Both of those leave a checkpoint behind, and that checkpoint is
what re-reads the record whole on the next pass.

### 6.143 An aggregation window's span reaches back over the records that fed it

`extract.Stream` holds a checkpoint back to the oldest record it is still
buffering: `HeldFrom` reports that floor and a driver that checkpoints byte
offsets may not acknowledge past it, because a record that produced no
sample yet is not a record that has been dealt with (§6.60). The floor is
computed from the *spans* of the open multiline buffers and aggregation
windows — the first and last position each one absorbed.

A window's span only ever grew forwards. `aggregate()` recorded
`a.lastMark = max(a.lastMark, st.mark)` and nothing else, on the assumption
that a record folded into a window is always at or past the position that
opened it. That assumption is false for exactly one shape, and it is an
ordinary one: a buffered multiline record is processed under the mark of
the line that *opened* it (`processBuffered`, §6.117) and is flushed by a
later line, so a record whose first line sits at offset 800 can be folded
into a window that a record at offset 1000 opened. The window's span then
began after bytes that are part of it.

`HeldFrom` reported that later position, the driver acknowledged it, and a
replay from there met the continuation lines with no buffer open — so the
window was rebuilt *without* that record's value. Nothing had delivered the
window yet, so there was no duplicate to collapse and no diagnostic to
read: the contribution was simply gone, and the number the store ended up
holding was one the source never reported. It needed a restart, a hole
rewind (§6.24) or an SSH reconnect to happen while the window was open,
which on a stream with multiline records and a per-minute `every` is a
matter of when rather than whether.

The span now grows in both directions: a record that reaches a window from
behind lowers `a.mark`, and `lowerHoldMark` repositions the window's entry
so the hold list stays in mark order — which is what lets `holdFloor` make
its one backward pass and `pruneHolds` reason about what can no longer
constrain a checkpoint. `Flush` and `FlushIdle` also drop a buffered record
from the map *after* it has been processed rather than before, which is
what `Process` already did: while the buffer is still there it pins the
floor at its own mark, and `pruneHolds` — which runs from inside
`process()` whenever a window opens — may only discard what is behind the
floor.

`TestHeldFloorSurvivesACrashAtEveryRecord` is the general form: randomised
streams of multiline, aggregating and plain records, crashed after every
one of them, checked so that every sample the uninterrupted run produced
appears either in what was delivered before the crash or in the replay from
the floor.

### 6.144 A shard that is already gone is not a failed drop

`DropShards` and `RunRetention` both walk a *snapshot* of the physical set
list and then drop what they found. They race each other by construction:
the background sweep drops the aged-out shards of the very set an operator
is deleting, `POST /v1/admin/retention/run` runs a second sweep beside the
timer, and two admin deletes of one set overlap. `engine.DropSet` answers
`ErrUnknownSet` for a shard that has gone in between, and both callers
returned it as a failure.

On the admin path that is a 500 for a deletion that succeeded: the loop
stopped at the first missing shard, so the rest of the set was left on
disk, and `ForgetSet` was never reached — the catalogue went on advertising
fields and a time range for data that really had gone, and only another
delete could clear it. On the sweep it abandoned the rest of the pass, so
every other shard past its horizon stayed until the next tick, under an
`ERROR retention sweep: engine: unknown set` line that describes a race
rather than a fault.

Both now skip a shard that is already gone and carry on. Removing what is
already removed is what both functions ask for, and it is the only reading
under which either is idempotent.

### 6.145 A dictionary write that fails is the store's fault, not the sample's

`rowFor` reports two very different things through one error: a sample the
store refuses — a label value outside the charset, a cardinality budget
already spent — and a label dictionary record the store could not persist,
which is a full disk or a transient I/O fault. `Write` turned every one of
them into a named rejection of that one sample and committed the rest.

For a bad sample that is exactly right, and it is why `ErrBadRequest`
exists in the other direction: a client-input fault has to be fatal on the
first attempt so the spec gets fixed, rather than retried forever. For a
failed `PutDict` it is the worst available answer. The ingester was told
the batch had landed minus a few rows, so the delivery observers saw a
successful flush, every followed file's checkpoint advanced past those
records, and the data was gone — with nothing anywhere but a `WARNING
store rejected 1 sample(s)` line on the ingester's own console, naming a
sample that was never the problem.

`intern` now tags that failure with `ErrStoreFault` and `Write` returns it
rather than rejecting the sample, so the request comes back 500.
`wire.Client` classifies that as retryable, the sink holds the batch it
still has, and nothing is acknowledged. The cardinality and label-key
refusals are untouched: those really are properties of what the client
sent.

### 6.146 A bucket-set edge that is not a number cannot be declared

`fields: limits:` has been checked for a non-finite bound since §6.123:
YAML reads `.nan` and `.inf` as real float64s, `encoding/json` refuses to
marshal either, and an unencodable declaration makes the *whole* write
request carrying it undeliverable — which the write client classifies as
fatal, so the sink drops that batch outright, reports it to the delivery
observers as a hole, and every followed file's checkpoint freezes behind
it.

`bucket_sets: edges:` is the other number a spec puts on the wire, and it
was not checked. `strconv.ParseFloat` reads `inf`, `+Inf` and `NaN`
without complaint, so `edges: 'explicit:[1,inf]'` compiled; the ascending
test could not catch a NaN, because every comparison against one is false;
and `linear:inf` produced `0 * +Inf`, which is a NaN in the first bucket.
A finite step is not enough either — `linear:1e308` overflows the moment
it is multiplied by the bucket position. Every edge reached
`wire.FieldMeta.BucketEdge` and, if a heatmap was ever drawn from it,
`heatmapSeriesName`, which named a series `+Inf`.

`parseEdge` now refuses a non-finite literal, and `BucketSet.compile`
re-checks every *computed* edge, which is where the overflow shows up.
`check --spec` reports it, which is the point: the failure it prevents
happens at run time, once, on a batch nobody is looking at.

### 6.147 A query's width is bounded, as its depth is

§6.116 bounded the *depth* of a predicate, because the parser, the
printer, the validator and the store's lowering are all recursive over one
shape and Go's stack overflow is not recoverable. The *width* of a query
was left open, and it is a per-row cost in the executor rather than a
per-request one: `runTimeseries` reads every selected field off every
scanned row, and `seriesKey` resolves and length-prefixes every `BY` slot
into that row's grouping key.

Neither is covered by the datasource ceilings, and the reason is worth
stating: a selected field the catalogue does not carry yields no datapoint,
so `max_datapoints_received` never fires, and opens no series, so
`max_series_per_graph` never fires either. Both gates watch a counter that
does not move. An AST arrives as JSON, so a body inside the store's
default 32 MiB `max_request_bytes` names on the order of a million fields —
a million map lookups per scanned row, on a query-scoped endpoint, with
`max_concurrent_jobs` of them allowed at once.

`MaxSelectFields` (1024) and `MaxByLabels` (64) are refused as `E007`.
Both are far past anything a dashboard emits: the widest table in the
documentation has a handful of columns, and a grouping deeper than a few
labels already has more series than `max_series_per_graph` allows.

### 6.148 The lines listener frames the way the other two do

`POST /ingest/v1/lines` reads records through `readRecord` (§6.111) but
judged the last one by `len(rec.Line) > 0` alone. A trailing record with no
newline really is a record when the body simply ended — nothing more is
coming for it — and that is the ordinary case this endpoint is used in.
It is not a record when the *read* failed: a client that died mid-request
leaves a fragment, and half a line handed to the extractor does not fail
cleanly, it matches a prefix-anchored pattern and invents a sample from a
number that was cut in two. `handleConn` refuses a cut-short TCP record for
exactly that reason and `serveUDP` refuses a truncated datagram for it;
this was the third acquisition path. The fragment was extracted and
delivered, and only *then* was the request answered 400 — so a dropped
connection wrote a sample the sender never sent, and the sender, told its
request had failed, sent the whole body again.

The same handler also counted a *delivery* failure as a refusal of the
record. `Sink.Add` buffers the sample and only then flushes, so its error
is the verdict of that flush and says nothing about the line in hand —
which `handleRecordOutcome`'s own comment already says. Reporting it as
`"refused": 1` under a 200, with a reason describing the store's health,
left a sender unable to tell a spec that does not match its lines from a
store that is down, for records the sink had in fact accepted. A delivery
failure is now `errDelivery` and answers 503 with `Retry-After`, which is
the signal the store itself uses for the same condition and the only one
`wire.Client` honours.

### 6.149 A LABELS filter is lowered once, not once per set

`queryLabelValues` walks every set carrying the label and lowered the
predicate again for each of them. `buildExpr` took a set name to do it
with — and never read it, because there is one label-value dictionary per
key for the whole store ([05](05-storage.md) §8, ADR-004). Every lowering
therefore produced the identical expression, the identical projection and
the identical diagnostics; §6.117 already deduplicated the *warnings*
that duplication produced, which is the same observation one level in.

What it cost is not cosmetic. A regex clause is evaluated once against
every value of the key — up to `max_label_cardinality`, 100 000 by
default — so a dashboard variable written as
`LABELS host WHERE dc =~ /^eu/` swept the whole dictionary once per set
carrying `host`, on the path a dashboard hits on every variable refresh.
That is the shape §6.117 removed from `shardsFor` in the same function,
arriving through the predicate instead of the shard list.

The lowering now happens once before the loop and the projection is built
with it, and `buildExpr` no longer takes a set: a parameter that looks
like it scopes the resolution, and does not, is what invited the call to
sit inside the loop in the first place.

### 6.150 Two progress-reporting corrections

- **`--print-interval 0` silences the console and nothing else.**
  [02](02-ingest.md) §10 describes the progress document as available
  three independent ways: a JSON file, the console, and samples in the
  `_mensura_ingest` set. `startReporting` returned early when the print
  interval was zero *and* no `--progress-file` was given, so silencing
  the console — the ordinary thing to do under a supervisor that already
  captures stderr — silently switched off the set that exists so ingest
  health can be plotted next to the data. The reporter now always runs,
  on the print interval or on a 30-second default, and only the printing
  is gated on the flag that names it.
- **The loss counters reach the ingest set.** `extract_errors`,
  `oversize_records` and `binary_skipped` were maintained, written to the
  progress file and left out of `Progress.Report`, so they were plottable
  nowhere. `oversize_records` in particular exists so that a truncated
  record is not indistinguishable from data that was never there, which
  is exactly what it is when the only place it appears is a file nobody
  has to ask for; §10 of [02](02-ingest.md) lists extraction errors among
  the fields the set carries.

### 6.151 The shard width is resolved once per batch, not once per sample

`shardName` asked `shardWidth` for the width of every sample it routed,
and `shardWidth` takes two read locks and calls `retentionFor`, which
takes a third. The width is a property of the *set*, and the set is fixed
for the whole of a `model.Batch`, so a default 1024-sample batch paid
three thousand lock acquisitions for an answer that could not change
inside it — on the hottest path in the system, with every other writer in
the process contending for the same two mutexes.

`Write` now resolves it once per batch, the way it already resolves the
key scheme once, and `shardNameAt` takes the width the caller has. It is
resolved after `applySetMeta`, so a `set_meta` declaration in the same
request still shapes the batch that travels with it ([04](04-wire-protocol.md) §3.3).

### 6.152 A declared limit whose bounds cross drew the raw counter

`resolveField` installs a field's declared limits as its *default clamp*,
with the counter-reset escape hatch wired up (`ClampElseRaw`). That is
what makes a declared range useful: a `DELTA` that goes negative because
the counter restarted is replaced by the raw sample rather than by the
bound.

With `limit_min` above `limit_max`, *every* value falls outside the range,
so every rendered point was replaced by its raw sample. A `RATE` query on
a counter declaring `min: 100, max: 1` therefore drew the raw counter —
1007, 1014, 1021 where the rate is 7 — with no error, no warning and a
perfectly plausible-looking line. Both the other surfaces that can express
the same pair already refuse it: MQL answers `E007` for
`CLAMP MIN 5 MAX 1` ([06](06-query.md) §12) and `extract.Compile` refuses
`limits: {min: 5, max: 1}` in a spec (§6.146's neighbour). The write API
was the one door left open, and it is the door a third-party writer or an
ingester built before that check comes through.

`applyFieldMeta`'s validating pass now refuses a crossed pair as an
`ErrBadRequest`, alongside the kind, the names and the bucket index it
already checks — so nothing new can be persisted. `resolveField`
additionally ignores a crossed pair it finds in the catalogue rather than
installing it, because a pair an earlier build already wrote is on disk
for good: not bounding a field the declaration meant to bound is the
smaller and far more visible error of the two.

The same pass refuses a negative `max_interval_ms` or `max_interval_s`.
Neither matched an arm of the merge switch, so the declaration was
accepted, stored as zero, and then reported by `W103` as a field with *no*
declared cadence at all — a declaration the store takes and does not act
on, which is the case every other check there exists to refuse.

### 6.153 A predicate no value can satisfy skips the shards, whatever its shape

`isAlwaysFalse` is what lets a query that cannot match anything read no
shards at all, rather than opening each one and walking it with a filter
that can never be true. It only recognised the equality arms, so
`host = "typo"` skipped the scan while `host =~ /typo/` — which the
lowering folds to exactly the same `engine.Const(false)` — read the whole
time range off disk to arrive at the same empty answer. It is also the arm
a dashboard variable lands on, where the range is the panel's and the sets
are every set carrying the label ([06](06-query.md) §5).

It now mirrors every arm of the lowering that folds to a constant: the
regex match, and a disjunction all of whose arms are impossible. A
*negated* match is deliberately absent — it lowers to an existence test,
not to a constant, so a key with no matching value still matches rows.

### 6.154 Two duplicate-clause gaps in MQL

[06](06-query.md) §12 lists a duplicate clause within one query under
`E006`, and two shapes fell through it.

`LIMIT SERIES 10, SERIES 5` parsed: the loop accepts either keyword on
every pass and the AST holds one value per gate, so whichever was written
last silently won. A safety limit is precisely the clause where "the one
you wrote second quietly took effect" is worth knowing. The parser now
refuses a second `SERIES` or `POINTS`.

`BY host, host` validated: a repeated grouping slot is not a wider
grouping, it is the same grouping rendered twice. `seriesKey`
length-prefixes the label's value once per slot and `seriesName` joins the
slots with `" : "`, so every series drew as `web1 : web1 : cpu`, while
costing a second dictionary lookup on every scanned row. `Validate` now
answers `E006`, which is what two selected fields sharing a display name
already get.

### 6.155 A bucket set could silently overwrite a pattern's captures

`expand()` writes the histogram into the same field map the named captures
were collected into, and it runs afterwards — so any column the bucket set
produces overwrites a capture of the same name, replacing the count the
source reported with a number derived from somewhere else. The row is well
formed and the heatmap draws, so nothing at run time says so.

Only the `tail` collision was refused ([03](03-extraction.md) §8), and it
is the least likely of the three: a bucket set naming its buckets
`fast`/`slow`, or deriving `<bucket>plus` columns from names like those,
collides with an ordinary capture far more readily than the literal word
`tail` does. `Profile.compile` now holds every column a bucket set writes
— the buckets, the cumulative columns and the tail — to the rule the tail
already had. `total_field` stays exempt: it is the one name a bucket set
*reads* rather than writes, and capturing it is the only way it is ever
populated.

### 6.138 Smaller corrections

- **`Print` has the nesting bound the rest of the package has.** The
  parser counts predicate depth in `parseUnary` and `Validate` counts it
  again on the AST, so the text and JSON forms stay the same language.
  `Print` could not: it returns a string, not an error. So it was the one
  entry point that walked whatever depth the JSON decoder allowed — ten
  thousand levels — rebuilding the whole sub-expression at every one of
  them, which turned a few hundred kilobytes posted to `/v1/print` into
  hundreds of megabytes of transient copying, on a query-scoped endpoint
  an editor calls freely. That is the hazard `MaxQueryBytes` and
  `MaxPredicateDepth` closed for `/v1/parse`, on the endpoint beside it.
  `mql.CheckPredicateDepth` is what the two print surfaces now gate on;
  it checks the predicate and nothing else, so a half-built query with no
  `SELECT` still prints.
- **An oversize sample post is too large, not malformed.**
  `POST /ingest/v1/samples` decoded through a `LimitReader` at
  `maxHTTPBodyBytes`, so a body that overran the cap was handed to
  `encoding/json` truncated and the sender was told "unexpected end of
  JSON input" — which sends it looking for a bug in what it encoded. The
  lines endpoint beside it already reads one byte past the cap and answers
  413 with "split it across requests" (§6.113); this one now does the same.
- **`--listen-query` exists.** The read-only surface was reachable from a
  config file and from nowhere else, while the other three listeners each
  had a flag — so the one listener an operator publishes to Grafana, the
  one that must not carry `/v1/write` or `/v1/admin/*`, could not be
  asked for without writing a config file for it.
- **A field's declared `limits:` are validated.** They become the clamp
  the query layer installs by default, and they were the one part of a
  `fields:` block nothing checked. A non-finite bound cannot travel —
  YAML reads `.nan` and `.inf` as real float64s and `encoding/json`
  refuses both, so the whole write request carrying the declaration is
  undeliverable and every followed file's checkpoint freezes behind the
  lost batch until the next flush thaws it. An inverted pair is quieter
  and lasts longer: MQL refuses `CLAMP MIN 5 MAX 1` as `E007`, while the
  same pair arriving from the catalogue installed a clamp whose two
  halves cancel, so a field declared a range and was not bounded by it.
- **`store_stream_label:` is gone from the worked example.**
  [03](03-extraction.md) §7 still showed it in a `patterns:` block while
  the compiler refuses it outright, so copying the documented example
  failed to compile.

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
- **The unsharded shard is never swept.** A set written before any
  retention was declared for it lands in `@all`, and `RunRetention` skips
  that shard by name because it carries no time range to compare against a
  horizon. In the ordinary case there is nothing there: `Write` applies a
  request's `set_meta` before its batches, so the first batch of the first
  ingester is already sharded, and §6.109 keeps the declaration across a
  restart. A second ingester declaring retention for a set an earlier one
  had already filled without it leaves those rows immortal; emptying them
  needs a row-wise sweep of `@all`, which is not built.
- **`NOT label = "v"` and `label != "v"` are not the same predicate.** The
  inequality requires the row to carry the label (§4.4 of
  [06](06-query.md)); the negation is a logical complement, so it also
  matches every row that does not. Both readings are defensible and the
  grammar offers both; the asymmetry is recorded here rather than resolved,
  because changing either one silently changes what a stored panel draws.
- **Documented CLI surface that does not exist.** `mensura-store
  convert-dashboard`, `mensura-store config check`, `mensura-ingest
  --label-from-path`, `--read-only-input`, and the whole ingest config
  file of [09](09-operations.md) §1.2 — `store:`, `state_dir:`, `spec:`,
  `labels:`, several `inputs:` entries, `progress:` — are described in
  [02](02-ingest.md), [06](06-query.md) and [09](09-operations.md) and
  are not built. `mensura-ingest` is configured by flags only, and the
  subcommands that exist cover the single-input case each of the missing
  ones is sugar for. What `config check` describes does happen: the store
  refuses an inline secret at startup and names the key
  ([config.go](../../cmd/mensura-store/config.go)); there is simply no
  subcommand of that name.
- **There is no environment-variable configuration layer.**
  [09](09-operations.md) §1 describes three sources in increasing
  precedence — config file, environment, flag — with variables following
  the field path (`MENSURA_STORE_DB_CACHE_BYTES`). Only the two bearer
  tokens are read from the environment (`MENSURA_STORE_TOKEN`,
  `MENSURA_INGEST_TOKEN`), which is also the one thing §1 says may *only*
  come from there. Everything else is config file or flag.
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
