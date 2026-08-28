# 11 — Roadmap, testing and open questions

## 1. Milestones

Each milestone is shippable and demonstrable on its own; nothing here needs the
next milestone to be useful.

### M1 — Vertical slice

*Goal: one log file on disk becomes one Grafana panel.*

- `pkg/model`, `pkg/wire` (NDJSON only), `internal/engine` (single set, no
  sharding), `pkg/extract` (line framing, one timestamp format, regex patterns),
  `pkg/render` (the full ten-stage walk — it is small and it is the point),
  minimal plugin backend with a code-only query editor.
- `mensura-ingest batch --source file` → `mensura-store --mode=server` →
  Grafana panel.
- **Exit criteria**: C1–C8 property tests pass; a 1 GiB log file imports and
  renders.

### M2 — The real engine and the real protocol

- Time-sharded sets, retention, catalogue, label dictionary with cardinality
  limits, storage profiles and all tunings.
- Protobuf wire, zstd, idempotency keys, auth (bearer + mTLS), rate limits,
  backpressure, partial-result semantics.
- Store metrics and `/v1/stats`.
- **Exit criteria**: ingest throughput within a documented factor of a direct
  in-process write (see §3); a retention sweep drops a shard and reclaims disk.

### M3 — MQL and the builder

- `pkg/mql`: lexer, parser, AST, printer, validator, JSON Schema, lints.
- `CallResource` catalogue endpoints; the visual builder; Monaco code mode with
  completion; `Explain`.
- Flag-payload dashboard converter ([06-query.md §10](06-query.md)).
- **Exit criteria**: `parse(print(ast)) == ast` fuzz test green; a dashboard
  built entirely in the builder with no text editing.

### M4 — Follow and remote

- Checkpoints, rotation handling (all five events in
  [02-ingest.md §6.1](02-ingest.md)), archive catch-up, spec reload.
- SSH follow with reconnect and probe-based rotation detection.
- Ingest metrics and progress panels.
- **Exit criteria**: the rotation test harness (§2.3) passes with zero gaps and
  zero unexpected duplicates across all rotation styles, including kill -9 at
  each stage.

### M5 — Receive, histograms, aggregation

- TCP/UDP/HTTP receivers with backpressure and drop accounting.
- Bucket sets, cumulative generation, `HISTOGRAM()`, `FORMAT heatmap`,
  percentile estimation.
- Window aggregation at ingest.
- `AGGREGATE sum|avg|max|min BY …` in MQL (the honest fix for the no-`BY`
  interleave caveat in [06-query.md §8](06-query.md)).

### M6 — Production polish

- Alerting support with the `EVERY`/`SSE OFF` rules; annotations; streaming for
  live tail.
- Dashboard library, provisioning helper, signed plugin packages.
- Runbook validation: every situation in [09-operations.md §6](09-operations.md)
  reproduced in a test environment.

### M7 — Deferred, designed but unbuilt

Kept in the design so the interfaces do not preclude them:

- **Dictionary-delta wire mode** — clients cache label→index maps and send
  indices; the store returns deltas. Build only if profiles show label repetition
  is material after compression (ADR-004).
- **Ingest fan-out to multiple stores** — the poor-man's HA escape hatch
  (ADR-011).
- **Downsampled rollup shards** — pre-aggregated coarse shards for very long
  ranges. Only if scans over multi-month ranges become the bottleneck; note that
  it reintroduces the tier boundaries the whitepaper argues against, so it would
  be opt-in per set and visibly labelled in the panel.
- **`raw_line: true`** — optional storage of matched log lines for a
  "show me the lines" drill-down (ADR-015).

## 2. Testing strategy

### 2.1 Render pipeline

The correctness properties are the acceptance criteria, not an afterthought:

- **Property tests** over randomly generated series (random gaps, duplicates,
  plateaus, resets, single points, empty series) asserting C1–C8.
- **Differential tests** against a deliberately naive reference implementation
  (brute-force per-window min/max plus explicit null placement) that is slow,
  obviously correct, and never optimised.
- **Golden files** over recorded real series, byte-compared, to catch
  accidental behaviour drift.
- **Degenerate inputs**: `w == 0`, one sample, two samples one millisecond
  apart, all-duplicate timestamps, everything clamped, `SSE OFF` everywhere.

### 2.2 Extraction

- Fixture-driven: each spec ships sample input lines and expected samples;
  `mensura-ingest check --sample` is the same code path CI uses.
- Fuzzing of the framer and the timestamp scanner (never panic, never hang,
  never emit a timestamp the line does not contain).
- Regex safety: compile-time size limits, and a nested-quantifier lint.

### 2.3 Rotation harness

A dedicated test that drives a real filesystem through each rotation style
(rename+create, create+delete, truncate, copytruncate, compressed archive) while
a writer appends known, numbered lines, then asserts on the store:

- every line number appears at least once (no gaps);
- under `key: content`, every line number appears exactly once (no duplicates);
- the assertions hold when the ingest process is killed at each pipeline stage
  and restarted.

The same harness runs over SSH against a container to cover remote follow, with
its weaker guarantees asserted as *documented* rather than as *equal* to local.

### 2.4 Engine

Codec round-trip, index semantics, concurrency, iterator close/leak detection,
crash-and-reopen, storage-version mismatch. Plus:
shard-routing correctness at boundaries (a sample exactly on a shard edge lands
in exactly one shard), retention deletion completeness (no `I/` entry survives
its `D/` pointer), and cross-shard query merge.

### 2.5 Protocol

- Contract tests generated from the protobuf schema, run against both the real
  server and a mock.
- Idempotency: the same batch sent twice, concurrently, in either order.
- Fault injection: connection resets mid-body, slow readers, `429`/`503` storms,
  clock skew between client and server.
- Fuzzing of the receive line protocol.

### 2.6 End-to-end and performance

- A docker-compose environment: store + ingest + Grafana + a synthetic log
  generator, with a scripted panel query asserted against expected series.
- Benchmarks with published numbers and CI gates:
  | Benchmark | Gate |
  | --- | --- |
  | Ingest throughput, local batch | within 1.5× of a direct in-process write baseline |
  | Query latency, 10 M rows, 100 series, 1 day | p95 under 500 ms warm |
  | Render walk | ≥ 20 M samples/s/core |
  | Write API round trip, loopback, 1024-sample batch | p95 under 5 ms |

  The ingest gate exists specifically to keep ADR-001 honest: if the network
  path costs more than 1.5×, the decision gets revisited with data.

## 3. Open questions

These need a decision before the milestone that depends on them; none block M1.

1. **Sub-millisecond timestamps** (M2). Storage is millisecond; some sources emit
   microseconds. Options: keep truncating (simple, documented), or store an
   optional `ts_us_frac` column used only for ordering within a millisecond.
   Leaning: truncate, because the render contract is millisecond and the
   duplicate-drop rule already defines the collision behaviour.
2. **Out-of-order arrival beyond the retention edge** (M2). A sample older than
   the oldest live shard is currently rejected. Alternative: resurrect the shard.
   Leaning: reject, count, and expose the counter — resurrection makes retention
   unpredictable.
3. **Set explosion from `route:`** (M3). A spec that routes on a captured value
   could create unbounded sets. Leaning: a `max_sets` limit with a loud error,
   mirroring the label-cardinality guard.
4. **Multi-tenancy** (post-M6). Currently one store, one tenant. If needed, the
   cheap form is one store process per tenant; the expensive form is a tenant
   label enforced on every write and query. No demand yet, so no design yet.
5. **Percentile accuracy reporting** (M5). Bucket-interpolated percentiles carry
   an error bound; the design says report it in frame metadata. Open: whether to
   also render it (a band) or only expose it in the inspector.
