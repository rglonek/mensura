# 10 — Design decisions

Each entry: the decision, the alternatives considered, and the cost accepted.
Status is `accepted` unless stated otherwise.

---

## ADR-001 — Ingest is a separate binary and always writes over the network API

**Decision.** `mensura-ingest` never opens the database. Every mode — one-shot
batch, follow, remote follow, receive — delivers samples through the store's
`/v1/write` API.

**Alternatives.** (a) Ingest linked into the same process as the plugin,
writing to the LSM directly. (b) A hybrid: direct writes when local, network
writes when remote.

**Why.** Follow, remote and receive ingest are all inherently remote; the moment
one mode is remote, the network path must exist and be correct. Keeping a second
in-process path would mean two sets of semantics for ordering, idempotency,
backpressure, auth and error handling — and the direct path would be the
better-tested one, so the network path would rot exactly where it matters.

**Cost.** One-shot import of a local bundle pays serialisation, compression and
an HTTP round trip that a direct writer would not. Mitigated by batching (1024
samples or 4 MiB per request), zstd-1, HTTP/2 keep-alive, and loopback with
compression off. The benchmark gate in [11-roadmap.md](11-roadmap.md) pins the
acceptable overhead to a measured number rather than a hope.

---

## ADR-002 — Plugin and store are the same binary; exactly one process owns the data

**Decision.** The Grafana backend datasource executable *is* `mensura-store`.
In embedded mode it opens the engine; in proxy mode it forwards to a store that
does.

**Alternatives.** (a) A separate thin plugin binary always proxying to a store.
(b) Always embedded, no standalone server.

**Why.** Embedded gives the whitepaper's latency story (a panel query is a range
seek, not a network hop) and matches "storage and plugin are Grafana
neighbours". Proxy mode is required when the store must outlive Grafana or serve
several Grafanas. One binary means one query implementation, and the interface
seam (`QueryService`) is small.

**Cost.** An operator can misconfigure both at once; the Pebble lock makes that a
loud startup failure naming the holder, not corruption.

---

## ADR-003 — Sparse-column LSM engine, with time-sharded sets for retention

**Decision.** A Pebble-backed sparse-column engine with a covering time index.
Shard sets by time window; retention drops whole shards with a range delete.

**Alternatives.** (a) Per-row TTL with point tombstones. (b) Multiple Pebble
instances, one per day. (c) No retention at all.

**Why.** Point tombstones put compaction debt precisely where the read path
lives. Multiple instances multiply file handles, caches and open-iterator
accounting. No retention is fine for triage and impossible for continuous
collection.

**Cost.** Cross-shard queries open N iterators and merge N streams; too fine a
shard width wastes LSM overhead on low-rate sets. Default 24 h with a per-set
override.

---

## ADR-004 — The store owns the label dictionary; the wire is strings

**Decision.** Ingest sends label strings; the store interns them into integer
indices.

**Alternatives.** (a) Ingest assigns indices, which is only safe when ingest
and store are one process. (b) A negotiated dictionary with client-side caching
and delta updates.

**Why.** With many independent ingesters, a single writer is the only way to keep
the dictionary consistent without a coordination protocol. Strings on the wire
keep clients stateless, so a retried, replayed or duplicated batch is always
safe.

**Cost.** Label maps repeat per sample on the wire. zstd compresses this
extremely well; the dictionary-delta optimisation is designed
([11-roadmap.md](11-roadmap.md) M7) but not built until profiles justify it.

---

## ADR-005 — Content-addressed primary keys by default, byte-offset keys opt-in

**Decision.** `xxh3-128(set ‖ ts ‖ labels ‖ fields)` by default; an `offset`
scheme keyed on `(stream, byte offset)` per set.

**Alternatives.** (a) Always offset-based. (b) Monotonic sequence numbers.

**Why.** Content addressing makes at-least-once delivery behave as exactly-once
for the common case: a replayed line overwrites itself, which is what makes
crash recovery and retries boring.

**Cost.** Two identical records in the same millisecond from the same stream
collapse into one row. Real for occurrence-counting patterns, hence the opt-out,
the aggregation alternative, and a `check` warning when a pattern has no numeric
capture under content keying.

---

## ADR-006 — Modifiers are declarative; execution order is fixed

**Decision.** MQL has no composable transform algebra. `DELTA`, `NEGATE`,
`CLAMP`, `PER SECOND`, `GAP`, `SSE` set flags, applied in the canonical order
from the whitepaper.

**Alternatives.** (a) PromQL-style nested functions. (b) A pipeline syntax where
stage order is meaningful.

**Why.** The stage ordering is the correctness contract: each stage relies on
invariants established by earlier ones (raw timestamp for gap detection, raw
value for the clamp fallback, raw inter-sample interval for rate). Making it
configurable moves that complexity to every dashboard author, where it would be
exercised inconsistently and wrongly.

**Cost.** Some transformations are not expressible. Composable expressions
(`a/b`, `a-b`) remain out of scope; where they are needed, compute them at
ingest.

---

## ADR-007 — Strict label matching; unknown values do not silently widen a query

**Decision.** `label = "v"` does not match rows lacking the label, and a value
absent from the dictionary yields an empty result with a warning.

**Alternatives.** The permissive defaults common in this space: a filter
matches rows missing the column, and a clause naming an unknown value is
dropped.

**Why.** Both behaviours were there to keep dashboards rendering during
ingestion, and both make the graph mean something other than what it says.
Rendering something wrong during an incident is worse than rendering nothing
with an explanation.

**Cost.** Dashboards ported from a permissive datasource may need
`OR MISSING label` in a few places. The converter emits it automatically.

---

## ADR-008 — No cross-series arithmetic at render time; no recording rules

**Decision.** No `a/b` in MQL, no recording-rule engine.

**Why.** Windows are anchored per series (which is what buys zoom stability), so
two series do not share a time grid and arithmetic between them would require
re-interpolation — the exact falsification the pipeline exists to prevent.
Derived values belong at ingest, where the inputs are on one record.

**Cost.** Ratios (error rate as a fraction of requests) must be extracted or
computed at ingest, or done in a Grafana transformation with the operator's eyes
open. Documented, not hidden.

---

## ADR-009 — One frame per series, never a wide frame

**Decision.** `QueryData` returns one data frame per series.

**Why.** Wide frames require a shared time axis. Per-series window anchoring
means each series has its own timestamps; merging would demand alignment, which
means interpolation.

**Cost.** More frames per response and slightly higher per-frame overhead in
Grafana. Irrelevant next to the scan.

---

## ADR-010 — Field metadata flows from spec to builder

**Decision.** `kind`, `unit`, `max_interval` and `limits` are declared in the
extraction spec, stored in the catalogue, and used as *defaults* in the query
builder.

**Alternatives.** (a) No metadata: every per-field flag typed per panel.
(b) Metadata applied automatically at query time.

**Why.** The most common wrong graphs — a counter plotted raw, a gap drawn as a
continuous line — are wrong because the panel author did not know a fact the
spec author did. Carrying the fact forward fixes the default. Applying it
*invisibly* would violate "no hidden behaviour", so it becomes a pre-filled,
visible control instead.

**Cost.** Metadata can be wrong or contradictory across ingesters; conflicts are
recorded and surfaced rather than resolved silently.

---

## ADR-011 — Single-process store; no clustering or replication

**Decision.** One process, one directory, no HA.

**Why.** The workload is one host's worth of operational data with a
re-ingestible or continuously-arriving source. Clustering an LSM correctly is a
project in itself and would compromise the properties that make this engine fast
(no coordination, no network hop, WAL-optional).

**Cost.** No failover. The escape hatch is fan-out at the ingest client: a write
client may address several stores, and the same batch (same idempotency key,
same content-addressed keys) can be sent to both, giving two independent,
eventually identical stores at the cost of double writes. Designed, listed under
M7, not built.

---

## ADR-012 — Receive is an ingest-side concern, not a store-side one

**Decision.** TCP/UDP/HTTP receive listeners live in `mensura-ingest`; the store
accepts only fully-formed samples.

**Alternatives.** Listeners in the store, so a sender can write to it directly.

**Why.** Received traffic needs the same treatment as tailed lines — extraction,
labelling, aggregation, backpressure. Putting the listeners where the extraction
engine already lives keeps one code path and one place to reason about untrusted
input. It also keeps the store's attack surface to two authenticated APIs.

**Cost.** A sender that already emits perfect samples still goes through an
ingest hop. It may instead be given a `write` credential and talk `/v1/write`
directly — that path is open, just not the documented default.

---

## ADR-013 — Aho-Corasick prefilter with first-match-wins

**Decision.** One Aho-Corasick automaton over every pattern's `search` literal;
the lowest-index matching pattern wins.

**Why.** It turns per-line cost from N substring scans into one `O(len(line))`
pass, and preserves the linear scan's semantics exactly, so pattern files behave
the same either way. First-match-wins also makes pattern order meaningful and
auditable (and `check` reports shadowed patterns).

**Cost.** A pattern without a `search` literal is always evaluated and is slow;
`check` warns.

---

## ADR-014 — Whitepaper §3 is normative and reproduced, not re-derived

**Decision.** The render walk is implemented exactly as specified, including the
details that look like quirks: the strict `>` boundary, the duplicate-drop
ordering, the three-slot null classification, ±500 ms SSE padding, and SSE
values derived from the window's min point.

**Why.** These are load-bearing for the correctness properties C1–C8, and each
has an operational failure mode behind it. "Improving" one in isolation breaks a
proof.

**Cost.** Some behaviour is surprising until read alongside the paper — hence
[07-downsampling.md](07-downsampling.md), which restates the reasoning next to
the code contract.

---

## ADR-015 — No full-text search

**Decision.** Mensura extracts structured samples. It does not index log text.

**Why.** Indexing tens of gigabytes for free-text retrieval is minutes-to-hours
of work that the triage budget does not have, and it optimises for a question
(`grep`) that the original files already answer.

**Cost.** No "show me the lines behind this spike" from inside Grafana beyond
what `FORMAT logs` returns from extracted string fields. A future
`raw_line: true` per-pattern option (store the matched line as a string column,
opt-in, expensive) is the escape hatch — designed, deferred, and off by default.
