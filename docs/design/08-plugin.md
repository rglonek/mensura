# 08 — Grafana plugin

## 1. What "native" means here

The cheap way to put a store behind Grafana is a generic JSON-over-HTTP bridge
datasource, where the query is a blob of JSON typed into a text area.
Everything Grafana can do for a datasource — typed query models, a builder UI,
variable support, annotations, alerting, the query inspector, provisioning —
then has to be faked or is simply absent.

Mensura ships a **native datasource plugin**: a signed plugin package with a
React frontend and a Go backend built on `grafana-plugin-sdk-go`, registered as
`mensura-datasource`. Grafana launches the backend over its plugin gRPC
protocol; the backend answers `QueryData`, `CheckHealth`, `CallResource` and
(where enabled) streaming.

```
plugin package (mensura-datasource/)
├── plugin.json          id, backend: true, alerting: true, metrics: true
├── module.js            frontend: QueryEditor, ConfigEditor, VariableEditor
├── img/…
└── gpx_mensura_<os>_<arch>   ← the mensura-store binary, plugin entrypoint
```

The backend executable in the package *is* `mensura-store`. Which role it plays
depends on the datasource's configuration:

| Datasource setting | Backend behaviour |
| --- | --- |
| `mode: embedded`, `dataDir: /var/lib/mensura` | Opens the engine, serves queries in-process, and binds the write API for ingest |
| `mode: proxy`, `storeURL: https://…` | No engine; forwards MQL over `/v1/query` |

Both are the same binary and the same query code behind a `QueryService`
interface. See [01-architecture.md §2](01-architecture.md) for the ownership
rules; the important one is that exactly one process may open the data
directory, and the plugin fails loudly rather than silently starting a second
store.

## 2. Backend

### 2.1 `QueryData`

```
for each Grafana query:
    model  = decode(query.JSON)          # the MQL AST, not text
    ast    = validate(model, catalogue)
    ast    = interpolate(ast, scopedVars)
    resp   = queryService.Query(ctx, ast, query.TimeRange,
                                query.MaxDataPoints, query.Interval)
    frames = toFrames(resp)
```

- `MaxDataPoints` and `Interval` come from Grafana and feed the window formula
  ([07-downsampling.md §2](07-downsampling.md)) untouched.
- The request context is propagated all the way into the engine iterator, so a
  panel navigation that cancels an in-flight query unwinds the scan instead of
  finishing it. This is one of the reasons panel spam does not melt the store.
- Partial results are returned as frames *plus* `response.Error`, so Grafana
  shows the red banner and still draws what was collected
  ([04-wire-protocol.md §9](04-wire-protocol.md)).
- Warnings become frame notices, visible in the panel without being errors.

### 2.2 `CheckHealth`

Reports store reachability, mode, catalogue version, set count, the observed
data time range, and — in embedded mode — the engine's disk usage and open
iterator count. A health check that merely says "OK" is a wasted round trip;
this one answers "is there data, and from when to when".

### 2.3 `CallResource`

The builder's data source. All of it is catalogue and dictionary reads, cached
in the backend with `ETag`/`catalogue_version` revalidation:

| Resource | Returns |
| --- | --- |
| `GET /sets` | Set names, row counts, time coverage |
| `GET /fields?set=` | Fields with kind, unit, description, `max_interval`, limits, bucket-set membership, staleness |
| `GET /labels?set=` | Label keys present on the set |
| `GET /label-values?key=` | Dictionary values |
| `POST /parse` | MQL text → AST, or parse errors with positions |
| `POST /print` | AST → canonical MQL text |
| `POST /explain` | Plan summary: shards, pushdown projection, resolved fields, downsample window (embedded mode only) |

`/explain` takes the same body as a query — a `wire.QueryRequest` carrying
the AST and the panel's range — and answers with what `Store.Explain`
produces. A proxy-mode datasource has no local engine to ask and the
store's own plan endpoint lives on its loopback-only debug listener
(`--listen-debug`), which by construction no proxy can reach, so it
answers with that rather than inventing a plan on this side.

The `filter=` parameter `/label-values` used to be described with is not
built; it is named in [12](12-implementation.md) §7. `LABELS <key> WHERE …` on
the query API is the filtered form, and it needs a time range that a
resource call does not carry.

`/parse` and `/print` living in the backend is what keeps the two
representations honest: there is one parser, in Go, and the frontend never
implements a second one.

### 2.4 Streaming (follow mode)

For live tail panels, the backend implements `SubscribeStream` /
`RunStream` on channel `ds/<uid>/<queryHash>`: it re-runs the query on the
store's write-notification signal (debounced to `min_stream_interval`, default
1 s) and pushes only the newly-closed windows. Off by default; enabled per
panel. Deferred to milestone M6, but the channel naming is fixed now so panels
do not have to change later.

## 3. Frontend query editor

The editor has two modes with a toggle, exactly like Grafana's own Prometheus
and Loki editors, and — the point of §1 of [06-query.md](06-query.md) — the
toggle is never disabled. The backend and its `CallResource` endpoints are
implemented; the React editor is not yet built
([12-implementation.md §5](12-implementation.md)).

### 3.1 Builder mode

```
┌ Query A ─────────────────────────────────────────── [Builder | Code] ┐
│ From      [ http ▾ ]                    Format [ Time series ▾ ]     │
│                                                                      │
│ Select    [ requests_total ▾ ]  counter · reqs                       │
│           ⟨ Rate ✓ ⟩ ⟨ Negate ⟩ ⟨ Gap 30s ⟩ ⟨ Clamp… ⟩ ⟨ SSE 0 ⟩     │
│           as [ req/s            ]                            [ − ]   │
│           [ + field ]                                                │
│                                                                      │
│ Where     [ host ▾ ] [ in ▾ ] [ $host          ▾ ]           [ − ]   │
│           [ dc   ▾ ] [ =  ▾ ] [ eu-west-1      ▾ ]           [ − ]   │
│           [ + filter ]   ( ⊕ and | or, group )                       │
│                                                                      │
│ Group by  [ host ×] [ + ]              Every [ auto ]                │
│                                                                      │
│ ⓘ requests_total is a counter; Rate is applied.       [ Explain ]    │
└──────────────────────────────────────────────────────────────────────┘
```

Behaviours worth designing for explicitly:

- **Field metadata drives defaults.** Picking a `counter` field pre-enables
  `Rate`; a field with `max_interval` pre-fills `Gap`; a field with declared
  limits pre-fills `Clamp … else raw`. Each is a visible, editable control —
  never an invisible transform.
- **Every control names its consequence.** The `Gap` tooltip says "draw a break
  when samples are more than this far apart", not "MaxIntervalSeconds".
- **Modifier order is not exposed**, because it is not configurable
  ([06-query.md §4.3](06-query.md)). The builder shows the canonical order in a
  hint next to the modifier row.
- **Label values are typeahead-loaded** from `/label-values`, scoped by the
  filters already chosen, so picking a `dc` narrows the `host` list.
- **Explain** shows the plan (shards touched, index range, pushdown expression,
  projected columns) before running an expensive query.

### 3.2 Code mode

A Monaco editor with an MQL language definition: syntax highlighting,
completion from the catalogue (sets → fields → labels → label values),
signature help for modifiers, inline diagnostics from `POST /parse`, and
format-on-save via `POST /print`.

Switching Code → Builder parses; if it fails, the toggle shows the parse error
at its position and stays in Code mode. Switching Builder → Code prints the
canonical text. Because the panel stores the AST, neither direction can lose
information.

## 4. Variables

`FIELDS`, `LABELS`, `LABEL KEYS` and `SETS` ([06-query.md §5](06-query.md))
back Grafana's variable queries, with a small builder of their own so a
variable can be defined without typing MQL.

- Multi-value variables interpolate to `IN (…)`.
- `All` removes the comparison rather than expanding to every value — cheaper
  and semantically identical given §4.4's strict matching.
- A `NONE` sentinel value may be added per label (a datasource setting, carried
  `add_none_to_labels`); selecting it short-circuits the query to an empty
  result, which is how a dashboard offers "draw nothing" as an explicit
  choice.
- Chained variables work because `/label-values` accepts the already-chosen
  filters.

## 5. Annotations

An annotation query is an MQL query with `FORMAT logs` or a timeseries query
with a threshold, plus a mapping from fields to annotation text and tags. The
natural source is the event sets produced by aggregating patterns
([03-extraction.md §9](03-extraction.md)): "restart", "cluster state change",
"error class first seen". Annotation queries are subject to the same
`LIMIT POINTS` gate as logs queries.

## 6. Alerting

`plugin.json` declares `alerting: true`. Grafana's unified alerting calls
`QueryData` with a rule-supplied time range and `MaxDataPoints`, and expects
frames it can reduce.

The honest caveat, documented in the plugin's help and in the query editor when
a query is used in a rule: the render pipeline is designed for *rendering*, and
its min/max-per-window output is not a statistical summary. An alert rule that
reduces such a series with `avg` is averaging extremes. Therefore:

- Rules **should** set `EVERY` explicitly rather than inheriting a pixel-derived
  window;
- the editor warns when a query with no `EVERY` is used in a rule;
- `SSE` is forced `OFF` for alerting queries, because synthetic padding points
  must never contribute to a rule evaluation. This is enforced in the backend by
  inspecting the request's `Headers["FromAlert"]`, not left to the author.

## 7. Provisioning

The plugin ships:

- a datasource provisioning example (`provisioning/datasources/mensura.yaml`);
- a small dashboard library: an ingest-health dashboard (built on the
  `_mensura_ingest` set), a store-health dashboard (built on `/v1/stats`
  exported as metrics), and a "generic explorer" dashboard whose panels are
  driven entirely by variables, so a fresh deployment has something to look at
  before anyone writes a dashboard;
- an optional `mensura-store dashboards install` command that writes those
  files into Grafana's provisioning directory. This is a one-shot convenience,
  not a reconciliation daemon: Mensura does not manage the Grafana install.

## 8. Packaging and signing

- Built for `linux/amd64`, `linux/arm64`, `darwin/arm64`, `windows/amd64`; the
  backend binary is the same one operators run standalone.
- Signed with a Grafana plugin signature for public distribution; unsigned local
  installs require `allow_loading_unsigned_plugins`, which the install docs
  spell out rather than leaving to a stack overflow answer.
- The plugin version, the store version and the wire protocol version are
  reported together by `CheckHealth`; a proxy-mode plugin talking to an
  incompatible store fails health rather than producing confusing query errors.
