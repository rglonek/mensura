# 03 — Extraction spec

## 1. What a spec is

An **extraction spec** is a YAML document that tells `mensura-ingest` how to
turn text into samples. It is data, not code: the binary contains no knowledge
of any product's log format: the format lives in the spec, and the spec is
supplied by the operator.

One spec file may be shared by many inputs. Specs compose: `include:` pulls in
other files, so a deployment can keep one `base.yaml` plus one file per log
format.

A spec is validated by `mensura-ingest check --spec F [--sample FILE]`, which
compiles every regex, checks that captures resolve, detects unreachable
patterns (a `search` literal shadowed by an earlier pattern), and — with
`--sample` — reports match rates and the first unmatched lines per profile.
`--label k=v` supplies the operator labels the import would carry, because
profile selection reads them: a profile chosen by `select.label_equals`
matches nothing without them. The sample is opened and framed exactly as
the import opens and frames it — single-file gzip and bzip2 are
decompressed, archives and binary content are declined by name, and
`--max-record-bytes` truncates where the import truncates — because a tool
that predicts an import must not read differently from one.

## 2. Document structure

```yaml
version: 1                  # spec format version, required

defaults:                   # optional, applies to every profile
  timestamp:
    timezone: UTC           # used when the format carries no zone
    assume_year: file-mtime # for formats without a year (syslog): file-mtime | now | <int>
  labels: [host, source]    # stream labels always attached

identity:                   # how to discover stream labels from content/path
  - match_path: '(?P<host>[^/]+)/(?P<source>[^/]+)\.log$'
  - scan_lines: 500
    regex: 'node-id (?P<node>[0-9a-f]+) .*service (?P<service>[a-z]+)'

profiles:                   # a profile = one log format
  - name: nginx-access
    select:                 # when does this profile apply to a stream?
      path_glob: ['*/access.log', '*/access.log.*']
      content_contains: ['HTTP/1.']
    timestamp: …            # §3
    framing: …              # §4
    labels: […]             # §5 — which captures are labels, not fields
    fields: …               # §6 — field metadata (kind, unit, defaults)
    patterns: […]           # §7 — the extraction rules
    bucket_sets: […]        # §8 — histogram/heatmap bucket declarations

sets:                       # optional per-set overrides (retention, etc.)
  http:
    retention: 30d
```

### 2.1 Profile selection

A stream is bound to exactly one profile at open time, by the first `select:`
that matches. Selection may use `path_glob`, `content_contains` (a literal
search over the first `scan_bytes`, default 64 KiB), `label_equals`, or
`listener` (for `receive` inputs). A stream that matches no profile is either
ignored or routed to `default_profile:`, and the decision is counted and
logged — silently dropping unmatched files is the failure mode that wastes an
afternoon.

Dialect (which profile parses this stream) and identity (which labels the
stream carries) are separate concepts, and conflating them — using the identity
label to select the parser — is the mistake this two-key design exists to
prevent.

## 3. Timestamps

```yaml
timestamp:
  formats:
    - layout: 'Jan 02 2006 15:04:05.000 '     # Go reference layout
      regex:  '[A-Z][a-z]{2} \d{2} \d{4} \d{2}:\d{2}:\d{2}\.\d{3} '
    - layout: '2006-01-02T15:04:05Z07:00'
      regex:  '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})'
    - layout: epoch_ms                        # or epoch_s, epoch_us, epoch_ns
      regex:  '^\d{13}'
  anchor: prefix                              # prefix | anywhere
  strip: true                                 # remove the matched timestamp from the line
  on_parse_error: count                       # count (the only value implemented)
```

`drop-stream` and `fail` are **not implemented**, and the compiler refuses
them rather than accepting the declaration and counting anyway: an
operator must not be able to believe a stream is being dropped when it is
not. See [12](12-implementation.md) section 6.27.

Behaviour:

- Formats are tried in order **once**; the index of the first format that
  matched is cached per stream and tried first on every subsequent line, so
  the steady state is one regex match. A miss falls back to the full scan and
  re-caches.
- `anchor: prefix` skips the regex entirely once the format is known, slicing a
  fixed prefix — the common fast path.
- Records without a parseable timestamp are counted per stream and reported;
  they are never assigned "now", because a wrong timestamp is worse than a
  dropped sample on an operational dashboard.
- Millisecond resolution is the storage resolution. Sub-millisecond precision
  in the source is truncated, not rounded, and this is stated in the field
  catalogue.
- Year-less formats (classic syslog) resolve the year per `assume_year`, with
  a December→January rollover rule: if applying the assumed year produces a
  timestamp more than 24 h in the future relative to the previous record, the
  year is decremented.

## 4. Framing

```yaml
framing:
  record: line                 # line (the only value implemented; see below)
  max_record_bytes: 1048576    # oversize records are truncated + counted
  multiline:
    - start_contains: 'histogram dump'
      continue_regex: '\(hist\.c:\d+\)[ ]+\('
      join:
        - regex: '( |\(hist\.c:\d+\))( \(.*)'
          capture: 2
      idle_timeout: 30s        # follow mode: flush a stuck partial record
```

Multiline semantics: a line containing
`start_contains` opens (or replaces) a buffered record; subsequent lines
matching `continue_regex` have the nominated capture group appended; a new
start line, a timestamp regression, stream close, or `idle_timeout` flushes it.

Both numbers are validated at compile time, because both fail *open* rather
than closed. `max_record_bytes` is read everywhere as `n > 0`, so a negative
value removes the bound instead of setting one; an `idle_timeout` that is
negative or zero is never *not* elapsed, so every buffered record is flushed on
the next tick and the rule joins nothing. A declaration that quietly does the
opposite of what it says is refused, the way `identity.scan_lines` and a
negative `sets:` retention are.
Timestamp regression inside a multiline record is an error for that record,
not a silent join.

Both keys are required, and the compiler refuses a rule missing either.
`continue_regex` is the only test applied to a candidate continuation, so
a rule without one joins nothing while still opening a buffer on every
start marker: the record that opened it waits for the next start marker
or the idle timeout, and in follow mode the checkpoint waits with it. An
empty `start_contains` is the mirror image — it matches every line, so
every record opens a buffer and none is ever joined.

`record: json` would decode each line as a JSON object, with captures
addressed by JSON pointer (`/http/status`) rather than by regex group. It
is **not implemented**: every record is framed by line, and the compiler
refuses `record: json` rather than silently applying line framing to a
spec that asked for something else. Multiline framing is configured
through `multiline:` above, not through `record:`.

## 5. Labels vs fields

The single most important classification in the spec.

- A **label** is a low-cardinality string that identifies a series
  (`host`, `namespace`, `device`, `queue`). Labels are dictionary-interned by
  the store and are what you can filter and group by.
- A **field** is a numeric measurement (`requests_total`, `latency_p99`) or,
  for table queries, a string payload. Fields are the columns you plot.

```yaml
labels: [namespace, device, queue, error_class]   # profile-level
```

Any named capture whose name appears in the profile's `labels:` list (or in a
pattern's own `labels:`) becomes a label; every other named capture becomes a
field. One list, one rule, and it is visible in the spec rather than inferred
from a value's shape.

Cardinality guard: the store rejects a label whose distinct-value count exceeds
`max_label_cardinality` (default 100 000 per label), returning `400` with the
label named. Unbounded labels (request IDs, URLs with parameters) are the
classic way to destroy a metrics system; the failure is loud and early.

## 6. Field metadata

New in Mensura, and the reason the query builder can be helpful:

```yaml
fields:
  requests_total:
    kind: counter          # counter | gauge | delta | string
    unit: reqs             # free text; Grafana unit hint via unit_hint
    unit_hint: short
    max_interval: 30s      # declared ticker cadence → gap/null injection default
    description: 'total requests served since worker start'
  latency_p99:
    kind: gauge
    unit: ms
    unit_hint: ms
  cpu_pct:
    kind: gauge
    unit: percent
    unit_hint: percentunit
    limits: {min: 0, max: 100}
```

Field metadata travels with the samples (once per field per set, not per
sample — see [04-wire-protocol.md §5](04-wire-protocol.md)), is stored in the
store's catalogue, and is served to the plugin. Effects:

- `kind: counter` makes the builder pre-select `RATE`, and makes MQL *warn*
  (not error) when a counter is plotted raw.
- `max_interval` becomes the default `GAP` for that field, so gap detection is
  right by default instead of being a per-panel chore.
- `limits` become the default `CLAMP … ELSE RAW`, which is the counter-reset
  escape hatch pre-wired.
- `unit_hint` populates the frame's field config so panels get units without
  manual configuration.

`kind` is one of the four spellings above and nothing else: an unrecognised
one is refused when the spec compiles, and by the write API if it arrives
from anywhere else. Everything downstream compares it against those four
and ignores what it does not know, so a typo would leave the field looking
declared while behaving as though it carried no kind at all — no `RATE`
pre-selection, no `W102`, no string column. Declared label keys — in
`defaults.labels`, in a profile's `labels:` and in a pattern's — are
validated at compile time for the same reason: they become column names on
every row the profile writes, and one the store refuses would cost every
sample that pattern produces, one rejection at a time.

Every default is overridable per query. Metadata changes what a *new* panel
suggests; it never silently changes an existing panel's rendering, because the
panel stores the resolved query, not a reference to the metadata.

## 7. Patterns

```yaml
patterns:
  - set: http                       # destination set
    search: ' HTTP/1.'              # literal prefilter (Aho-Corasick)
    replace:                        # optional normalisation before extraction
      - regex: 'upstream_time: -'
        sub:   'upstream_time: 0'
    extract:                        # tried in order, first match wins
      - '(?P<method>[A-Z]+) (?P<path_x>\S+) HTTP/1\.\d" (?P<status>\d{3}) (?P<bytes_sent>\d+) (?P<request_ms>[\d.]+)'
    labels: [method]                # pattern-local label promotion
    default_values: {bytes_sent: 0} # pad missing captures
    store_stream_label: worker      # attach the stream's ordinal as a field
```

Semantics:

1. `search` is a literal substring used as a prefilter. All `search` strings in
   a profile compile into one Aho-Corasick automaton; the lowest-index pattern
   whose literal appears wins. A pattern with no `search` is always evaluated
   (and is slow — `check` warns).
2. `replace` runs before extraction, in order, on the record text. This is how
   irregular shapes are normalised into one regex — for instance rewriting a
   terse `migration: complete` line into the same shape as the periodic
   `migration: remaining …` line, so one extraction regex covers both.
3. `extract` is a list of regexes tried in order; the first with a match wins.
   Named captures become labels or fields per §5. Numeric-looking string
   captures are coerced to integers, then to floats, and left as strings only
   if neither parses.
4. `route:` lets one pattern fan out to different sets depending on which
   regex matched:

```yaml
  - set: hist               # default set
    search: 'histogram dump'
    route:
      - regex: 'histogram dump: \{(?P<namespace>[^}]+)\}-(?P<hist>[^ ]+) \((?P<total>\d+) total\) msec\s*(?P<buckets>.*)'
        set: hist_ms
      - regex: 'histogram dump: \{(?P<namespace>[^}]+)\}-(?P<hist>[^ ]+) \((?P<total>\d+) total\) usec\s*(?P<buckets>.*)'
        set: hist_us
    bucket_set: hdr24
```

5. `default_values` fills captures the matching regex did not produce, so a
   sparse row still carries a zero where the panel expects one.

## 8. Bucket sets (histograms / heatmaps)

Histogram layouts are declared, not compiled in. A tool that hard-codes "24
power-of-two buckets" in the query layer cannot render anything else; a
declaration costs nothing and renders everything:

```yaml
bucket_sets:
  - name: hdr24
    parse: 'paren_pairs'        # "(00: 1234) (01: 5678) …"
    buckets: ['00','01','02','03','04','05','06','07','08','09','10','11',
              '12','13','14','15','16','17','18','19','20','21','22','23']
    edges:   'pow2'             # pow2 | linear:<step> | explicit:[…]
    edge_unit: ms
    total_field: total
    cumulative: true            # also emit <bucket>plus fields
    tail: true                  # emit `tail` = total − Σ buckets
```

Rules:

- `parse` selects the bucket splitter: `paren_pairs`, `csv` or `key_value`.
  `json_object` is **not implemented**, and the compiler refuses it by name
  rather than accepting a mode that would then fail on every record.
- `edges` gives each bucket a numeric lower bound so the plugin can render a
  real heatmap axis instead of an ordinal one. `pow2` means bucket *k* covers
  `[2^(k-2), 2^(k-1))` with the first three buckets mapping to 0/1/2, matching
  the common HDR-style layout; `explicit:[0,1,2,4,8,…]` covers anything else.
- `cumulative: true` emits `<bucket>plus` fields (`03plus` = count of
  everything at or above bucket 03), computed at ingest so the query path stays
  a scan.
- `tail` captures counts beyond the declared buckets, in a field named
  literally `tail`. A pattern feeding a `tail: true` bucket set may not also
  capture a group by that name; the compiler refuses the clash rather than
  letting one silently overwrite the other.
- A pattern that names a `bucket_set` must capture the payload in a group
  called `buckets` or `histogram`, and the compiler checks that it does.

Bucket-set membership is recorded in the field catalogue, which is how
`FORMAT heatmap` knows which fields form one histogram
([06-query.md §6](06-query.md)).

## 9. Aggregation

Some lines carry an event, not a measurement — "connection refused",
"restarting", `(repeated: 4)` suffixes. Counting them per window at ingest is
far cheaper than storing one row each.

```yaml
    aggregate:
      every: 10s              # window length
      on: [error_class, host] # unique key: one accumulator per distinct tuple
      field: count            # field to accumulate into
      mode: increment         # increment | sum | max | last (an unknown mode is refused)
```

Windows are anchored on the first record of each accumulator, close when a
record arrives at or after `start + every` (and on stream close, or on a
wall-clock timeout in follow mode), and emit one sample carrying the accumulated
field plus the labels of the accumulator. `mode: increment` adds one per record
(the `(repeated: N)` case uses `mode: sum` with the parsed count).

An accumulator is identified by the destination set, the `field` it writes,
the `mode` it writes it with, and the `on` tuple. Two patterns therefore share
a window only when they declare the same column with the same semantics —
which is the case where merging is what the spec asks for. Two patterns
writing different columns of one set, on the same keys, keep their own
windows; before they shared one, and a window keeps the field and mode of
whichever pattern opened it, so one column reported the other's numbers and
the second column was never written at all
([12-implementation.md §6.101](12-implementation.md)).

Aggregation is a *lossy* choice, deliberately: individual occurrences are gone.
`check` prints, for each aggregating pattern, an estimate of the reduction, so
the trade is visible.

## 10. Worked example

A complete, generic profile:

```yaml
version: 1
identity:
  - match_path: '(?P<host>[^/]+)/.*\.log$'
profiles:
  - name: appserver
    select: {path_glob: ['*/app*.log']}
    timestamp:
      formats: [{layout: '2006-01-02 15:04:05.000', regex: '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}'}]
      anchor: prefix
      strip: true
    labels: [pool, error_class]
    fields:
      requests_total:   {kind: counter, max_interval: 20s, unit_hint: short}
      inflight:         {kind: gauge,   max_interval: 20s}
      heap_bytes:       {kind: gauge,   unit_hint: bytes}
      errors:           {kind: delta}
    patterns:
      - set: app
        search: 'stats: '
        extract:
          - 'stats: pool=(?P<pool>\S+) reqs=(?P<requests_total>\d+) inflight=(?P<inflight>\d+) heap=(?P<heap_bytes>\d+)'
      - set: app_errors
        search: 'ERROR '
        extract:
          - 'ERROR (?P<error_class>[A-Za-z.]+): '
        aggregate: {every: 10s, on: [error_class], field: errors, mode: increment}
```

Which, on the query side, becomes:

```mql
FROM app
SELECT requests_total RATE AS "req/s", inflight
WHERE host IN ($host) AND pool = "default"
BY host, pool
```

## 11. Spec changes over time

Specs are versioned by content hash. On reload (`SIGHUP` or
`--spec-reload-interval`):

- a spec that fails to compile is rejected and the running spec is kept, loudly;
- new patterns apply to subsequent records only — no retroactive re-parse
  unless the operator re-runs a `batch` import;
- field metadata changes are pushed to the store's catalogue immediately (they
  affect defaults for *new* queries only);
- removing a field from a spec does not delete stored data; the catalogue marks
  it `stale: true` with a last-seen timestamp so the builder can grey it out.
