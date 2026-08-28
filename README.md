# Mensura

Mensura turns operational text logs and pushed metrics into interactive
Grafana time-series, built around one idea: **the operator must be able to
trust the plot**. It refuses to erase spikes, to interpolate across outages,
to silently drop series, or to hide counter resets behind a smooth slope.

The render-time algorithm is specified in *A Correctness-First, Single-Pass
Downsampling Pipeline for Operational Time-Series* (Glonek, 2026,
[DOI 10.5281/zenodo.22133876](https://doi.org/10.5281/zenodo.22133876)).

## Three components, two binaries

| Component | Binary | Owns |
| --- | --- | --- |
| **Ingest** | `mensura-ingest` | Acquisition, extraction, delivery to the store's write API |
| **Store** | `mensura-store` | The embedded LSM engine, write and query APIs, the render pipeline |
| **Plugin** | shipped inside `mensura-store` | The Grafana backend datasource |

Ingest speaks to the store over the network in **every** mode — one-shot
import, file follow, remote follow and network receive — so there is one write
path with one set of semantics. The plugin is not a third binary because
plugin and store are Grafana neighbours: in the default topology the datasource
backend *is* the store process, so a panel query is a range seek rather than a
network hop.

## Quick start

```bash
go build -o bin/mensura-store ./cmd/mensura-store
go build -o bin/mensura-ingest ./cmd/mensura-ingest

# A store. Loopback with auth off is fine; a non-loopback bind without auth
# is refused at startup rather than quietly serving an open write API.
bin/mensura-store --data-dir /var/lib/mensura --listen-write 127.0.0.1:9631 \
                  --durability batch --retention 0 &

# Check the spec against a real file before importing anything: `check`
# reports match rates and prints the first lines it could not handle.
bin/mensura-ingest check --spec examples/specs/appserver.yaml --sample /logs/web1/app.log

# Import, then query.
bin/mensura-ingest batch --spec examples/specs/appserver.yaml --source /logs --label dc=eu-west-1
bin/mensura-ingest query --from 24h \
  'FROM app SELECT requests_total RATE AS "req/s" BY host, pool'
```

Continuous ingest instead of a one-shot import:

```bash
bin/mensura-ingest follow  --spec spec.yaml --path '/var/log/app/*.log'
bin/mensura-ingest follow  --spec spec.yaml --ssh-host db1 --path '/var/log/service/*.log'
bin/mensura-ingest receive --spec spec.yaml --listen-tcp :9640 --listen-udp :9640 --mode metrics
```

## MQL

Queries are stored as a JSON AST and written as text; the two round-trip
losslessly, so a visual builder and a text editor are the same query.

```mql
FROM   http
SELECT requests_total RATE GAP 30s AS "req/s",
       errors_total   RATE GAP 30s AS "err/s"
WHERE  host IN ($host) AND dc = "eu-west-1"
BY     host
```

Modifiers (`DELTA`, `PER SECOND`, `RATE`, `NEGATE`, `CLAMP … ELSE RAW`, `GAP`,
`SSE`) are declarative flags: the engine applies them in one canonical order,
because that ordering is the correctness contract rather than a preference.

## Documentation

The design set lives in [`docs/design/`](docs/design/README.md) — architecture,
ingest, extraction spec, wire protocol, storage engine, query language,
downsampling, plugin, operations, decisions, roadmap, and
[implementation status with every knowing divergence](docs/design/12-implementation.md).

## Status

The engine, store, query language, render pipeline, extraction engine, ingest
pipeline and Grafana backend are implemented and tested. The plugin's React
frontend, protobuf wire encoding, remote batch sources and a few M5/M6 items
are not yet built; [12-implementation.md](docs/design/12-implementation.md)
lists exactly what is and is not there.

## License

Apache 2.0. See [LICENSE](LICENSE).
