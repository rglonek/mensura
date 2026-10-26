# Grafana plugin package

The backend datasource is implemented (`internal/plugin`, served by
`mensura-store --mode=plugin`). The query editor lives in `src/` and posts
MQL text to the backend `parse` resource, which is the only parser. Build it
with `npm install && npm run build`; Grafana loads `dist/module.js`.

What is here:

- `plugin.json` — the plugin manifest. `executable` points at the
  `mensura-store` binary, renamed per Grafana's `gpx_<name>_<os>_<arch>`
  convention.
- `provisioning/datasources/mensura.yaml` — datasource provisioning for both
  topologies.

A panel can still supply the MQL AST directly. The editor stores the same AST
after a successful parse:

```json
{ "ast": { "from": "app",
           "select": [ { "field": "requests_total",
                         "as": "req/s",
                         "modifiers": { "delta": true, "perSecond": true, "gapMs": 20000 } } ],
           "by": ["host"] } }
```

The backend also accepts `{"text": "FROM app SELECT …"}` and parses it, which
is what the editor's code mode will post. There is exactly one parser, in Go,
reachable over `CallResource` at `parse` and `print`; the frontend will never
implement a second one.
