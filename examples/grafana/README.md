# Grafana plugin package

The backend datasource is implemented (`internal/plugin`, served by
`mensura-store --mode=plugin`); the React query editor is not built yet, so
this directory holds the packaging pieces rather than an installable plugin.

What is here:

- `plugin.json` — the plugin manifest. `executable` points at the
  `mensura-store` binary, renamed per Grafana's `gpx_<name>_<os>_<arch>`
  convention.
- `provisioning/datasources/mensura.yaml` — datasource provisioning for both
  topologies.

Until the frontend exists, a panel supplies its query as the MQL **AST** in
the query model:

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
