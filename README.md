# ArangoDB Connector

Adds ArangoDB connectivity as an installable connector extension.

This connector is listed in the public Irodori extension marketplace.

## Connector

- Extension ID: `irodori.arangodb`
- Engine ID: `arangodb`
- Wire: `graph`
- Default port: `8529`
- Native ABI: `irodori.connector.native.v1`
- Driver linked: `true`

The native driver uses ArangoDB's HTTP API for version checks, AQL cursor queries, and collection metadata.

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust code keeps native ABI exports in `src/lib.rs`, shared buffer/JSON helpers in `src/abi.rs`, and ArangoDB behavior in `src/driver.rs`.

## Connection Metadata

- Endpoint modes: `hostPort`, `connectionString`
- Transport modes: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS supported: `true`
- Custom driver options: `true`

| Auth method | Label | Secret purposes |
|---|---|---|
| `none` | No authentication | none |
| `connectionString` | Connection string / DSN | none |
| `basic` | Basic authentication | `password` |
| `jwt` | JWT bearer token | `token` |
| `clientCertificate` | Client certificate / mTLS | `privateKey`, `privateKeyPassphrase` |
| `customDriverOptions` | Custom driver options | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## Experience Metadata

- Domains: `graph`
- Result views: `graph`, `path`, `table`, `json`
- Inspired by: `ArangoDB Web Interface Graph Viewer`, `AQL graph traversals`, `AQL shortest path`

| Workflow | Result view | Templates |
|---|---|---|
| Explore neighborhood | graph | graph-aql-neighborhood |
| Shortest path | path | graph-aql-shortest-path |
| Collection sample | table | graph-aql-sample |

| Template | Label | Language | Result view |
|---|---|---|---|
| `graph-aql-neighborhood` | Neighborhood traversal | `aql` | `graph` |
| `graph-aql-shortest-path` | Shortest path | `aql` | `path` |
| `graph-aql-sample` | Collection sample | `aql` | `table` |

## ABI Calls

The driver handles these JSON requests today:

| Method | Response |
|---|---|
| `health` / `ping` | Connector health, engine id, ABI version, and driver link status. |
| `describe` / `capabilities` | Embedded manifest and connector config. |
| `manifest` | Raw `irodori.extension.json`. |
| `config` | Raw `connector.config.json`. |
| `connect` | Opens an HTTP client and reads ArangoDB version. |
| `query` | Runs AQL through `_api/cursor`. |
| `metadata` | Loads collection metadata. |
| `close` | Removes the cached native connection. |

## Development


Generated extension repositories share `../target` across sibling repositories so Rust dependencies are compiled once per checkout. DuckDB and MotherDuck are driver-linked by default; set `IRODORI_CONNECTOR_LINK_DUCKDB=0` only when you need metadata-only DuckDB-compatible scaffolds.


```sh
make check
make build
```

Release packages place platform-specific native artifacts under `dist/native`.
