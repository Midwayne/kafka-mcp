# Changelog

## [1.0.0] - 2026-11-01

First stable release of Kafka MCP: explore, sample, search, and aggregate Kafka topics across configured clusters.

### Features

- Discover clusters, topics, partition metadata, retention, freshness, and consumer-group lag.
- Search with grep patterns, CEL expressions, exact keys and headers, time windows, and partition or offset selection.
- Infer message structure, validate filters, count matches, and group counts without exporting payloads.
- Run bounded background scans, page results, follow traffic, export JSON Lines, and save reusable scans.
- Decode JSON and Schema Registry-framed Avro, Protobuf, and JSON Schema messages.
- Connect through TLS, Java keystores, SCRAM, OAuth, AWS MSK IAM, and Kerberos.
- Read-only access by default, topic visibility controls, output redaction, and opt-in message production.
- Parallel record processing with batched scan bookkeeping and reduced sampling-lock contention.

### Installation

- Self-contained binaries for Linux, macOS, and Windows on AMD64 and ARM64.
- Claude Desktop `.mcpb` bundles with a configuration-file picker; no Go or Node.js required.
- Cursor and VS Code install buttons using a version-pinned launcher package attached to this release. These require Node.js 20 or newer.
- The launcher downloads the correct binary, verifies its SHA-256 checksum, and caches it locally.
- `SHA256SUMS` covers every binary, desktop bundle, and launcher package.

See the [installation guide](https://github.com/Midwayne/kafka-mcp#quick-start). Configure your Kafka connections before starting the server. Enterprise setups can reuse an existing YAML/JSON configuration and keystores.

### Limitations

Jobs are held in memory and do not survive a server restart. Avro schema references and raw Protobuf without Schema Registry framing are not decoded. See [operations](https://github.com/Midwayne/kafka-mcp/blob/main/docs/operations.md#limitations) for details.
