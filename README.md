# kafka-mcp

A [Model Context Protocol](https://modelcontextprotocol.io) server that lets AI agents explore, sample, search, and aggregate Kafka topics across many clusters, safely and with bounded resource use, even on topics with millions of messages.

An agent can:

- **Discover** clusters, topics, partitions, offsets, retention, freshness, and consumer-group lag.
- **Learn the data**: infer field paths, types, and example values from recent messages instead of reading raw payloads.
- **Search** with grep-style patterns (regex, fixed strings, ignore case, whole words, invert, any or all), scoped to raw bytes, decoded payloads, or single fields. CEL expressions handle structured logic.
- **Aggregate**: count matches grouped by any expression without pulling messages back.
- **Drill in**: fetch any message with surrounding context (like `grep -C`), page through results, follow live traffic, and export every match to a file.
- **Automate**: save useful scans and re-run them later with new time windows.

Read-only by default. It never joins consumer groups or commits offsets, so it cannot disturb production consumers.

## Contents

- [Quick start](#quick-start)
- [Connect your agent](#connect-your-agent)
- [Tools](#tools)
- [How an agent uses it](#how-an-agent-uses-it)
- [Efficiency on large topics](#efficiency-on-large-topics)
- [Security model](#security-model)
- [Documentation](#documentation)
- [Development](#development)

## Quick start

**1. Build** (Go 1.26+):

```sh
git clone https://github.com/Midwayne/kafka-mcp.git
cd kafka-mcp
make build            # produces ./bin/kafka-mcp
```

**2. Configure.** Create `kafka-mcp.yaml`, or `~/.kafka-mcp/config.yaml`, or point `KAFKA_MCP_CONFIG` at a file:

```yaml
clusters:
  - name: local
    brokers: ["localhost:9092"]
```

For TLS with Java keystores, which is the common corporate setup:

```yaml
clusters:
  - name: orders
    brokers: ["kafka.example.com:9093"]
    security_protocol: SSL
    tls:
      truststore: ~/certs/truststore.jks          # JKS or PKCS#12, detected automatically
      truststore_password: ${file:~/certs/truststore.pw}
      keystore: ~/certs/keystore.jks
      keystore_password: ${file:~/certs/keystore.pw}
```

Secrets are read from files or environment variables at startup (`${file:...}`, `${ENV}`), so they never live in the config. See [Authentication](docs/authentication.md) for SASL/SCRAM, OAuth, AWS MSK IAM, Kerberos, Confluent Cloud, and PEM.

**3. Verify.** The check connects to every broker of every cluster:

```sh
./bin/kafka-mcp -config kafka-mcp.yaml -check
# OK   orders               80 brokers reachable (6.2s)
```

When something fails, the output includes a hint. For example, brokers whose certificates don't match their advertised addresses get pointed to `tls.verify_hostname: false`.

## Connect your agent

The server speaks MCP over stdio by default. Use absolute paths.

<details open>
<summary><b>Claude Code</b></summary>

```sh
claude mcp add kafka -- /abs/path/bin/kafka-mcp -config /abs/path/kafka-mcp.yaml
```
</details>

<details>
<summary><b>Claude Desktop</b> (<code>claude_desktop_config.json</code>)</summary>

```json
{
  "mcpServers": {
    "kafka": { "command": "/abs/path/bin/kafka-mcp", "args": ["-config", "/abs/path/kafka-mcp.yaml"] }
  }
}
```
</details>

<details>
<summary><b>Cursor</b> (<code>~/.cursor/mcp.json</code> or <code>.cursor/mcp.json</code>)</summary>

```json
{
  "mcpServers": {
    "kafka": { "command": "/abs/path/bin/kafka-mcp", "args": ["-config", "/abs/path/kafka-mcp.yaml"] }
  }
}
```
</details>

<details>
<summary><b>VS Code</b> (<code>.vscode/mcp.json</code>)</summary>

```json
{
  "servers": {
    "kafka": { "type": "stdio", "command": "/abs/path/bin/kafka-mcp", "args": ["-config", "/abs/path/kafka-mcp.yaml"] }
  }
}
```
</details>

<details>
<summary><b>OpenCode</b> (<code>opencode.json</code>)</summary>

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "kafka": { "type": "local", "command": ["/abs/path/bin/kafka-mcp", "-config", "/abs/path/kafka-mcp.yaml"] }
  }
}
```
</details>

<details>
<summary><b>Shared HTTP server</b></summary>

Set `server.serve_mode: http` (streamable HTTP at `/mcp`) or `sse`. Binding beyond localhost requires `server.bearer_token`; clients send `Authorization: Bearer <token>`. See [examples/http-shared.yaml](examples/http-shared.yaml).
</details>

Then ask, for example: *"Which event types were published to order-events in the last 6 hours, and how many of each?"*

## Tools

| Tool | Purpose |
|---|---|
| `list_clusters` | Configured clusters, access mode, auth type (no secrets), filters |
| `describe_cluster` | Connect to every broker; cluster ID, broker and rack counts |
| `list_topics` | Visible topics, partitions, replication, optional message counts |
| `describe_topic` | Per-partition offsets and counts, newest message time, retention and other configs, offsets at a given time |
| `list_consumer_groups` / `describe_consumer_group` | Groups, state, members, lag per partition (read-only) |
| `sample_messages` | A few decoded messages, newest by default |
| `get_messages` | One message by offset with surrounding context (`before`/`after`/`context`) |
| `infer_schema` | Field paths, types, coverage, examples, detected decoders, starter filters |
| `validate_filter` | Compile a CEL filter, grep spec, or group-by and test it on recent messages |
| `scan_messages` | Search with time/offset ranges, key, headers, grep, and CEL; paging, follow, export, dry run |
| `count_messages` | Count matches, grouped by any CEL expression |
| `get_job` / `list_jobs` / `cancel_job` | Background scans: progress, results, cancellation |
| `save_scan` / `list_saved_scans` / `run_saved_scan` / `delete_saved_scan` | Reusable named scans |
| `list_schema_subjects` / `get_schema` | Schema Registry lookups |
| `produce_message` | Write a message (registered only when a cluster is `read_write`) |

Full parameters and examples: [docs/tools.md](docs/tools.md).

## How an agent uses it

```text
list_topics(cluster="east", pattern="*order*", with_counts=true)
infer_schema(cluster="east", topics=["order-events"])
  -> headers.event_type: string [ORDER_ADD, ...]; value.data.item_count: int (80%) ...
validate_filter(filter="value.data.item_count > 200", cluster="east", topics=["order-events"])
  -> 215 of 300 messages raise errors: the field is optional
scan_messages(cluster="east", topics=["order-events"], from="-6h",
              headers={"event_type": "ORDER_ADD"},
              filter="value.data.?item_count.orValue(0) > 200",
              fields=["value.data.order_id", "headers.store_id"], limit=20)
count_messages(..., group_by="headers['store_id']", top=10)
get_messages(cluster="east", topic="order-events", partition=2, offset=4810697, context=2)
save_scan(name="large-orders", spec={...})
```

grep-style search when the structure is unknown or irrelevant:

```json
{"grep": {"patterns": ["timeout", "ERR-\\d{3}"], "ignore_case": true, "in": ["value"], "only_matching": true}}
```

Every result carries `stats` (scanned, matched, stop reason, throughput) and `hints` that suggest the next step: resume a page, guard a missing field, or widen the window. See [Filtering](docs/filtering.md).

## Efficiency on large topics

- **Reads less.** Time windows become offsets via Kafka's timestamp index; a `key` with `key_partitioner` reads one partition; budgets (`limit`, `max_scanned`, `max_bytes`, `timeout_seconds`) stop work early, and `stats.next_offsets` resumes exactly where a scan stopped.
- **Checks cheaply first.** Exact key and header checks, then grep on raw bytes, then CEL. Payloads are decoded only if an expression actually reaches them.
- **Parallel and bounded.** Partitions are fetched concurrently and filtered by a worker pool, with constant memory. An optional per-scan `rate_limit` protects brokers. `rack` enables follower fetching.
- **Small results.** `fields`, `only_matching`, `count_messages`, and truncation keep responses small enough for an agent's context window.

Measured per-message CPU cost on one core: 0.2 µs for raw-bytes grep, 0.3 µs for header-only filters, about 2 µs for filters that decode a JSON value. In practice the network is the limit. See [Operations](docs/operations.md#performance).

## Security model

- **Read-only by default.** `produce_message` exists only for clusters marked `read_write`.
- **No consumer groups.** Reads use group-less, assigned-partition consumers that never commit offsets.
- **Visibility controls.** Per-cluster topic include/exclude globs, tool allow/deny lists, and `redact` paths masked in every result, grep snippet, and export.
- **Secrets stay out of config.** Credentials come from files or environment variables, are never echoed by any tool, and TLS keys stay in memory.
- **HTTP hardening.** Bearer-token auth is required when the server listens beyond localhost.

Redaction masks output; it is not access control. Use topic filters or broker ACLs to restrict reads. Details: [Operations](docs/operations.md#security).

## Documentation

| Guide | Contents |
|---|---|
| [Configuration](docs/configuration.md) | Every setting, defaults, placeholders, decoders |
| [Authentication](docs/authentication.md) | TLS (PEM, JKS, PKCS#12), SASL, OAuth, MSK IAM, Kerberos, Schema Registry, troubleshooting |
| [Tools](docs/tools.md) | Tool reference with parameters and examples |
| [Filtering](docs/filtering.md) | grep, CEL cookbook, group-by, field paths, decoding |
| [Operations](docs/operations.md) | Performance tuning, jobs and paging, exports, saved scans, security, limitations |
| [Architecture](docs/architecture.md) | Package layout and data flow for contributors |
| [examples/](examples) | Ready-to-copy configs |

## Development

```sh
make check    # go vet, golangci-lint, race-enabled tests, build
make test     # tests only
make lint     # linters, including 199-line files and 50-line functions
```

Tests use an in-memory Kafka (`kfake`), so no Docker is needed. Coverage includes mutual TLS with JKS and PKCS#12, SCRAM, Schema Registry Avro and Protobuf, transactions, and full MCP sessions. See [CONTRIBUTING.md](CONTRIBUTING.md).
