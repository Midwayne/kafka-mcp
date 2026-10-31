# Configuration

kafka-mcp reads one YAML or JSON file. The fully annotated reference is [examples/kafka-mcp.example.yaml](../examples/kafka-mcp.example.yaml); this page explains each setting.

- [Where the config is loaded from](#where-the-config-is-loaded-from)
- [Secrets and placeholders](#secrets-and-placeholders)
- [server](#server)
- [defaults](#defaults)
- [clusters](#clusters)
- [Payload decoding (formats)](#payload-decoding-formats)
- [Topic filters, redaction, and tool filters](#topic-filters-redaction-and-tool-filters)
- [Units](#units)

## Where the config is loaded from

The first match wins:

1. `-config /path/to/file.yaml`
2. The `KAFKA_MCP_CONFIG` environment variable
3. `./kafka-mcp.yaml`, `./kafka-mcp.yml`, or `./kafka-mcp.json`
4. `~/.kafka-mcp/config.yaml`

Unknown keys are rejected, so a typo like `brokerz` fails at startup instead of being ignored. Run `kafka-mcp -check` after editing.

## Secrets and placeholders

Placeholders are expanded in **values** when the server starts. Comments are ignored, and expanded values are never re-parsed as YAML, so a password may contain `:`, `#`, quotes, or braces.

| Syntax | Result |
|---|---|
| `${VAR}` | Environment variable (empty if unset) |
| `${VAR:-fallback}` | Environment variable, or `fallback` if unset |
| `${file:/path/to/secret}` | File contents with surrounding whitespace trimmed. `~/` is allowed. |
| `$$` | A literal `$` |

A bare `$VAR` (without braces) is left as is, so passwords containing `$` survive.

```yaml
tls:
  keystore_password: ${file:~/certs/client/pw.txt}
sasl:
  password: ${KAFKA_PASSWORD}
```

YAML anchors work, which is handy when several clusters share TLS material:

```yaml
clusters:
  - name: dc1
    brokers: ["kafka-dc1:9093"]
    tls: &corp-tls
      truststore: ~/certs/truststore.jks
      truststore_password: ${file:~/certs/truststore.pw}
  - name: dc2
    brokers: ["kafka-dc2:9093"]
    tls: *corp-tls
```

## server

| Key | Default | Meaning |
|---|---|---|
| `name` | `Kafka MCP` | Name reported to MCP clients |
| `serve_mode` | `stdio` | `stdio` for desktop and CLI agents; `http` for streamable HTTP at `/mcp`; `sse` |
| `host` | `127.0.0.1` | Listen address for `http`/`sse` |
| `port` | `8090` | Listen port for `http`/`sse` |
| `bearer_token` | empty | Require `Authorization: Bearer <token>`. **Mandatory** when `host` is not loopback. |
| `state_dir` | `~/.kafka-mcp` | Saved scans (`saved-scans.json`) and exports (`exports/*.jsonl`) |

## defaults

These apply to every cluster unless the cluster overrides them.

| Key | Default | Meaning |
|---|---|---|
| `access` | `read_only` | `read_only` or `read_write` (enables `produce_message`) |
| `client_id` | `kafka-mcp` | Kafka client ID, visible in broker logs and quotas |
| `dial_timeout` | `10s` | TCP and TLS connect timeout |
| `request_timeout` | `30s` | Extra time allowed per Kafka request |
| `isolation` | `read_uncommitted` | `read_committed` hides records from aborted transactions |
| `fetch.max_bytes` | `50MiB` | Maximum bytes per fetch response |
| `fetch.max_partition_bytes` | `1MiB` | Maximum bytes per partition per fetch; keep small over slow links |

### defaults.scan

Hard limits for every scan. Agents may ask for less, never more.

| Key | Default | Meaning |
|---|---|---|
| `workers` | number of CPUs | Goroutines decoding and filtering records per scan |
| `default_limit` / `max_limit` | `20` / `500` | Matches returned per call |
| `max_scanned` | `5000000` | Records one scan may read |
| `max_bytes` | `2GiB` | Key and value bytes one scan may read |
| `default_timeout` / `max_timeout` | `1m` / `15m` | Time budget per scan |
| `rate_limit` | `0` (unlimited) | Bytes per second per scan, e.g. `20MiB` to protect brokers |
| `max_value_chars` | `1000` | Longer strings in results are truncated (agents can raise this up to 100000) |
| `max_export_records` | `1000000` | Cap on records written by `export: true` |
| `idle_timeout` | `30s` | How long a partition that has already returned data may stay silent before the scan treats it as drained. Applies only to the rare compacted topic whose final offsets were removed. |

### defaults.jobs

| Key | Default | Meaning |
|---|---|---|
| `max_concurrent` | `4` | Background scans running at once; further requests are refused with a clear message |
| `ttl` | `1h` | How long finished jobs remain available to `get_job` |

## clusters

Each entry is one Kafka cluster. Only `name` and `brokers` are required.

| Key | Meaning |
|---|---|
| `name` | Identifier agents pass as `cluster` |
| `brokers` | Bootstrap servers, as a list or comma-separated |
| `access` | Overrides `defaults.access` |
| `enabled` | `false` hides the cluster without deleting it |
| `client_id`, `isolation`, `dial_timeout`, `request_timeout`, `fetch` | Override the defaults |
| `rack` | Client rack ID; with broker-side `replica.selector.class` configured, reads come from same-rack followers |
| `security_protocol` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`. Optional; inferred from the `tls` and `sasl` blocks and validated for consistency. |
| `tls` | See [Authentication: TLS](authentication.md#tls) |
| `sasl` | See [Authentication: SASL](authentication.md#sasl) |
| `schema_registry` | See [Authentication: Schema Registry](authentication.md#schema-registry) |
| `topics` | Visible topics (include/exclude globs) |
| `formats` | Default key and value decoders, optionally per topic |
| `redact` | Field paths masked in all output |
| `tools` | Per-cluster tool include/exclude |

## Payload decoding (formats)

| Format | Behaviour |
|---|---|
| `auto` (default) | Schema Registry framing if a registry is configured and the payload starts with magic byte 0; else JSON if it parses; else printable text; else base64 |
| `json` | JSON. Integers stay exact (64-bit IDs are not rounded). Invalid JSON falls back to text with a `decode_errors` note. |
| `string` | Printable UTF-8 text, else base64 |
| `base64`, `hex` | Binary rendering |
| `schema_registry` (aliases `avro`, `protobuf`, `json_schema`) | Confluent wire format; the schema type comes from the registry |
| `none` | Skip decoding (shows `null`). Useful for keys you never look at. |

Per-topic overrides apply to topics matching the first matching glob:

```yaml
formats:
  key: string
  value: auto
  topics:
    - pattern: "payments.*"
      value: schema_registry
```

Agents can override decoding per call with `key_format` and `value_format`.

## Topic filters, redaction, and tool filters

**Topics.** Hidden topics are absent from listings, cannot be scanned, and are omitted from consumer-group lag.

```yaml
topics:
  include: ["orders.*", "shipments.*"]   # empty = everything
  exclude: ["*.pii", "_*"]
```

**Redaction.** Matching values become `[REDACTED]` in results, grep snippets, inferred-schema examples, and exports. `*` matches any key or array index.

```yaml
redact:
  - value.customer.email
  - value.payments.*.card_number
  - headers.authorization
```

Redaction masks output only. Filters can still test redacted fields, so an agent could infer their values by trial. Use `topics.exclude` or broker ACLs for real restrictions.

**Tools.** Glob filters over tool names, globally (`tools:` at the top level) or per cluster:

```yaml
tools:
  exclude: [produce_message, "*_saved_scan*"]
```

## Units

- **Durations:** `500ms`, `30s`, `5m`, `2h`, `7d`.
- **Sizes:** plain bytes or `KiB`, `MiB`, `GiB`, `TiB` (powers of 1024), `KB`, `MB`, `GB` (powers of 1000), and the shorthands `k`, `m`, `g` (powers of 1024).
