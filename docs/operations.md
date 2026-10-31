# Operations

- [Performance](#performance)
- [Jobs, paging, and follow mode](#jobs-paging-and-follow-mode)
- [Exports and saved scans](#exports-and-saved-scans)
- [Security](#security)
- [Limitations](#limitations)

## Performance

### Where the time goes

A scan reads records from brokers, then filters them in a worker pool. Measured CPU cost per message on one core (Apple M-series, `go test -bench . ./internal/filter ./internal/grep`):

| Check | Cost |
|---|---|
| grep, fixed string or simple regex on raw bytes | ~0.2 µs |
| grep, `ignore_case` on raw bytes | ~0.4 µs |
| CEL on headers only (value never decoded) | ~0.3 µs |
| grep on one decoded field | ~1.4 µs |
| CEL on a decoded JSON value | ~2 µs |

That is roughly 0.5 to 5 million messages per second per core, so **the network is almost always the limit**. For example, over a corporate VPN to a production cluster we measured ~0.75 MB/s per broker connection with no broker throttling, which works out to about 1,000–2,500 records per second for ~600-byte messages. On a host in the same network as the brokers, expect orders of magnitude more.

### Read less

These matter far more than CPU tuning:

1. **Narrow the window.** `from`/`to` become offsets through Kafka's timestamp index, so nothing outside the window is fetched.
2. **Use `count_messages`** for "how many" and "which kinds" questions; it never transfers results to the agent.
3. **Use the key partitioner** for key lookups: `key` plus `key_partitioner: murmur2` reads one partition instead of all of them.
4. **Use `dry_run: true`** to see `records_in_range` before committing to a long scan.
5. **Stop early** with a small `limit`, then page with `resume_offsets`.

### Settings that affect throughput

| Setting | Effect |
|---|---|
| `fetch.max_partition_bytes` (1 MiB) | Response size per partition. Kafka's default. Larger values add little on fast networks and make responses slow to arrive on slow links, which delays limits and progress. |
| `fetch.max_bytes` (50 MiB) | Response size per broker request |
| `defaults.scan.workers` (CPUs) | Filter parallelism within one scan |
| `defaults.scan.rate_limit` (unlimited) | Per-scan byte-rate cap that protects brokers and shared links |
| `rack` | Same-rack follower fetching (needs `replica.selector.class` on brokers): cheaper cross-zone traffic and less load on leaders |
| `defaults.jobs.max_concurrent` (4) | Concurrent background scans |

### Impact on the cluster

- Consumers are group-less and assigned to explicit partitions: no group coordinator, no rebalances, no committed offsets.
- Reading old data is served from disk (or tiered storage) and can evict hot pages from broker memory. Prefer recent windows, and set `rate_limit` on shared deployments.
- Broker quotas apply as usual, under the configured `client_id` and principal.

## Jobs, paging, and follow mode

**Waiting.** Each scan runs as a job. The call waits up to `wait_seconds` (default 20, max 50, which keeps it under typical MCP client timeouts). If the scan is still running, the call returns `job_id` and live `progress`; finish with `get_job` (which can also wait) or stop it with `cancel_job`.

**Budgets.** `limit`, `max_scanned`, `max_bytes`, and `timeout_seconds` are capped by `defaults.scan`. The `stop_reason` and `hints` say which one ended the scan.

**Paging.** When a scan stops before the end of its range, `stats.next_offsets` holds, per partition, the first record that was not both read and delivered. Passing it back as `resume_offsets` continues with **no gaps**. Because partitions are filtered in parallel, a page may repeat a few matches from the previous page; deduplicate by `topic`/`partition`/`offset` if that matters.

**Follow.** `follow: true` starts at `latest` by default and keeps reading until `limit` matches arrive or `timeout_seconds` passes, like `tail -f | grep`.

**Ordering.** Results are sorted by timestamp, then topic, partition, and offset. Kafka only orders records within a partition, so a limited scan returns *some* matches from the range, not necessarily the earliest ones overall. Narrow `from`/`to` when "first" matters.

## Exports and saved scans

**Exports.** `export: true` streams every match to `<state_dir>/exports/<job_id>.jsonl`, one JSON object per line. Values are redacted but not truncated, the file is created with mode `0600`, and writing stops at `max_export_records`. The result's `export` field gives the path and record count. Agents with file access can then analyse the file with their own tools.

**Saved scans.** `save_scan` validates a scan or count definition and stores it in `<state_dir>/saved-scans.json` (written atomically, mode `0600`). `run_saved_scan` re-runs it with optional overrides, for example `from: "-24h"` for a daily check, or `resume_offsets` to continue from the last run.

## Security

| Concern | Behaviour |
|---|---|
| Writes | Read-only by default. `produce_message` is registered only if a cluster is `read_write` and is refused on read-only clusters. |
| Consumer groups | Never joined; offsets are never committed |
| Credentials | Read from files or environment variables at startup; never returned by any tool. `list_clusters` reports only the auth *type*. |
| TLS | Full chain verification by default. `verify_hostname: false` skips only the name match; `insecure_skip_verify` disables verification and is reported by `list_clusters`. |
| Visibility | Topic include/exclude globs apply to listings, scans, describe calls, and consumer-group lag |
| Redaction | `redact` paths are masked in results, grep snippets, schema examples, samples, and exports |
| Tool control | Global and per-cluster tool include/exclude lists |
| HTTP transport | Binds to `127.0.0.1` by default. Binding elsewhere requires `bearer_token`, which is compared in constant time. |
| Local state | The state directory, exports, and saved scans are created owner-only |

**Redaction is output masking, not access control.** Filters and `group_by` evaluate the raw values, so an agent can test or group by a redacted field (for example `group_by: value.email` reveals values as group keys). If a field must not be readable at all, exclude the topic or restrict it with broker ACLs.

Agents act on behalf of the configured principal. Give kafka-mcp its own Kafka credentials with only the ACLs it needs (typically `Describe` and `Read` on specific topics, and `Describe` on the groups whose lag you want to see).

## Limitations

- Avro schemas that use Schema Registry *references* are not decoded yet (they fall back to base64 with a `decode_errors` note). Protobuf references are supported.
- Protobuf without Schema Registry framing (raw protobuf with a local descriptor) is not decoded.
- Jobs are held in memory and do not survive a restart; saved scans do.
- `idle` stops depend on a quiet period (`idle_timeout`), because Kafka clients cannot observe a partition's read position directly. They only happen for compacted topics whose final offsets were removed.
- One scan covers one cluster. To search several clusters, start one scan per cluster with `wait_seconds: 0` and collect each with `get_job`.
