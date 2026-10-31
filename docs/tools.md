# Tool reference

Every tool returns compact JSON. Errors are returned as tool errors whose text ends with a `Hint:` line when the cause is recognizable (TLS, auth, ACLs, network).

Shared argument groups are described once in [Common scan arguments](#common-scan-arguments).

- Discovery: [`list_clusters`](#list_clusters), [`describe_cluster`](#describe_cluster), [`list_topics`](#list_topics), [`describe_topic`](#describe_topic), [`list_consumer_groups`](#list_consumer_groups), [`describe_consumer_group`](#describe_consumer_group)
- Messages: [`sample_messages`](#sample_messages), [`get_messages`](#get_messages), [`infer_schema`](#infer_schema), [`validate_filter`](#validate_filter), [`scan_messages`](#scan_messages), [`count_messages`](#count_messages)
- Jobs: [`get_job`](#get_job), [`list_jobs`](#list_jobs), [`cancel_job`](#cancel_job)
- Saved scans: [`save_scan`](#save_scan), [`list_saved_scans`](#list_saved_scans), [`run_saved_scan`](#run_saved_scan), [`delete_saved_scan`](#delete_saved_scan)
- Schema Registry: [`list_schema_subjects`](#list_schema_subjects), [`get_schema`](#get_schema)
- Writes: [`produce_message`](#produce_message)

## Discovery

### `list_clusters`

No arguments. Returns each cluster's name, brokers, access mode, auth summary (TLS, client certificate, SASL mechanism; never secrets), topic filter, redacted fields, and default decoders. Does not contact brokers.

### `describe_cluster`

| Argument | Description |
|---|---|
| `cluster` | Cluster name |
| `include_brokers` | List every broker address (off by default; large clusters have many) |

Connects to **every** broker and returns the cluster ID, controller, broker and rack counts, reachable brokers, and visible topics.

### `list_topics`

| Argument | Description |
|---|---|
| `cluster` | Cluster name |
| `pattern` | Glob, e.g. `*orders*` |
| `include_internal` | Include `__consumer_offsets` and similar |
| `with_counts` | Add approximate message counts (end minus start offsets) |
| `limit` | Default 500 |

### `describe_topic`

| Argument | Description |
|---|---|
| `cluster`, `topic` | |
| `at` | Also report each partition's first offset at or after this time |

Returns partitions (leader, replicas, ISR, start and end offsets, approximate count, newest message time), the topic's newest message time, and key configs (`cleanup.policy`, `retention.ms`, `retention.bytes`, `min.insync.replicas`, `max.message.bytes`, `compression.type`, `message.timestamp.type`). If describing configs is denied, `configs_error` explains why and the rest still returns.

### `list_consumer_groups`

| Argument | Description |
|---|---|
| `cluster` | |
| `pattern` | Glob on group IDs |

### `describe_consumer_group`

| Argument | Description |
|---|---|
| `cluster`, `group` | |

Returns state, member count, total lag, lag by topic, and per-partition committed offset, end offset, lag, and owning member. Read-only: it never joins the group. Partitions of hidden topics are omitted.

## Messages

### Common scan arguments

`scan_messages`, `count_messages`, and `save_scan` (inside `spec`) accept these. See [Filtering](filtering.md) for semantics and examples.

| Group | Arguments |
|---|---|
| Target | `cluster`, `topics` (names or globs, up to 50), `partitions` |
| Window | `from`, `to`, `start_offset`, `end_offset`, `resume_offsets`, `follow` |
| Match | `filter` (CEL), `grep` (object), `key`, `key_partitioner`, `headers` |
| Output | `fields`, `max_value_chars`, `key_format`, `value_format` |
| Budget | `limit`, `max_scanned`, `max_bytes`, `timeout_seconds`, `wait_seconds` |
| Other | `export`, `dry_run` |

Budgets are capped by the server's `defaults.scan` limits.

**Result shape** (`scan_messages`, `count_messages`, `get_job`):

```json
{
  "job_id": "job-1a2b3c4d5e6f",
  "status": "completed",
  "plan": {"topics": ["orders"], "partitions": 3, "records_in_range": 88196, "time_window": {"from": "..."}, "ranges": [...]},
  "stats": {
    "scanned": 1262, "matched": 2, "bytes": 731004, "filter_errors": 0,
    "partitions": 3, "partitions_done": 0, "elapsed_ms": 4932, "records_per_sec": 255,
    "stop_reason": "limit",
    "next_offsets": {"orders": {"0": 120034, "1": 119876, "2": 120500}}
  },
  "matches": [{"topic": "orders", "partition": 2, "offset": 120499, "timestamp": "...", "key": "...", "value": {...}, "headers": {...}}],
  "hints": ["Stopped at the match limit. To get more, call again with resume_offsets set to stats.next_offsets (or raise limit)."]
}
```

`stop_reason` is one of:

| Value | Meaning |
|---|---|
| `completed` | The whole range was read |
| `limit` | Enough matches were found |
| `max_scanned`, `max_bytes`, `timeout` | A budget ran out |
| `cancelled` | `cancel_job` was called |
| `idle` | Partitions went quiet before reaching their end (rare; compacted tails) |

`next_offsets` is present whenever unread or undelivered records remain.

### `sample_messages`

| Argument | Description |
|---|---|
| Target | `cluster`, `topics`, `partitions` |
| `count` | Default 5, max 50 |
| `from` | `latest` (default) returns the newest messages; `earliest` or a time reads forward |
| Output | `fields`, `max_value_chars`, `key_format`, `value_format` |

### `get_messages`

| Argument | Description |
|---|---|
| `cluster`, `topic`, `partition`, `offset` | The message to fetch |
| `before`, `after` | Neighbouring messages in the same partition (like `grep -B`/`-A`, max 50) |
| `context` | Both directions (like `grep -C`) |
| Output | As above |

### `infer_schema`

| Argument | Description |
|---|---|
| Target | `cluster`, `topics`, `partitions` |
| `sample_size` | Messages per partition (default 200, max 2000) |
| `from` | `latest` (default), `earliest`, or a time |

Returns `schema.fields[]` (`path`, `types` with counts, `coverage` from 0 to 1, up to 3 `examples`), `formats` (decoders seen for keys and values), and `filter_suggestions`. A `string(json)` type marks strings holding JSON documents; open them with `parse_json()`. Examples respect redaction.

### `validate_filter`

| Argument | Description |
|---|---|
| `filter`, `grep`, `group_by` | At least one |
| `cluster`, `topics` | Optional: evaluate against the newest messages |
| `sample_size` | Messages per partition (default 50, max 500) |

Compile errors return `{"valid": false, "error": "..."}` with the position of the problem. With a target, `sample` reports `evaluated`, `matched`, `filter_errors`, `filter_error_samples`, `matched_examples` (coordinates), and distinct `group_examples`.

### `scan_messages`

[Common scan arguments](#common-scan-arguments). Returns up to `limit` matches, ordered by timestamp. Scans that outlast `wait_seconds` (default 20) continue in the background and return a `job_id`.

- **Paging:** pass `stats.next_offsets` back as `resume_offsets`. Pages never skip a match; a page may repeat a few matches from the previous one.
- **`follow: true`** tails new messages until `limit` or `timeout_seconds`.
- **`export: true`** writes every match (redacted, untruncated) to `<state_dir>/exports/<job_id>.jsonl`, up to `max_export_records`; `limit` then caps only the inline results.
- **`dry_run: true`** returns only the plan: partitions, offset ranges, and `records_in_range`.

### `count_messages`

Common scan arguments, plus:

| Argument | Description |
|---|---|
| `group_by` | CEL expression; omit to count everything |
| `top` | Groups returned (default 50) |
| `samples` | Example matches returned (default 3; `0` for none) |

Returns `counts`: `total`, `distinct_groups`, `groups[] {key, count}`, `other_groups_count`, `samples`.

## Jobs

Jobs live in server memory. Finished jobs are kept for `defaults.jobs.ttl` (1h), and at most `defaults.jobs.max_concurrent` (4) run at once.

### `get_job`

| Argument | Description |
|---|---|
| `job_id` | |
| `wait_seconds` | Wait for completion (max 50) |
| `offset`, `limit` | Page through `matches` (max 500 per call) |

While running, the result carries `progress` (live stats); once finished, it is the full scan result.

### `list_jobs`

No arguments. Recent jobs, newest first, with status and progress.

### `cancel_job`

| Argument | Description |
|---|---|
| `job_id` | |

Stops the job. Matches found so far and `next_offsets` stay available through `get_job`.

## Saved scans

Stored in `<state_dir>/saved-scans.json` and rewritten atomically.

### `save_scan`

| Argument | Description |
|---|---|
| `name` | Letters, digits, `_ . -`; up to 64 characters |
| `description` | |
| `kind` | `scan` (default) or `count` |
| `spec` | The arguments of `scan_messages` or `count_messages` |

The definition is validated against the cluster (topics, filter, grep, group_by) before it is saved.

### `list_saved_scans`

No arguments.

### `run_saved_scan`

| Argument | Description |
|---|---|
| `name` | |
| `from`, `to` | Override the window (replaces saved offsets) |
| `resume_offsets`, `limit`, `timeout_seconds`, `wait_seconds`, `export`, `dry_run` | Overrides |

### `delete_saved_scan`

| Argument | Description |
|---|---|
| `name` | |

## Schema Registry

Available for clusters with a `schema_registry` block.

### `list_schema_subjects`

| Argument | Description |
|---|---|
| `cluster` | |
| `pattern` | Glob, e.g. `orders-*` |

### `get_schema`

| Argument | Description |
|---|---|
| `cluster` | |
| `subject`, `version` | Version defaults to latest |
| `id` | Schema ID (decoded messages show it); overrides `subject` |

Returns the type, schema text, references, and the subject's versions.

## Writes

### `produce_message`

Registered only when at least one cluster has `access: read_write`; refused on read-only clusters.

| Argument | Description |
|---|---|
| `cluster`, `topic` | |
| `key` | Omit for a null key |
| `value` | Omit for a null value (tombstone) |
| `value_base64` | Treat `value` as base64-encoded binary |
| `headers` | String map |
| `partition` | Explicit partition. Default: murmur2 hash of the key (Java-compatible), or random for null keys. |

Returns `topic`, `partition`, `offset`, and `timestamp`.
