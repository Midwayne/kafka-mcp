# Filtering and searching

`scan_messages`, `count_messages`, and `validate_filter` share one selection model. Use any combination; a message must satisfy all of them.

- [Evaluation order (cheapest first)](#evaluation-order-cheapest-first)
- [Choosing what to read](#choosing-what-to-read)
- [grep](#grep)
- [CEL filters](#cel-filters)
- [group_by](#group_by)
- [Field paths and output shaping](#field-paths-and-output-shaping)
- [Recommended agent workflow](#recommended-agent-workflow)

## Evaluation order (cheapest first)

```text
time/offset range  ->  partitions (key_partitioner)  ->  timestamp window
  ->  exact key and headers  ->  grep  ->  CEL filter  ->  result
```

1. **Range.** `from`/`to` are converted to offsets with Kafka's timestamp index, so records outside the window are never fetched.
2. **Partitions.** `partitions`, or `key` plus `key_partitioner`, which reads only the partition the producer would have chosen for that key.
3. **Timestamp window.** Records inside the offset range but with out-of-order timestamps are dropped.
4. **`key` and `headers`.** Exact byte comparisons, no decoding.
5. **`grep`.** Raw-bytes scopes need no decoding.
6. **`filter`.** CEL; values are decoded only when the expression reaches them.

Measured per message on one core: grep on raw bytes ~0.2 µs, a header-only CEL filter ~0.3 µs, CEL over a decoded JSON value ~2 µs.

## Choosing what to read

| Argument | Example | Notes |
|---|---|---|
| `from` | `-15m`, `-2h`, `-7d`, `3h ago`, `2026-05-01T10:00:00Z`, `2026-05-01`, `1714557600`, `earliest`, `latest` | Default `-1h` for scans; `latest` when `follow` is set |
| `to` | Same formats | Exclusive. Default: each partition's end when the scan starts |
| `start_offset` / `end_offset` | `0`, `120000`, `-100` | Per partition; negative counts back from the end (`-100` = the last 100) |
| `partitions` | `[0, 3]` | |
| `key` | `"order-123"` | Exact raw key |
| `key_partitioner` | `murmur2` / `java`, `crc32` / `librdkafka`, `fnv1a` / `sarama` | Use the producer's partitioner; Java, Kafka Streams, Spring, and franz-go default to `murmur2` |
| `headers` | `{"event_type": "ORDER_ADD"}` | All must match exactly |
| `resume_offsets` | a previous `stats.next_offsets` | Continue where a scan stopped |
| `follow` | `true` | Keep reading new messages until `limit` or `timeout_seconds` |

## grep

```json
{
  "grep": {
    "patterns": ["timeout", "ERR-\\d{3}"],
    "ignore_case": true,
    "in": ["value", "headers"],
    "only_matching": true
  }
}
```

| Option | grep equivalent | Meaning |
|---|---|---|
| `patterns` | `-e` | RE2 regular expressions (linear time, so no catastrophic backtracking); up to 32 |
| `fixed_strings` | `-F` | Literal text; the fastest option |
| `ignore_case` | `-i` | Case-insensitive |
| `word` | `-w` | Whole words only |
| `all` | | Every pattern must match somewhere (default: any) |
| `invert` | `-v` | Select messages that do **not** match |
| `only_matching` | `-o` | Return coordinates plus matched snippets instead of whole messages |
| `in` | | Where to search (below) |

**Scopes (`in`)**

| Scope | Searches | Cost |
|---|---|---|
| `key`, `value` | Raw bytes, before any decoding | Cheapest |
| `headers` | `name: value` lines | Cheap |
| `decoded_key`, `decoded_value` | The decoded payload rendered as JSON; needed for Avro or Protobuf | Decodes |
| `value.<path>`, `key.<path>` | One decoded field; objects and arrays are rendered as JSON | Decodes |
| `headers.<name>` | One header value | Cheap |

The default is `["key", "value", "headers"]`, which is like grepping the whole message. With `only_matching`, snippets come from the **redacted** view, so a pattern can match a masked field without revealing it.

Examples:

```json
{"patterns": ["^R[A-Z]{3}$"], "in": ["value.data.carrier_scac"], "only_matching": true}
{"patterns": ["INFO", "DEBUG"], "invert": true, "in": ["value.level"]}
{"patterns": ["order-42", "refund"], "all": true, "fixed_strings": true}
```

## CEL filters

[CEL](https://cel.dev) is a small, safe, side-effect-free expression language. Variables:

| Variable | Type | Notes |
|---|---|---|
| `key`, `value` | dynamic | Decoded payloads (maps, lists, strings, numbers) |
| `headers` | `map<string,string>` | For repeated header names, the last value wins |
| `topic` | string | |
| `partition`, `offset` | int | |
| `ts` | int | Record timestamp in Unix milliseconds |
| `timestamp` | timestamp | Use with `timestamp("...")`, `duration("...")`, `.getHours()` |
| `raw_key`, `raw_value` | string | Undecoded bytes as text |

Integers and floats compare naturally (`value.amount > 9.5` works on int fields), and 64-bit IDs stay exact.

### Cookbook

```cel
value.status == 'FAILED'
value.status in ['FAILED', 'REJECTED'] && value.amount > 1000
has(value.error)                                   // field present
value.?data.?carton_count.orValue(0) > 200         // optional chain: no error if missing
headers['event_type'] == 'ORDER_ADD'
headers.exists(h, h.startsWith('x-retry'))
key.startsWith('store-1375')
value.items.exists(i, i.qty > 5)                   // any element
value.items.all(i, i.sku != '')                    // every element
size(value.items) > 10
value.email.matches('^[^@]+@example\\.com$')       // RE2 regex
value.reason.lowerAscii().contains('timeout')
parse_json(value.payload).customer.id == 42        // JSON stored inside a string
timestamp > timestamp('2026-05-01T00:00:00Z')
timestamp.getHours() >= 22                         // late-night traffic (UTC)
int(value.big_id) > 1000                           // protobuf int64 fields arrive as strings
raw_value.contains('"retry":true')
```

Available extensions: strings (`lowerAscii`, `split`, `replace`, `trim`, `format`), math (`math.greatest`, `math.abs`), lists (`sort`, `distinct`, `flatten`), sets (`sets.contains`), regex (`regex.extract`), encoders (`base64.decode`), optional types (`?.`, `orValue`), and `parse_json`.

### Missing fields

`value.x == 1` raises an error when `x` is absent; that message counts as a non-match, and the scan reports `filter_errors` with samples. In topics that mix event types, guard fields:

```cel
has(value.x) && value.x == 1
value.?x.orValue(0) == 1
```

`validate_filter` with `cluster` and `topics` shows how many recent messages would raise errors before you run a long scan.

## group_by

`count_messages` groups by any CEL expression. The result is rendered as a string.

```cel
value.status
headers['event_type']
[value.store, value.status]          // composite key
timestamp.getHours()                 // hour of day (UTC)
value.amount > 1000                  // true or false buckets
value.?region.orValue('unknown')
```

Up to 10,000 distinct groups are tracked; the rest are counted in `other_groups_count`. `top` sets how many groups are returned.

## Field paths and output shaping

Paths are dot-separated, with numeric segments for list indexes: `value.items.0.sku`, `headers.trace-id`, `key`.

| Argument | Effect |
|---|---|
| `fields` | Return only these paths plus topic, partition, offset, and timestamp |
| `max_value_chars` | Truncate long strings (default 1000) |
| `grep.only_matching` | Coordinates plus matched snippets |
| `limit` | Maximum matches |
| `key_format` / `value_format` | Override decoding: `auto`, `json`, `string`, `base64`, `hex`, `schema_registry`, `none` |

Arrays longer than 50 items are truncated in results; exports keep every value. Redacted paths are always masked.

## Recommended agent workflow

1. `describe_topic` to see size, retention, and freshness, and choose `from`/`to`.
2. `infer_schema` to learn field paths. It also suggests starter filters and flags string fields that hold JSON.
3. `validate_filter` against recent messages to check error and match rates.
4. `count_messages` first for "how many" or "which kinds" questions.
5. `scan_messages` with `fields` or `only_matching` and a modest `limit`; page with `resume_offsets`.
6. `get_messages` with `context` to read a hit in full.
7. `save_scan` when the query should be repeatable.
