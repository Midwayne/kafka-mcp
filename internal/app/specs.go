package app

import "github.com/Midwayne/kafka-mcp/internal/grep"

// Target selects a cluster and topics.
type Target struct {
	Cluster    string   `json:"cluster" jsonschema:"Cluster name from list_clusters."`
	Topics     []string `json:"topics" jsonschema:"Topics to read. Glob patterns such as orders.* expand to matching topics (max 50)."`
	Partitions []int32  `json:"partitions,omitempty" jsonschema:"Only read these partitions. Default: all."`
}

// Window selects where reading starts and stops.
type Window struct {
	From          string                      `json:"from,omitempty" jsonschema:"Start: earliest, latest, RFC 3339 time, Unix seconds/ms, or relative like -15m, -2h, -7d. Default -1h for scans (latest when follow=true)."`
	To            string                      `json:"to,omitempty" jsonschema:"End (exclusive), same formats as from. Default: the end of each partition when the scan starts."`
	StartOffset   *int64                      `json:"start_offset,omitempty" jsonschema:"Start offset per partition; negative counts back from the end (-100 = last 100). Overrides from."`
	EndOffset     *int64                      `json:"end_offset,omitempty" jsonschema:"End offset (exclusive) per partition; negative counts back from the end. Overrides to."`
	ResumeOffsets map[string]map[string]int64 `json:"resume_offsets,omitempty" jsonschema:"Continue a previous scan: pass its stats.next_offsets ({topic: {partition: offset}})."`
	Follow        bool                        `json:"follow,omitempty" jsonschema:"Keep reading new messages as they arrive (like tail -f) until limit or timeout."`
}

// Match selects messages. All given criteria must hold.
type Match struct {
	Filter         string            `json:"filter,omitempty" jsonschema:"CEL predicate. Variables: key, value (decoded), headers (map), topic, partition, offset, ts (Unix ms), timestamp, raw_key, raw_value. Examples: value.status == 'FAILED' && value.amount > 100; has(value.error); value.?a.?b.orValue('') == 'x'; headers['type'] == 'X'; value.items.exists(i, i.qty > 5); parse_json(value.payload).id == 7."`
	Grep           *grep.Spec        `json:"grep,omitempty" jsonschema:"grep-style matching (regex or fixed strings, ignore_case, word, invert, all/any, scopes). Evaluated before filter."`
	Key            *string           `json:"key,omitempty" jsonschema:"Exact record key (raw bytes as UTF-8). Cheap; combine with key_partitioner to read only that key's partition."`
	KeyPartitioner string            `json:"key_partitioner,omitempty" jsonschema:"With key: read only the partition the producer would choose. murmur2 (Java/Kafka Streams/franz-go), crc32 (librdkafka/confluent-kafka default), or fnv1a (Sarama)."`
	Headers        map[string]string `json:"headers,omitempty" jsonschema:"Exact header values that must all be present. Cheap, checked before decoding."`
}

// Output shapes returned messages.
type Output struct {
	Fields        []string `json:"fields,omitempty" jsonschema:"Only return these dot paths (e.g. value.order.id, headers.trace-id) plus topic/partition/offset/timestamp. Saves tokens."`
	MaxValueChars int      `json:"max_value_chars,omitempty" jsonschema:"Truncate longer strings in results. Default from server config."`
	KeyFormat     string   `json:"key_format,omitempty" jsonschema:"Override key decoding: auto, json, string, base64, hex, schema_registry (avro/protobuf/json_schema), none."`
	ValueFormat   string   `json:"value_format,omitempty" jsonschema:"Override value decoding (same options as key_format)."`
}

// Budget bounds the work a call may do.
type Budget struct {
	Limit          int   `json:"limit,omitempty" jsonschema:"Maximum matches to return. Default and maximum come from server config."`
	MaxScanned     int64 `json:"max_scanned,omitempty" jsonschema:"Stop after reading this many records (resume with next_offsets)."`
	MaxBytes       int64 `json:"max_bytes,omitempty" jsonschema:"Stop after reading this many key+value bytes."`
	TimeoutSeconds int   `json:"timeout_seconds,omitempty" jsonschema:"Total time budget for the scan."`
	WaitSeconds    *int  `json:"wait_seconds,omitempty" jsonschema:"How long this call waits for results (default 20). If the scan is still running, a job_id is returned; poll it with get_job. Use 0 to start in the background immediately."`
}

// ScanSpec is a complete scan request.
type ScanSpec struct {
	Target
	Window
	Match
	Output
	Budget
	Export bool `json:"export,omitempty" jsonschema:"Also write every match (redacted, untruncated) to a JSON Lines file under the server state directory, up to max_export_records; limit then only caps inline results."`
	DryRun bool `json:"dry_run,omitempty" jsonschema:"Only resolve the plan: partitions, offset ranges, and how many records would be read. Nothing is consumed."`
}

// CountSpec aggregates matches instead of returning them.
type CountSpec struct {
	ScanSpec
	GroupBy string `json:"group_by,omitempty" jsonschema:"CEL expression to group by, e.g. value.status, headers['type'], [value.store, value.status], timestamp.getHours(). Omit to count all matches."`
	Top     int    `json:"top,omitempty" jsonschema:"Return the top N groups (default 50)."`
	Samples *int   `json:"samples,omitempty" jsonschema:"Also return this many example matches (default 3; 0 for none)."`
}
