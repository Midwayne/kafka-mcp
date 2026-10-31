# Architecture

kafka-mcp is a single Go binary. Packages depend downward only, so each layer can be tested on its own.

```text
main.go, check.go            CLI: flags, -check, serve
internal/server              MCP layer: tool catalog, typed handlers, transports, bearer auth
internal/app                 Use cases: validation, access rules, planning, jobs, hints, error advice
 ├─ internal/scan            Planner and parallel scanner (budgets, resume offsets, follow)
 ├─ internal/sink            Result collectors: bounded matches, JSON Lines export, grouped counts
 ├─ internal/grep            grep-style matcher and snippet renderer
 ├─ internal/filter          CEL environment, predicates, group-by expressions
 ├─ internal/infer           Schema inference from rendered messages
 ├─ internal/jobs            Background jobs: progress, wait, cancel, TTL
 ├─ internal/saved           Persistent saved-scan store
 ├─ internal/record          Lazily decoded messages; projection, redaction, truncation
 ├─ internal/codec           Payload decoders (JSON, text, binary, Schema Registry dispatch)
 ├─ internal/schemaregistry  Registry client, per-ID decoder cache, Avro/Protobuf/JSON Schema
 ├─ internal/kafka           franz-go clients per cluster: admin, group-less consumers, producer
 ├─ internal/auth            TLS (PEM/JKS/PKCS#12) and SASL mechanism builders
 └─ internal/config          Schema, placeholder expansion, defaults, validation
internal/testutil            Test-only fixtures: throwaway PKI, keystores, in-memory Kafka
```

## A scan, end to end

1. **server** binds the JSON arguments to `app.ScanSpec`, whose schema is generated from the struct, and calls `Service.Scan`.
2. **app.prepare** checks the cluster and tool permissions, expands topic globs against the allow-list, compiles grep and CEL, validates decoder overrides, and clamps budgets.
3. **scan.BuildPlan** lists start, end, and time-based offsets and produces per-partition `[start, end)` ranges plus a timestamp window. Key-based pruning happens here.
4. **jobs.Manager** runs the scan detached from the tool call, bounded by the timeout. The call waits up to `wait_seconds`.
5. **scan.Scanner** opens a group-less consumer for the exact ranges.
   - The poll loop clips records at the range end, applies the record and byte budgets, tracks positions, and stops fetching finished partitions.
   - Workers apply the prefilter (timestamp, key, headers), then each matcher in order (grep, then CEL), then offer matches to the sink.
6. **record.Message** decodes the key and value only when first accessed, which is usually inside a CEL activation, and caches the result.
7. **sink** renders matches (redact, project, truncate), streams exports, or aggregates counts. When the sink is full, it halts the scan; workers record the first undelivered offset per partition so `next_offsets` never skips a record.
8. **app.hints** turns the stats into next-step advice; **app.Explain** adds remedies to errors.

## Design decisions

| Decision | Why |
|---|---|
| Group-less consumers with explicit offsets | No rebalances and no commits: zero interference with production consumer groups |
| `KeepControlRecords` | Transaction markers count as progress, so ranges that end in a commit marker complete immediately |
| Lazy decoding through a CEL `Activation` | `headers[...] == x && value.y == z` decodes the value only for messages that pass the header test |
| Matchers ordered cheapest first | Raw-bytes grep runs before CEL that decodes |
| Expand placeholders in the YAML node tree | Comments are ignored, and secrets with YAML syntax stay literal |
| Content-sniffed keystores | `keytool` writes PKCS#12 into `.jks` files; Java detects this, and so does kafka-mcp |
| `verify_hostname: false` re-implements chain checks | Mirrors Java's empty endpoint identification without disabling verification |
| Sinks report `accepted` and `more` separately | Exact limits, plus gap-free resume when results are dropped |
| Idle stop only after every open partition has served data | Slow first fetches (cold or tiered storage) are not mistaken for drained partitions |
| Kafka-default fetch sizes | Responsive progress and limits on slow links |
| Tool schemas generated from Go structs | One source of truth for arguments, validation, and documentation |
| Tests check the docs | Example configs must load, CEL cookbook entries must compile, and every tool must be documented |

## Extending

- **New tool.** Add a method on `app.Service` with a spec struct (use `jsonschema` tags for descriptions), register it with `define(...)` in an `internal/server/tools_*.go` file, add its name to `app/tools.go`, and document it in `docs/tools.md` (a test enforces this).
- **New SASL mechanism.** Add a builder to `mechanismBuilders` in `internal/auth/sasl.go` and an alias in `internal/config/security.go`.
- **New payload format.** Add a case in `codec.Decoder.Decode` and a name in `config.NormalizeFormat`.
- **New Schema Registry type.** Implement `payloadDecoder` in `internal/schemaregistry` and dispatch it in `cache.go`.
- **New result type.** Implement `scan.Sink` (plus `fill` for `app.collector`).
