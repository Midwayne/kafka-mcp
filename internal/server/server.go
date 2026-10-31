package server

import (
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/Midwayne/kafka-mcp/internal/app"
)

// Instructions are sent to MCP clients to guide the agent.
const Instructions = `Kafka MCP reads (and, where allowed, writes) Kafka clusters by name.

Workflow:
1. list_clusters, then list_topics / describe_topic to find topics and judge size and freshness.
2. infer_schema (or sample_messages) to learn field paths and decoders.
3. validate_filter to check expressions against real messages.
4. scan_messages to find messages, or count_messages to aggregate. Scans default to the last hour; set from/to, start_offset, or follow.
5. Results include hints. Page with resume_offsets = stats.next_offsets. Long scans return a job_id for get_job.
6. get_messages shows a hit in full with surrounding messages. save_scan turns a useful scan into a reusable check.

Selecting messages (combine freely; cheapest checks run first):
- key (+ key_partitioner to read one partition), headers: exact matches before decoding.
- grep: patterns (RE2 regex or fixed_strings), ignore_case, word, invert, all, scoped with in: key, value, headers, decoded_value, value.<path>, headers.<name>. only_matching returns just snippets.
- filter: CEL over key, value, headers, topic, partition, offset, ts, timestamp, raw_key, raw_value. Guard optional fields with has() or value.?field.orValue(x). parse_json() opens JSON stored in strings.

Keep outputs small: use fields, only_matching, count_messages, and limit. Redacted fields show [REDACTED].`

// New registers every enabled tool on a new MCP server.
func New(svc *app.Service, name, version string) *mcpserver.MCPServer {
	server := mcpserver.NewMCPServer(name, version,
		mcpserver.WithInstructions(Instructions),
		mcpserver.WithToolCapabilities(false),
		mcpserver.WithRecovery(),
	)

	defs := catalog(svc)
	for i := range defs {
		server.AddTool(defs[i].tool, defs[i].handler)
	}

	return server
}

// catalog returns enabled tools: write tools appear only if some cluster is
// read_write, and the global tools include/exclude filter applies.
func catalog(svc *app.Service) []toolDef {
	groups := [][]toolDef{exploreTools(svc), messageTools(svc), jobTools(svc), savedScanTools(svc), registryTools(svc)}
	if svc.AnyWritable() {
		groups = append(groups, writeTools(svc))
	}

	filter := svc.Config().Tools

	var enabled []toolDef
	for _, group := range groups {
		for i := range group {
			if filter.Allows(group[i].tool.Name) {
				enabled = append(enabled, group[i])
			}
		}
	}

	return enabled
}
