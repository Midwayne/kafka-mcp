package server

import (
	"context"

	"github.com/Midwayne/kafka-mcp/internal/app"
)

func messageTools(svc *app.Service) []toolDef {
	return []toolDef{
		define(app.ToolSampleMessages,
			"Return a few decoded messages (default the newest 5) to see what a topic contains. Use infer_schema for structure across many messages.",
			reads, func(ctx context.Context, args app.SampleSpec) (any, error) { return svc.Sample(ctx, args) }),

		define(app.ToolGetMessages,
			"Fetch one message by partition and offset, optionally with surrounding messages (before/after/context, like grep -B/-A/-C). Use to inspect a scan hit in full.",
			reads, func(ctx context.Context, args app.GetMessagesSpec) (any, error) { return svc.GetMessages(ctx, args) }),

		define(app.ToolInferSchema,
			"Summarize the structure of recent messages without returning payloads: every field path (value.a.b, value.items[].sku, headers.x) with types, how often it is present, and example values, plus detected decoders and starter filter expressions. Do this before writing filters.",
			reads, func(ctx context.Context, args app.InferSpec) (any, error) { return svc.InferSchema(ctx, args) }),

		define(app.ToolValidateFilter,
			"Check a CEL filter, grep spec, or group_by expression. Compile errors are returned as valid=false with the message. With cluster and topics, also evaluates against the newest messages and reports match and error counts.",
			reads, func(ctx context.Context, args app.ValidateSpec) (any, error) { return svc.ValidateFilter(ctx, args) }),

		define(app.ToolScanMessages,
			"Search topics for messages matching any combination of: time or offset range, exact key, header values, grep patterns (regex/fixed, ignore_case, word, invert, any/all, scoped to raw bytes, decoded payloads, or single fields), and a CEL filter. "+
				"Reads partitions in parallel within budgets and returns up to limit matches (or only matched snippets with grep.only_matching). "+
				"Long scans continue as background jobs (poll with get_job). Use stats.next_offsets as resume_offsets to page. Default window: the last hour.",
			reads, func(ctx context.Context, args app.ScanSpec) (any, error) { return svc.Scan(ctx, args) }),

		define(app.ToolCountMessages,
			"Count matching messages (same selection options as scan_messages), optionally grouped by a CEL expression such as value.status or headers['type'], returning the top groups and a few samples. Use for 'how many' and distribution questions without pulling messages.",
			reads, func(ctx context.Context, args app.CountSpec) (any, error) { return svc.Count(ctx, args) }),
	}
}
