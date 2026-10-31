package server

import (
	"context"

	"github.com/Midwayne/kafka-mcp/internal/app"
)

func jobTools(svc *app.Service) []toolDef {
	return []toolDef{
		define(app.ToolGetJob,
			"Get a background scan's progress, or its results once finished (matches are pageable with offset/limit). Optionally wait for it to finish.",
			manages, func(ctx context.Context, args app.GetJobSpec) (any, error) { return svc.GetJob(ctx, args) }),

		define(app.ToolListJobs,
			"List recent background jobs with status and progress.",
			manages, func(context.Context, none) (any, error) { return svc.ListJobs(), nil }),

		define(app.ToolCancelJob,
			"Stop a running job. Results gathered so far stay available through get_job.",
			manages, func(ctx context.Context, args app.JobIDSpec) (any, error) { return svc.CancelJob(ctx, args) }),
	}
}

func savedScanTools(svc *app.Service) []toolDef {
	return []toolDef{
		define(app.ToolSaveScan,
			"Save a scan_messages or count_messages definition under a name so it can be re-run later with run_saved_scan. The definition is validated (cluster, topics, filter, grep, group_by) before saving.",
			manages, func(ctx context.Context, args app.SaveScanSpec) (any, error) { return svc.SaveScan(ctx, args) }),

		define(app.ToolListSavedScans,
			"List saved scan definitions.",
			manages, func(context.Context, none) (any, error) { return svc.ListSavedScans(), nil }),

		define(app.ToolRunSavedScan,
			"Run a saved scan, optionally overriding from/to, resume_offsets, limit, timeout, export, or dry_run.",
			reads, func(ctx context.Context, args app.RunSavedSpec) (any, error) { return svc.RunSavedScan(ctx, args) }),

		define(app.ToolDeleteSavedScan,
			"Delete a saved scan definition.",
			manages, func(_ context.Context, args app.NameSpec) (any, error) { return svc.DeleteSavedScan(args) }),
	}
}

func registryTools(svc *app.Service) []toolDef {
	return []toolDef{
		define(app.ToolListSchemaSubjects,
			"List Schema Registry subjects for a cluster, optionally filtered by a glob.",
			reads, func(ctx context.Context, args app.SubjectsSpec) (any, error) {
				return svc.ListSchemaSubjects(ctx, args)
			}),

		define(app.ToolGetSchema,
			"Get a registered schema (Avro, Protobuf, or JSON Schema) by subject and version, or by schema ID.",
			reads, func(ctx context.Context, args app.GetSchemaSpec) (any, error) { return svc.GetSchema(ctx, args) }),
	}
}

func writeTools(svc *app.Service) []toolDef {
	return []toolDef{
		define(app.ToolProduceMessage,
			"Write one message to a topic. Only available on clusters configured with access: read_write.",
			writes, func(ctx context.Context, args app.ProduceSpec) (any, error) { return svc.ProduceMessage(ctx, args) }),
	}
}
