// Package server exposes the application service as MCP tools.
package server

import (
	"context"
	"encoding/json"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/Midwayne/kafka-mcp/internal/app"
)

// effect classifies what a tool does, for MCP annotations and registration.
type effect int

const (
	reads   effect = iota // reads cluster metadata or messages
	manages               // changes local server state (jobs, saved scans) only
	writes                // writes to Kafka; needs a read_write cluster
)

// toolDef is a tool plus its handler.
type toolDef struct {
	tool    mcp.Tool
	handler mcpserver.ToolHandlerFunc
	effect  effect
}

// define builds a tool whose input schema is generated from T. Errors become
// tool errors (visible to the agent) and results are compact JSON.
func define[T any](name, description string, kind effect, run func(context.Context, T) (any, error)) toolDef {
	tool := mcp.NewTool(name,
		mcp.WithDescription(description),
		mcp.WithInputSchema[T](),
		mcp.WithReadOnlyHintAnnotation(kind == reads),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(kind == reads),
		mcp.WithOpenWorldHintAnnotation(kind != manages),
	)

	handler := mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, args T) (*mcp.CallToolResult, error) {
		result, err := run(ctx, args)
		if err != nil {
			return mcp.NewToolResultError(app.Explain(err)), nil
		}

		return jsonResult(result), nil
	})

	return toolDef{tool: tool, handler: handler, effect: kind}
}

func jsonResult(value any) *mcp.CallToolResult {
	encoded, err := json.Marshal(value)
	if err != nil {
		return mcp.NewToolResultError("encode result: " + err.Error())
	}

	return mcp.NewToolResultText(string(encoded))
}

// none is the argument type for tools without parameters.
type none struct{}
