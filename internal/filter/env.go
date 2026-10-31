// Package filter compiles CEL expressions that agents use to select and group messages.
package filter

import (
	"sync"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"cel.dev/cel-go/ext"

	"github.com/Midwayne/kafka-mcp/internal/codec"
)

// Variable names available to expressions.
const (
	VarKey       = "key"
	VarValue     = "value"
	VarHeaders   = "headers"
	VarTopic     = "topic"
	VarPartition = "partition"
	VarOffset    = "offset"
	VarTS        = "ts"
	VarTimestamp = "timestamp"
	VarRawKey    = "raw_key"
	VarRawValue  = "raw_value"
)

// environment returns the shared CEL environment, built once. cel.Env is safe for concurrent use.
var environment = sync.OnceValues(func() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable(VarKey, cel.DynType),
		cel.Variable(VarValue, cel.DynType),
		cel.Variable(VarHeaders, cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable(VarTopic, cel.StringType),
		cel.Variable(VarPartition, cel.IntType),
		cel.Variable(VarOffset, cel.IntType),
		cel.Variable(VarTS, cel.IntType),
		cel.Variable(VarTimestamp, cel.TimestampType),
		cel.Variable(VarRawKey, cel.StringType),
		cel.Variable(VarRawValue, cel.StringType),
		cel.OptionalTypes(),
		cel.CrossTypeNumericComparisons(true),
		cel.DefaultUTCTimeZone(true),
		cel.EnableIdentifierEscapeSyntax(),
		ext.Strings(),
		ext.Encoders(),
		ext.Math(),
		ext.Lists(),
		ext.Sets(),
		ext.Regex(),
		parseJSONFunction(),
	)
})

// parseJSONFunction exposes parse_json(string) for JSON documents embedded in string fields.
func parseJSONFunction() cel.EnvOption {
	return cel.Function("parse_json",
		cel.Overload("parse_json_string", []*cel.Type{cel.StringType}, cel.DynType,
			cel.UnaryBinding(func(arg ref.Val) ref.Val {
				text, ok := arg.Value().(string)
				if !ok {
					return types.NewErr("parse_json expects a string")
				}

				parsed, err := codec.DecodeJSON([]byte(text))
				if err != nil {
					return types.NewErr("parse_json: %v", err)
				}

				return types.DefaultTypeAdapter.NativeToValue(parsed)
			}),
		),
	)
}
