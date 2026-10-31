package infer

import (
	"fmt"
	"strings"

	"github.com/Midwayne/kafka-mcp/internal/codec"
)

// Type names reported in Field.Types.
const (
	TypeObject     = "object"
	TypeArray      = "array"
	TypeString     = "string"
	TypeJSONString = "string(json)"
	TypeInt        = "int"
	TypeFloat      = "float"
	TypeBool       = "bool"
	TypeNull       = "null"
)

const minJSONStringSize = 2

func typeName(value any) string {
	switch typed := value.(type) {
	case nil:
		return TypeNull
	case map[string]any:
		return TypeObject
	case []any:
		return TypeArray
	case bool:
		return TypeBool
	case int64:
		return TypeInt
	case float64:
		return TypeFloat
	case string:
		return stringType(typed)
	default:
		return fmt.Sprintf("%T", value)
	}
}

// stringType flags strings holding JSON documents, which filters can open with parse_json().
func stringType(s string) string {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) < minJSONStringSize || (trimmed[0] != '{' && trimmed[0] != '[') {
		return TypeString
	}

	if _, err := codec.DecodeJSON([]byte(trimmed)); err != nil {
		return TypeString
	}

	return TypeJSONString
}
