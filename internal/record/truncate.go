package record

import (
	"fmt"
	"unicode/utf8"
)

// Truncate returns a copy of value with long strings and arrays shortened.
// maxChars <= 0 leaves strings intact; maxItems <= 0 leaves arrays intact.
func Truncate(value any, maxChars, maxItems int) any {
	switch typed := value.(type) {
	case string:
		return TruncateString(typed, maxChars)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = Truncate(item, maxChars, maxItems)
		}
		return out
	case []any:
		return truncateSlice(typed, maxChars, maxItems)
	default:
		return value
	}
}

// TruncateString shortens s to maxChars runes, noting how many were removed.
func TruncateString(s string, maxChars int) string {
	if maxChars <= 0 || len(s) <= maxChars {
		return s
	}

	runes := utf8.RuneCountInString(s)
	if runes <= maxChars {
		return s
	}

	return string([]rune(s)[:maxChars]) + fmt.Sprintf("…(+%d chars)", runes-maxChars)
}

func truncateSlice(items []any, maxChars, maxItems int) []any {
	keep := len(items)
	if maxItems > 0 && keep > maxItems {
		keep = maxItems
	}

	out := make([]any, 0, keep+1)
	for _, item := range items[:keep] {
		out = append(out, Truncate(item, maxChars, maxItems))
	}

	if keep < len(items) {
		out = append(out, fmt.Sprintf("…(+%d items)", len(items)-keep))
	}

	return out
}
