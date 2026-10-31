package record

import (
	"strconv"
	"strings"
)

// Wildcard matches every key of an object or every element of an array.
const Wildcard = "*"

// RedactedValue replaces redacted fields in output.
const RedactedValue = "[REDACTED]"

// Lookup resolves a dot path such as "value.items.0.sku" against nested maps and slices.
func Lookup(root any, path string) (any, bool) {
	current := root

	for _, segment := range strings.Split(path, ".") {
		next, ok := child(current, segment)
		if !ok {
			return nil, false
		}

		current = next
	}

	return current, true
}

// Redact replaces every value matched by path with RedactedValue.
// Segments may be "*" to match all keys or elements at that level.
func Redact(root any, path string) {
	redact(root, strings.Split(path, "."))
}

func redact(node any, segments []string) {
	if len(segments) == 0 {
		return
	}

	switch typed := node.(type) {
	case map[string]any:
		for key, child := range typed {
			if matchesSegment(segments[0], key) {
				typed[key] = redactChild(child, segments)
			}
		}
	case []any:
		for i, child := range typed {
			if matchesSegment(segments[0], strconv.Itoa(i)) {
				typed[i] = redactChild(child, segments)
			}
		}
	}
}

// redactChild masks child when segments is exhausted, otherwise recurses into it.
func redactChild(child any, segments []string) any {
	if len(segments) == 1 {
		return RedactedValue
	}

	redact(child, segments[1:])

	return child
}

func matchesSegment(segment, name string) bool {
	return segment == Wildcard || segment == name
}

func child(node any, segment string) (any, bool) {
	switch typed := node.(type) {
	case map[string]any:
		value, ok := typed[segment]
		return value, ok
	case []any:
		index, err := strconv.Atoi(segment)
		if err != nil || index < 0 || index >= len(typed) {
			return nil, false
		}

		return typed[index], true
	default:
		return nil, false
	}
}
