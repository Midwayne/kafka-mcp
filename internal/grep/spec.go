// Package grep implements grep-style matching over Kafka messages: regex or
// fixed-string patterns, case folding, whole words, inversion, any/all
// semantics, and scopes from raw bytes to decoded fields.
package grep

import (
	"errors"
	"fmt"
)

// Limits on agent-supplied patterns.
const (
	MaxPatterns      = 32
	MaxPatternLength = 1024
)

// Spec is a grep query. Patterns are RE2 regular expressions unless FixedStrings is set.
type Spec struct {
	Patterns     []string `json:"patterns" jsonschema:"Patterns to search for (RE2 regex syntax unless fixed_strings). A message matches if any pattern matches (all=true requires every pattern)."`
	FixedStrings bool     `json:"fixed_strings,omitempty" jsonschema:"Treat patterns as literal strings (like grep -F). Fastest option."`
	IgnoreCase   bool     `json:"ignore_case,omitempty" jsonschema:"Case-insensitive matching (like grep -i)."`
	Word         bool     `json:"word,omitempty" jsonschema:"Match whole words only (like grep -w)."`
	All          bool     `json:"all,omitempty" jsonschema:"Require every pattern to match somewhere in the scopes (default: any pattern)."`
	Invert       bool     `json:"invert,omitempty" jsonschema:"Select messages that do NOT match (like grep -v)."`
	In           []string `json:"in,omitempty" jsonschema:"Where to search. Raw bytes (fast): key, value, headers. Decoded text (Avro/Protobuf/JSON rendered as JSON): decoded_key, decoded_value. Single fields: value.<path>, key.<path>, headers.<name>. Default: key, value, headers."`
	OnlyMatching bool     `json:"only_matching,omitempty" jsonschema:"Return only message coordinates plus matched snippets instead of full messages (like grep -o). Saves tokens."`
}

// Matcher evaluates a compiled Spec. It is safe for concurrent use.
type Matcher struct {
	patterns []pattern
	scopes   []scope
	all      bool
	invert   bool
}

// Compile validates and compiles a spec.
func Compile(spec Spec) (*Matcher, error) {
	if len(spec.Patterns) == 0 {
		return nil, errors.New("grep needs at least one pattern")
	}

	if len(spec.Patterns) > MaxPatterns {
		return nil, fmt.Errorf("grep supports at most %d patterns", MaxPatterns)
	}

	matcher := &Matcher{all: spec.All, invert: spec.Invert}

	for _, raw := range spec.Patterns {
		compiled, err := compilePattern(raw, spec)
		if err != nil {
			return nil, err
		}

		matcher.patterns = append(matcher.patterns, compiled)
	}

	names := spec.In
	if len(names) == 0 {
		names = DefaultScopes
	}

	for _, name := range names {
		compiled, err := parseScope(name)
		if err != nil {
			return nil, err
		}

		matcher.scopes = append(matcher.scopes, compiled)
	}

	return matcher, nil
}
