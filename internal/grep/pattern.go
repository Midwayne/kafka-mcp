package grep

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
)

// pattern matches one compiled grep pattern against text.
type pattern interface {
	match(text []byte) bool
	findAll(text []byte, limit int) [][]byte
}

// literal is the fast path for case-sensitive fixed strings.
type literal []byte

func (l literal) match(text []byte) bool { return bytes.Contains(text, l) }

func (l literal) findAll(text []byte, limit int) [][]byte {
	var hits [][]byte

	for len(hits) < limit {
		index := bytes.Index(text, l)
		if index < 0 {
			break
		}

		hits = append(hits, text[index:index+len(l)])
		text = text[index+len(l):]
	}

	return hits
}

// expression wraps an RE2 regexp, which runs in linear time on any input.
type expression struct{ re *regexp.Regexp }

func (e expression) match(text []byte) bool { return e.re.Match(text) }

func (e expression) findAll(text []byte, limit int) [][]byte { return e.re.FindAll(text, limit) }

func compilePattern(raw string, spec Spec) (pattern, error) {
	if raw == "" {
		return nil, errors.New("grep pattern must not be empty")
	}

	if len(raw) > MaxPatternLength {
		return nil, fmt.Errorf("grep pattern exceeds %d characters", MaxPatternLength)
	}

	if spec.FixedStrings && !spec.IgnoreCase && !spec.Word {
		return literal(raw), nil
	}

	source := raw
	if spec.FixedStrings {
		source = regexp.QuoteMeta(raw)
	}

	if spec.Word {
		source = `\b(?:` + source + `)\b`
	}

	if spec.IgnoreCase {
		source = `(?i)` + source
	}

	re, err := regexp.Compile(source)
	if err != nil {
		return nil, fmt.Errorf("invalid grep pattern %q: %w", raw, err)
	}

	return expression{re: re}, nil
}
