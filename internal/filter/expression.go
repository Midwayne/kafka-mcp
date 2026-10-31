package filter

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

// MaxExpressionLength bounds expressions supplied by agents.
const MaxExpressionLength = 4096

// Expression is a compiled CEL program. It is safe for concurrent use.
type Expression struct {
	source    string
	program   cel.Program
	variables []string
}

// CompilePredicate compiles an expression that must evaluate to a bool.
func CompilePredicate(source string) (*Expression, error) {
	return compile(source, func(t *cel.Type) bool {
		return t.IsExactType(cel.BoolType) || t.IsExactType(cel.DynType)
	})
}

// CompileValue compiles an expression of any type, e.g. a group-by key.
func CompileValue(source string) (*Expression, error) {
	return compile(source, func(*cel.Type) bool { return true })
}

func compile(source string, acceptOutput func(*cel.Type) bool) (*Expression, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, errors.New("expression is empty")
	}

	if len(source) > MaxExpressionLength {
		return nil, fmt.Errorf("expression exceeds %d characters", MaxExpressionLength)
	}

	env, err := environment()
	if err != nil {
		return nil, fmt.Errorf("cel environment: %w", err)
	}

	ast, issues := env.Compile(source)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("invalid expression: %w", issues.Err())
	}

	if !acceptOutput(ast.OutputType()) {
		return nil, fmt.Errorf("expression must return bool, got %s", ast.OutputType())
	}

	program, err := env.Program(ast, cel.EvalOptions(cel.OptOptimize))
	if err != nil {
		return nil, fmt.Errorf("plan expression: %w", err)
	}

	return &Expression{source: source, program: program, variables: referencedVariables(ast)}, nil
}

// Source returns the expression text.
func (e *Expression) Source() string { return e.source }

// Variables lists the message variables the expression references.
func (e *Expression) Variables() []string { return e.variables }

// Match evaluates a predicate. Evaluation errors (such as a missing field) are returned.
func (e *Expression) Match(m *record.Message) (bool, error) {
	out, err := e.eval(m)
	if err != nil {
		return false, err
	}

	matched, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("expression returned %s, not bool", out.Type().TypeName())
	}

	return matched, nil
}

// String evaluates the expression and renders the result as a grouping key.
func (e *Expression) String(m *record.Message) (string, error) {
	out, err := e.eval(m)
	if err != nil {
		return "", err
	}

	if text, ok := out.ConvertToType(types.StringType).(types.String); ok {
		return string(text), nil
	}

	return fmt.Sprint(out.Value()), nil
}

func (e *Expression) eval(m *record.Message) (ref.Val, error) {
	out, _, err := e.program.Eval(activation{message: m})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func referencedVariables(ast *cel.Ast) []string {
	seen := map[string]bool{}

	for _, reference := range ast.NativeRep().ReferenceMap() {
		if _, ok := resolvers[reference.Name]; ok {
			seen[reference.Name] = true
		}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}
