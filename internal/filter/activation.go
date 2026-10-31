package filter

import (
	"time"

	"cel.dev/cel-go/interpreter"

	"github.com/Midwayne/kafka-mcp/internal/record"
)

// activation resolves variables on demand, so a message's value is only
// decoded when evaluation actually reaches a reference to it.
type activation struct {
	message *record.Message
}

var resolvers = map[string]func(*record.Message) any{
	VarKey:       func(m *record.Message) any { return m.Key().Value },
	VarValue:     func(m *record.Message) any { return m.Value().Value },
	VarHeaders:   func(m *record.Message) any { return m.Headers() },
	VarTopic:     func(m *record.Message) any { return m.Record.Topic },
	VarPartition: func(m *record.Message) any { return int64(m.Record.Partition) },
	VarOffset:    func(m *record.Message) any { return m.Record.Offset },
	VarTS:        func(m *record.Message) any { return m.TimestampMs() },
	VarTimestamp: func(m *record.Message) any { return m.Record.Timestamp.In(time.UTC) },
	VarRawKey:    func(m *record.Message) any { return m.RawKey() },
	VarRawValue:  func(m *record.Message) any { return m.RawValue() },
}

func (a activation) ResolveName(name string) (any, bool) {
	resolve, ok := resolvers[name]
	if !ok {
		return nil, false
	}

	return resolve(a.message), true
}

func (activation) Parent() interpreter.Activation { return nil }
