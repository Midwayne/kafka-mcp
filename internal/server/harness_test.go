package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	mcpclient "github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/app"
	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/testutil"
)

const (
	ordersTopic = "orders"
	hiddenTopic = "payments.pii"
)

// harness is an in-process MCP client talking to a server backed by fake clusters.
type harness struct {
	t      *testing.T
	client *mcpclient.Client
}

// newHarness seeds "prod" (read_only) with 2 partitions x 50 orders created in
// the last minutes, and "sandbox" (read_write) with an empty topic.
func newHarness(t *testing.T) harness {
	t.Helper()

	prod := testutil.FakeCluster(t, kfake.SeedTopics(2, ordersTopic, hiddenTopic))
	sandbox := testutil.FakeCluster(t, kfake.SeedTopics(1, "scratch"))

	testutil.Produce(t, prod, orderRecords(time.Now().Add(-10*time.Minute))...)

	cfg := testutil.Normalize(t, &config.Config{
		Server: config.Server{StateDir: t.TempDir()},
		Clusters: []config.Cluster{
			{Name: "prod", Brokers: prod, Topics: config.NameFilter{Exclude: []string{"*.pii"}}, Redact: []string{"value.customer.email"}},
			{Name: "sandbox", Brokers: sandbox, Access: config.AccessReadWrite},
		},
	})

	svc, err := app.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)

	client, err := mcpclient.NewInProcessClient(New(svc, "kafka-mcp-test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if _, err := client.Initialize(context.Background(), mcp.InitializeRequest{}); err != nil {
		t.Fatal(err)
	}

	return harness{t: t, client: client}
}

// orderRecords: order i has status FAILED when i%5==0, store "s<i%3>", and a
// "type" header; every 10th order carries "timeout" in its error text.
func orderRecords(start time.Time) []*kgo.Record {
	records := make([]*kgo.Record, 0, 100)

	for i := range 100 {
		status, errText := "OK", ""
		if i%5 == 0 {
			status, errText = "FAILED", "card declined"
		}
		if i%10 == 0 {
			errText = "Gateway timeout ERR-504"
		}

		value := fmt.Sprintf(`{"id":%d,"status":%q,"store":"s%d","error":%q,"customer":{"email":"c%d@example.com"}}`, i, status, i%3, errText, i)
		rec := testutil.Record(ordersTopic, int32(i%2), fmt.Sprintf("order-%d", i), value, start.Add(time.Duration(i)*time.Second))
		rec.Headers = []kgo.RecordHeader{{Key: "type", Value: []byte("OrderUpdated")}}

		records = append(records, rec)
	}

	return records
}

// call invokes a tool and returns the decoded JSON result, failing on tool errors.
func (h harness) call(name string, args map[string]any) map[string]any {
	h.t.Helper()

	text, isError := h.callRaw(name, args)
	if isError {
		h.t.Fatalf("%s returned an error: %s", name, text)
	}

	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		h.t.Fatalf("%s returned non-object JSON: %s", name, text)
	}

	return out
}

// callError invokes a tool that must fail and returns its message.
func (h harness) callError(name string, args map[string]any) string {
	h.t.Helper()

	text, isError := h.callRaw(name, args)
	if !isError {
		h.t.Fatalf("%s should have failed, got %s", name, text)
	}

	return text
}

func (h harness) callRaw(name string, args map[string]any) (string, bool) {
	h.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	request := mcp.CallToolRequest{}
	request.Params.Name = name
	request.Params.Arguments = args

	result, err := h.client.CallTool(ctx, request)
	if err != nil {
		h.t.Fatalf("call %s: %v", name, err)
	}

	var text strings.Builder
	for _, content := range result.Content {
		if textContent, ok := content.(mcp.TextContent); ok {
			text.WriteString(textContent.Text)
		}
	}

	return text.String(), result.IsError
}

// list decodes a JSON array result.
func (h harness) list(name string, args map[string]any) []any {
	h.t.Helper()

	text, isError := h.callRaw(name, args)
	if isError {
		h.t.Fatalf("%s returned an error: %s", name, text)
	}

	var out []any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		h.t.Fatalf("%s returned non-array JSON: %s", name, text)
	}

	return out
}

func args(pairs ...any) map[string]any {
	out := map[string]any{}
	for i := 0; i+1 < len(pairs); i += 2 {
		key, _ := pairs[i].(string)
		out[key] = pairs[i+1]
	}

	return out
}

func number(value any) int {
	f, _ := value.(float64)
	return int(f)
}

func object(value any) map[string]any {
	m, _ := value.(map[string]any)
	return m
}

func items(value any) []any {
	s, _ := value.([]any)
	return s
}
