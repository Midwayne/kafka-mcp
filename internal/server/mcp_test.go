package server

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/Midwayne/kafka-mcp/internal/app"
)

func TestToolCatalog(t *testing.T) {
	h := newHarness(t)

	listed, err := h.client.ListTools(context.Background(), mcp.ListToolsRequest{})
	if err != nil {
		t.Fatal(err)
	}

	names := make([]string, 0, len(listed.Tools))
	for _, tool := range listed.Tools {
		names = append(names, tool.Name)

		if tool.Annotations.ReadOnlyHint == nil {
			t.Fatalf("tool %s has no read-only annotation", tool.Name)
		}
	}

	for _, want := range []string{app.ToolListClusters, app.ToolScanMessages, app.ToolCountMessages, app.ToolInferSchema, app.ToolProduceMessage} {
		if !slices.Contains(names, want) {
			t.Fatalf("tool %s missing from %v", want, names)
		}
	}
}

func TestDiscoveryRespectsTopicFilters(t *testing.T) {
	h := newHarness(t)

	if clusters := h.list(app.ToolListClusters, nil); len(clusters) != 2 {
		t.Fatalf("list_clusters = %v", clusters)
	}

	topics := items(h.call(app.ToolListTopics, args("cluster", "prod", "with_counts", true))["topics"])
	if len(topics) != 1 || object(topics[0])["name"] != ordersTopic || number(object(topics[0])["approx_messages"]) != 100 {
		t.Fatalf("list_topics = %v", topics)
	}

	described := h.call(app.ToolDescribeTopic, args("cluster", "prod", "topic", ordersTopic))
	if number(described["approx_messages"]) != 100 || len(items(described["partitions"])) != 2 || described["last_message_at"] == "" {
		t.Fatalf("describe_topic = %v", described)
	}

	if msg := h.callError(app.ToolDescribeTopic, args("cluster", "prod", "topic", hiddenTopic)); !strings.Contains(msg, "not exposed") {
		t.Fatalf("hidden topic error = %s", msg)
	}

	if msg := h.callError(app.ToolScanMessages, args("cluster", "prod", "topics", []string{"payments.*"})); !strings.Contains(msg, "no visible topics") {
		t.Fatalf("hidden glob error = %s", msg)
	}
}

func TestSampleInferValidate(t *testing.T) {
	h := newHarness(t)

	messages := items(h.call(app.ToolSampleMessages, args("cluster", "prod", "topics", []string{ordersTopic}, "count", 3))["messages"])
	if len(messages) != 3 {
		t.Fatalf("sample_messages returned %d", len(messages))
	}

	customer := object(object(object(messages[0])["value"])["customer"])
	if customer["email"] != "[REDACTED]" {
		t.Fatalf("redaction not applied: %v", customer)
	}

	inferred := h.call(app.ToolInferSchema, args("cluster", "prod", "topics", []string{ordersTopic}))
	fields := items(object(inferred["schema"])["fields"])
	paths := make([]string, 0, len(fields))
	for _, field := range fields {
		paths = append(paths, object(field)["path"].(string))
	}

	if !slices.Contains(paths, "value.status") || !slices.Contains(paths, "headers.type") {
		t.Fatalf("infer_schema paths = %v", paths)
	}

	invalid := h.call(app.ToolValidateFilter, args("filter", "value.status =="))
	if invalid["valid"] != false || invalid["error"] == "" {
		t.Fatalf("invalid filter = %v", invalid)
	}

	valid := h.call(app.ToolValidateFilter, args("filter", "value.status == 'FAILED'", "cluster", "prod", "topics", []string{ordersTopic}))
	sample := object(valid["sample"])
	if valid["valid"] != true || number(sample["evaluated"]) != 100 || number(sample["matched"]) != 20 {
		t.Fatalf("validate_filter = %v", valid)
	}
}

func TestScanWithFilterAndGrep(t *testing.T) {
	h := newHarness(t)
	base := []any{"cluster", "prod", "topics", []string{ordersTopic}, "limit", 100}

	failed := h.call(app.ToolScanMessages, args(append(base, "filter", "value.status == 'FAILED'")...))
	if stats := object(failed["stats"]); number(stats["matched"]) != 20 || stats["stop_reason"] != "completed" {
		t.Fatalf("filter scan stats = %v", stats)
	}

	snippets := h.call(app.ToolScanMessages, args(append(base, "grep", map[string]any{
		"patterns": []string{`ERR-\d+`}, "in": []string{"value"}, "only_matching": true,
	})...))

	matches := items(snippets["matches"])
	first := object(matches[0])
	if len(matches) != 10 || first["value"] != nil || object(items(first["grep"])[0])["match"] != "ERR-504" {
		t.Fatalf("grep only_matching = %v", snippets)
	}

	combined := h.call(app.ToolScanMessages, args(append(base,
		"grep", map[string]any{"patterns": []string{"DECLINED"}, "ignore_case": true},
		"filter", "value.store == 's0'",
	)...))
	if number(object(combined["stats"])["matched"]) != 3 {
		t.Fatalf("grep + filter = %v", combined["stats"])
	}

	keyed := h.call(app.ToolScanMessages, args(append(base, "key", "order-7", "fields", []string{"value.id"})...))
	if keyedMatches := items(keyed["matches"]); len(keyedMatches) != 1 || number(object(keyedMatches[0])["value.id"]) != 7 {
		t.Fatalf("key scan = %v", keyed["matches"])
	}
}

func TestCountGroupBy(t *testing.T) {
	h := newHarness(t)

	counted := h.call(app.ToolCountMessages, args("cluster", "prod", "topics", []string{ordersTopic},
		"filter", "value.status == 'FAILED'", "group_by", "value.store"))

	counts := object(counted["counts"])
	groups := items(counts["groups"])

	if number(counts["total"]) != 20 || len(groups) != 3 || number(object(groups[2])["count"]) != 6 || object(groups[2])["key"] != "s1" {
		t.Fatalf("count_messages = %v", counts)
	}
}

func TestPagingWithResumeOffsets(t *testing.T) {
	h := newHarness(t)
	base := []any{"cluster", "prod", "topics", []string{ordersTopic}, "filter", "value.status == 'FAILED'", "fields", []string{"value.id"}}

	first := h.call(app.ToolScanMessages, args(append(base, "limit", 8)...))
	stats := object(first["stats"])

	if stats["stop_reason"] != "limit" || stats["next_offsets"] == nil || len(first["hints"].([]any)) == 0 {
		t.Fatalf("first page = %v", first)
	}

	second := h.call(app.ToolScanMessages, args(append(base, "limit", 100, "resume_offsets", stats["next_offsets"])...))

	seen := map[int]bool{}
	for _, page := range [][]any{items(first["matches"]), items(second["matches"])} {
		for _, match := range page {
			seen[number(object(match)["value.id"])] = true
		}
	}

	if len(seen) != 20 {
		t.Fatalf("two pages covered %d of 20 failed orders", len(seen))
	}
}
