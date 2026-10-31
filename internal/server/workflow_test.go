package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Midwayne/kafka-mcp/internal/app"
)

func TestBackgroundJobs(t *testing.T) {
	h := newHarness(t)

	started := h.call(app.ToolScanMessages, args("cluster", "prod", "topics", []string{ordersTopic}, "wait_seconds", 0))
	jobID, _ := started["job_id"].(string)

	if !strings.HasPrefix(jobID, "job-") {
		t.Fatalf("expected a job id, got %v", started)
	}

	finished := h.call(app.ToolGetJob, args("job_id", jobID, "wait_seconds", 20, "limit", 5))
	if finished["status"] != "completed" || len(items(finished["matches"])) != 5 {
		t.Fatalf("get_job = %v", finished)
	}

	if jobs := h.list(app.ToolListJobs, nil); len(jobs) != 1 {
		t.Fatalf("list_jobs = %v", jobs)
	}

	if msg := h.callError(app.ToolGetJob, args("job_id", "job-missing")); !strings.Contains(msg, "unknown") {
		t.Fatalf("missing job error = %s", msg)
	}
}

func TestSavedScans(t *testing.T) {
	h := newHarness(t)

	h.call(app.ToolSaveScan, args("name", "failed-by-store", "kind", "count", "spec", map[string]any{
		"cluster": "prod", "topics": []string{ordersTopic}, "filter": "value.status == 'FAILED'", "group_by": "value.store",
	}))

	if saved := h.list(app.ToolListSavedScans, nil); len(saved) != 1 {
		t.Fatalf("list_saved_scans = %v", saved)
	}

	ran := h.call(app.ToolRunSavedScan, args("name", "failed-by-store"))
	if number(object(ran["counts"])["total"]) != 20 {
		t.Fatalf("run_saved_scan = %v", ran)
	}

	invalid := args("name", "broken", "spec", map[string]any{"cluster": "prod", "topics": []string{ordersTopic}, "filter": "value.("})
	if msg := h.callError(app.ToolSaveScan, invalid); !strings.Contains(msg, "invalid") {
		t.Fatalf("invalid saved scan error = %s", msg)
	}

	if deleted := h.call(app.ToolDeleteSavedScan, args("name", "failed-by-store")); deleted["deleted"] != true {
		t.Fatalf("delete_saved_scan = %v", deleted)
	}
}

func TestProduceRequiresReadWrite(t *testing.T) {
	h := newHarness(t)

	if msg := h.callError(app.ToolProduceMessage, args("cluster", "prod", "topic", ordersTopic, "value", "x")); !strings.Contains(msg, "read_only") {
		t.Fatalf("read-only produce error = %s", msg)
	}

	produced := h.call(app.ToolProduceMessage, args("cluster", "sandbox", "topic", "scratch", "key", "k", "value", `{"hello":"world"}`))
	if produced["topic"] != "scratch" {
		t.Fatalf("produce_message = %v", produced)
	}

	read := h.call(app.ToolSampleMessages, args("cluster", "sandbox", "topics", []string{"scratch"}))
	if messages := items(read["messages"]); len(messages) != 1 || object(object(messages[0])["value"])["hello"] != "world" {
		t.Fatalf("produced message not readable: %v", read)
	}
}

func TestGetMessagesWithContextAndDryRun(t *testing.T) {
	h := newHarness(t)

	around := items(h.call(app.ToolGetMessages, args("cluster", "prod", "topic", ordersTopic, "partition", 0, "offset", 10, "context", 2))["messages"])
	if len(around) != 5 || number(object(around[0])["offset"]) != 8 || number(object(around[4])["offset"]) != 12 {
		t.Fatalf("get_messages = %v", around)
	}

	plan := h.call(app.ToolScanMessages, args("cluster", "prod", "topics", []string{"ord*"}, "dry_run", true))
	if number(object(plan["plan"])["records_in_range"]) != 100 || plan["stats"] != nil {
		t.Fatalf("dry_run = %v", plan)
	}
}

func TestRequireBearer(t *testing.T) {
	handler := requireBearer("s3cret", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	cases := map[string]int{"": http.StatusUnauthorized, "Bearer nope": http.StatusUnauthorized, "Bearer s3cret": http.StatusNoContent}

	for header, want := range cases {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/mcp", http.NoBody)
		request.Header.Set("Authorization", header)

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)

		if recorder.Code != want {
			t.Fatalf("Authorization %q: status %d, want %d", header, recorder.Code, want)
		}
	}
}
