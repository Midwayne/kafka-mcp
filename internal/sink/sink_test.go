package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/codec"
	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/record"
)

func message(offset int64, ts time.Time, value string) *record.Message {
	return record.New(&kgo.Record{Topic: "t", Partition: 0, Offset: offset, Timestamp: ts, Value: []byte(value)},
		&record.Source{Ctx: context.Background(), Decoder: codec.NewDecoder(nil), ValueFormat: config.FormatJSON})
}

func TestMatchesKeepsLimitAndSortsByTime(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	matches := NewMatches(2, record.View{}, nil)

	type result struct{ accepted, more bool }

	got := make([]result, 0, 3)
	for _, m := range []*record.Message{
		message(5, base.Add(2*time.Second), `{}`),
		message(9, base, `{}`),
		message(1, base.Add(time.Second), `{}`),
	} {
		accepted, more := matches.Offer(m)
		got = append(got, result{accepted, more})
	}

	want := []result{{true, true}, {true, false}, {false, false}}
	if !slices.Equal(got, want) {
		t.Fatalf("Offer results = %v, want %v", got, want)
	}

	items := matches.Items()
	if len(items) != 2 || items[0]["offset"] != int64(9) || items[1]["offset"] != int64(5) {
		t.Fatalf("unexpected order: %v", items)
	}
}

func TestMatchesWithExportStreamsEverythingUntilFull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exports", "job.jsonl")

	export, err := NewExport(path, 3, []string{"value.secret"})
	if err != nil {
		t.Fatal(err)
	}

	matches := NewMatches(1, record.View{}, export)

	for i := range 3 {
		accepted, more := matches.Offer(message(int64(i), time.Now(), `{"secret":"s","n":`+strconv.Itoa(i)+`}`))
		if !accepted || more != (i < 2) {
			t.Fatalf("Offer #%d = %v, %v", i, accepted, more)
		}
	}

	info := export.Close()
	if info.Records != 3 || !info.Full || info.Error != "" || len(matches.Items()) != 1 {
		t.Fatalf("export info = %+v, inline = %d", info, len(matches.Items()))
	}

	lines := readLines(t, path)
	if len(lines) != 3 || lines[2]["value"].(map[string]any)["secret"] != record.RedactedValue {
		t.Fatalf("unexpected export contents: %v", lines)
	}

	if stat, err := os.Stat(path); err != nil || stat.Mode().Perm() != 0o600 {
		t.Fatalf("export permissions = %v, %v", stat.Mode(), err)
	}
}

func TestCountsGroupsCapsAndRanks(t *testing.T) {
	key := func(m *record.Message) (string, error) {
		status, ok := m.Value().Value.(map[string]any)["status"].(string)
		if !ok {
			return "", errors.New("no status")
		}

		return status, nil
	}

	counts := NewCounts(key, 2, 1, record.View{})
	values := []string{`{"status":"A"}`, `{"status":"B"}`, `{"status":"A"}`, `{"status":"C"}`, `{}`}

	for i, value := range values {
		counts.Offer(message(int64(i), time.Now(), value))
	}

	result := counts.Result(1)

	if result.Total != 5 || result.DistinctGroups != 2 || len(result.Samples) != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}

	if result.Groups[0] != (Group{Key: "A", Count: 2}) || result.OtherGroups != 3 {
		t.Fatalf("groups = %+v, other = %d", result.Groups, result.OtherGroups)
	}
}

func TestCountsIsSafeForConcurrentOffers(t *testing.T) {
	counts := NewCounts(nil, 10, 3, record.View{})

	var wg sync.WaitGroup
	for i := range 200 {
		wg.Go(func() { counts.Offer(message(int64(i), time.Now(), `{}`)) })
	}
	wg.Wait()

	result := counts.Result(0)
	if result.Total != 200 || result.Groups[0] != (Group{Key: GroupAll, Count: 200}) || len(result.Samples) != 3 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func readLines(t *testing.T, path string) []map[string]any {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var out []map[string]any

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var line map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		out = append(out, line)
	}

	return out
}
