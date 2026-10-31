package scan

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/codec"
	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/filter"
	"github.com/Midwayne/kafka-mcp/internal/kafka"
	"github.com/Midwayne/kafka-mcp/internal/record"
	"github.com/Midwayne/kafka-mcp/internal/testutil"
)

const (
	fixtureTopic      = "events"
	fixturePartitions = 3
	fixturePerPart    = 100
)

var fixtureBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// fixture holds a fake cluster seeded with 3 partitions x 100 JSON records.
// Record i in each partition has timestamp base+i seconds; every 10th is FAILED.
type fixture struct {
	brokers []string
	cluster *kafka.Cluster
}

func newFixture(t *testing.T) fixture {
	t.Helper()

	brokers := testutil.FakeCluster(t, kfake.SeedTopics(fixturePartitions, fixtureTopic))

	records := make([]*kgo.Record, 0, fixturePartitions*fixturePerPart)
	for p := range int32(fixturePartitions) {
		for i := range fixturePerPart {
			status := "OK"
			if i%10 == 0 {
				status = "FAILED"
			}

			value := fmt.Sprintf(`{"n":%d,"status":%q}`, i, status)
			records = append(records, testutil.Record(fixtureTopic, p, fmt.Sprintf("k-%d", i), value, fixtureBase.Add(time.Duration(i)*time.Second)))
		}
	}
	testutil.Produce(t, brokers, records...)

	return fixture{brokers: brokers, cluster: newCluster(t, brokers)}
}

func newCluster(t *testing.T, brokers []string) *kafka.Cluster {
	t.Helper()

	cluster, err := kafka.NewCluster(testutil.ClusterConfig(t, "fake", brokers))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)

	return cluster
}

func (f fixture) plan(t *testing.T, sel Selection, bounds Bounds) Plan {
	t.Helper()

	if len(sel.Topics) == 0 {
		sel.Topics = []string{fixtureTopic}
	}

	plan, err := BuildPlan(testContext(t), f.cluster.Admin(), sel, bounds, fixtureBase.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	return plan
}

func (f fixture) run(t *testing.T, plan Plan, sink Sink, limits Limits, matchers ...Matcher) Stats {
	t.Helper()

	sources := map[string]*record.Source{}
	for _, rng := range plan.Ranges {
		sources[rng.Topic] = &record.Source{Ctx: context.Background(), Decoder: codec.NewDecoder(nil), ValueFormat: config.FormatJSON}
	}

	if limits.IdleTimeout == 0 {
		limits.IdleTimeout = 2 * time.Second
	}

	stats, err := New(Request{Plan: plan, Sources: sources, Matchers: matchers, Sink: sink, Limits: limits}).Run(testContext(t), f.cluster)
	if err != nil {
		t.Fatal(err)
	}

	return stats
}

func testContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	return ctx
}

func failedOnly(t *testing.T) Matcher {
	t.Helper()

	expression, err := filter.CompilePredicate(`value.status == "FAILED"`)
	if err != nil {
		t.Fatal(err)
	}

	return expression
}

// collector is a Sink that records message coordinates, optionally up to a limit.
type collector struct {
	limit int

	mu   sync.Mutex
	seen map[string]int
}

func newCollector(limit int) *collector { return &collector{limit: limit, seen: map[string]int{}} }

func (c *collector) Offer(m *record.Message) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.limit > 0 && len(c.seen) >= c.limit {
		return false, false
	}

	c.seen[fmt.Sprintf("%s/%d/%d", m.Record.Topic, m.Record.Partition, m.Record.Offset)]++

	return true, c.limit == 0 || len(c.seen) < c.limit
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.seen)
}
