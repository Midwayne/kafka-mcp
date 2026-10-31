package scan

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/testutil"
)

func int64p(v int64) *int64 { return &v }

func TestBuildPlanBounds(t *testing.T) {
	f := newFixture(t)
	at50 := fixtureBase.Add(50 * time.Second).Format(time.RFC3339)

	cases := []struct {
		name       string
		sel        Selection
		bounds     Bounds
		wantRanges []Range
		wantFromMs int64
	}{
		{"everything", Selection{}, Bounds{}, ranges(0, 100), 0},
		{"from time", Selection{}, Bounds{From: at50}, ranges(50, 100), fixtureBase.Add(50 * time.Second).UnixMilli()},
		{"to time", Selection{}, Bounds{To: at50}, ranges(0, 50), 0},
		{"last 10 per partition", Selection{}, Bounds{StartOffset: int64p(-10)}, ranges(90, 100), 0},
		{"absolute offsets", Selection{}, Bounds{StartOffset: int64p(5), EndOffset: int64p(20)}, ranges(5, 20), 0},
		{"start offset beats from", Selection{}, Bounds{From: at50, StartOffset: int64p(0), EndOffset: int64p(1)}, ranges(0, 1), 0},
		{"clamped past end", Selection{}, Bounds{StartOffset: int64p(500)}, nil, 0},
		{"one partition", Selection{Partitions: []int32{1}}, Bounds{EndOffset: int64p(3)}, []Range{{fixtureTopic, 1, 0, 3}}, 0},
		{"resume", Selection{Partitions: []int32{2}}, Bounds{Resume: map[string]map[int32]int64{fixtureTopic: {2: 97}}}, []Range{{fixtureTopic, 2, 97, 100}}, 0},
		{"follow", Selection{Partitions: []int32{0}}, Bounds{From: "latest", Follow: true}, []Range{{fixtureTopic, 0, 100, OpenEnd}}, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := f.plan(t, tc.sel, tc.bounds)

			if !slices.Equal(plan.Ranges, tc.wantRanges) || plan.FromMs != tc.wantFromMs {
				t.Fatalf("plan = %+v (from %d), want %+v (from %d)", plan.Ranges, plan.FromMs, tc.wantRanges, tc.wantFromMs)
			}
		})
	}
}

func TestBuildPlanErrors(t *testing.T) {
	f := newFixture(t)

	cases := map[string]struct {
		sel    Selection
		bounds Bounds
	}{
		"unknown topic":     {Selection{Topics: []string{"missing"}}, Bounds{}},
		"unknown partition": {Selection{Topics: []string{fixtureTopic}, Partitions: []int32{9}}, Bounds{}},
		"bad from":          {Selection{Topics: []string{fixtureTopic}}, Bounds{From: "someday"}},
	}

	for name, tc := range cases {
		if _, err := BuildPlan(testContext(t), f.cluster.Admin(), tc.sel, tc.bounds, time.Now()); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}

func TestKeyPruningAgreesWithProducerPartitioner(t *testing.T) {
	brokers := testutil.FakeCluster(t, kfake.SeedTopics(12, "keyed"))

	// franz-go's default partitioner hashes keys like the Java client (murmur2).
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	keys := []string{"order-1", "order-2", "store-1375", "trailer-abc"}
	for _, key := range keys {
		if err := client.ProduceSync(context.Background(), &kgo.Record{Topic: "keyed", Key: []byte(key), Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	f := fixture{cluster: newCluster(t, brokers)}
	murmur, _ := PartitionerByName("murmur2")

	for _, key := range keys {
		plan := f.plan(t, Selection{Topics: []string{"keyed"}, Key: []byte(key), Partitioner: murmur}, Bounds{})
		if len(plan.Ranges) != 1 {
			t.Fatalf("key %q: expected exactly the key's partition to hold data, got %+v", key, plan.Ranges)
		}

		sink := newCollector(0)
		f.run(t, plan, sink, Limits{})

		if sink.count() == 0 {
			t.Fatalf("key %q: pruned plan missed the record", key)
		}
	}
}

func ranges(start, end int64) []Range {
	out := make([]Range, 0, fixturePartitions)
	for p := range int32(fixturePartitions) {
		out = append(out, Range{Topic: fixtureTopic, Partition: p, Start: start, End: end})
	}

	return out
}
