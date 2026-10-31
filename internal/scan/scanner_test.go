package scan

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Midwayne/kafka-mcp/internal/testutil"
)

func TestScanCompletesWithMatcher(t *testing.T) {
	f := newFixture(t)
	sink := newCollector(0)

	stats := f.run(t, f.plan(t, Selection{}, Bounds{}), sink, Limits{Workers: 4}, failedOnly(t))

	if stats.StopReason != ReasonCompleted || stats.Scanned != 300 || stats.Matched != 30 || sink.count() != 30 {
		t.Fatalf("stats = %+v, collected %d", stats, sink.count())
	}

	if stats.NextOffsets != nil || stats.PartitionsDone != 3 {
		t.Fatalf("completed scan should have no resume offsets: %+v", stats)
	}
}

func TestScanTimeWindowFiltersRecords(t *testing.T) {
	f := newFixture(t)
	sink := newCollector(0)

	plan := f.plan(t, Selection{}, Bounds{
		From: fixtureBase.Add(10 * time.Second).Format(time.RFC3339),
		To:   fixtureBase.Add(20 * time.Second).Format(time.RFC3339),
	})

	stats := f.run(t, plan, sink, Limits{})

	if sink.count() != 30 || stats.StopReason != ReasonCompleted {
		t.Fatalf("window should select 10 records per partition, got %d (%+v)", sink.count(), stats)
	}
}

func TestScanLimitThenResumeHasNoGaps(t *testing.T) {
	f := newFixture(t)
	first := newCollector(7)

	stats := f.run(t, f.plan(t, Selection{}, Bounds{}), first, Limits{Workers: 3}, failedOnly(t))
	if stats.StopReason != ReasonLimit || first.count() != 7 || stats.NextOffsets == nil {
		t.Fatalf("first page stats = %+v, collected %d", stats, first.count())
	}

	resume := map[string]map[int32]int64{}
	for topic, partitions := range stats.NextOffsets {
		resume[topic] = map[int32]int64{}
		for partition, offset := range partitions {
			p, _ := strconv.Atoi(partition)
			resume[topic][int32(p)] = offset
		}
	}

	second := newCollector(0)
	f.run(t, f.plan(t, Selection{}, Bounds{Resume: resume}), second, Limits{}, failedOnly(t))

	union := map[string]bool{}
	for key := range first.seen {
		union[key] = true
	}
	for key := range second.seen {
		union[key] = true
	}

	if len(union) != 30 {
		t.Fatalf("first+resumed pages cover %d of 30 matches", len(union))
	}
}

func TestScanBudgets(t *testing.T) {
	f := newFixture(t)

	cases := map[string]struct {
		limits Limits
		reason string
	}{
		"max scanned": {Limits{MaxScanned: 50}, ReasonMaxScanned},
		"max bytes":   {Limits{MaxBytes: 500}, ReasonMaxBytes},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stats := f.run(t, f.plan(t, Selection{}, Bounds{}), newCollector(0), tc.limits)

			if stats.StopReason != tc.reason || stats.Scanned >= 300 || stats.NextOffsets == nil {
				t.Fatalf("stats = %+v", stats)
			}
		})
	}
}

func TestScanFollowReadsNewRecords(t *testing.T) {
	f := newFixture(t)
	sink := newCollector(2)
	plan := f.plan(t, Selection{Partitions: []int32{0}}, Bounds{From: "latest", Follow: true})

	go func() {
		time.Sleep(500 * time.Millisecond)
		testutil.Produce(t, f.brokers,
			testutil.Record(fixtureTopic, 0, "", `{"status":"FAILED"}`, time.Now()),
			testutil.Record(fixtureTopic, 0, "", `{"status":"FAILED"}`, time.Now()),
		)
	}()

	scanner := New(Request{Plan: plan, Sources: nil, Sink: sink, Follow: true, Limits: Limits{IdleTimeout: 200 * time.Millisecond}})
	stats, err := scanner.Run(testContext(t), f.cluster)

	if err != nil || stats.StopReason != ReasonLimit || sink.count() != 2 {
		t.Fatalf("follow stats = %+v, err %v, collected %d", stats, err, sink.count())
	}
}

func TestScanTimeoutReportsReason(t *testing.T) {
	f := newFixture(t)
	plan := f.plan(t, Selection{Partitions: []int32{0}}, Bounds{From: "latest", Follow: true})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	stats, err := New(Request{Plan: plan, Sink: newCollector(0), Follow: true}).Run(ctx, f.cluster)
	if err != nil || stats.StopReason != ReasonTimeout {
		t.Fatalf("stats = %+v, err %v", stats, err)
	}
}

func TestScanCompletesWhenTransactionMarkersEndThePartition(t *testing.T) {
	brokers := testutil.FakeCluster(t, kfake.SeedTopics(1, "tx"))

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.TransactionalID("tx-test"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	ctx := testContext(t)
	for range 3 {
		if err := producer.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: "tx", Value: []byte(`{}`)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}
	}

	f := fixture{cluster: newCluster(t, brokers)}
	sink := newCollector(0)
	stats := f.run(t, f.plan(t, Selection{Topics: []string{"tx"}}, Bounds{}), sink, Limits{IdleTimeout: 10 * time.Second})

	if stats.StopReason != ReasonCompleted || sink.count() != 3 || stats.ElapsedMs > 5000 {
		t.Fatalf("stats = %+v, collected %d", stats, sink.count())
	}
}

func TestScanWaitsForSlowFirstFetch(t *testing.T) {
	fake := testutil.NewFakeCluster(t, kfake.SeedTopics(1, "slow"))
	brokers := fake.ListenAddrs()

	testutil.Produce(t, brokers, testutil.Record("slow", 0, "", `{}`, time.Now()), testutil.Record("slow", 0, "", `{}`, time.Now()))

	// Delay the first fetch well beyond the idle timeout, like a cold tiered-storage read.
	var delayed atomic.Bool
	fake.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		if delayed.CompareAndSwap(false, true) {
			time.Sleep(1500 * time.Millisecond)
		}

		return nil, nil, false
	})

	f := fixture{cluster: newCluster(t, brokers)}
	sink := newCollector(0)
	stats := f.run(t, f.plan(t, Selection{Topics: []string{"slow"}}, Bounds{}), sink, Limits{IdleTimeout: 300 * time.Millisecond})

	if stats.StopReason != ReasonCompleted || sink.count() != 2 {
		t.Fatalf("slow first fetch should not end the scan as idle: %+v, collected %d", stats, sink.count())
	}
}
