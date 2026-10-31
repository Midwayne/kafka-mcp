package server

import (
	"context"

	"github.com/Midwayne/kafka-mcp/internal/app"
)

func exploreTools(svc *app.Service) []toolDef {
	return []toolDef{
		define(app.ToolListClusters,
			"List configured Kafka clusters: name, brokers, access mode, auth type (no secrets), topic filters, redacted fields, and default decoders. Start here; pass the name as cluster to other tools.",
			reads, func(context.Context, none) (any, error) { return svc.ListClusters(), nil }),

		define(app.ToolDescribeCluster,
			"Connect to every broker of a cluster and return its ID, controller, broker and rack counts, reachable brokers, and number of visible topics. Use to verify connectivity and auth.",
			reads, func(ctx context.Context, args app.DescribeClusterSpec) (any, error) {
				return svc.DescribeCluster(ctx, args)
			}),

		define(app.ToolListTopics,
			"List visible topics with partition and replication counts; optionally approximate message counts. Filter with a glob pattern.",
			reads, func(ctx context.Context, args app.ListTopicsSpec) (any, error) { return svc.ListTopics(ctx, args) }),

		define(app.ToolDescribeTopic,
			"Describe a topic: per-partition leader/replicas/ISR, start and end offsets, approximate message counts, newest message time per partition, key configs (retention, cleanup.policy, ...), and optionally the offsets at a given time. Use before scanning to judge size and freshness.",
			reads, func(ctx context.Context, args app.DescribeTopicSpec) (any, error) {
				return svc.DescribeTopic(ctx, args)
			}),

		define(app.ToolListConsumerGroups,
			"List consumer groups and their states, optionally filtered by a glob.",
			reads, func(ctx context.Context, args app.ListGroupsSpec) (any, error) {
				return svc.ListConsumerGroups(ctx, args)
			}),

		define(app.ToolDescribeConsumerGroup,
			"Show a consumer group's state, members, and lag per topic and partition (committed vs end offsets). Read-only: never joins the group or commits offsets.",
			reads, func(ctx context.Context, args app.DescribeGroupSpec) (any, error) {
				return svc.DescribeConsumerGroup(ctx, args)
			}),
	}
}
