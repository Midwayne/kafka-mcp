package kafka

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/testutil"
)

func pingCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	return ctx
}

func TestMutualTLSWithJKSAndSCRAM(t *testing.T) {
	ca := testutil.NewCA(t)
	server, client := ca.Issue(t, "localhost"), ca.Issue(t, "client")
	dir := t.TempDir()

	brokers := testutil.FakeCluster(t,
		kfake.TLS(&tls.Config{
			Certificates: []tls.Certificate{server.TLSCertificate()},
			ClientCAs:    ca.Pool(),
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
		}),
		kfake.EnableSASL(),
		kfake.Superuser(config.MechScram512, "svc", "s3cret"),
	)

	cfg := testutil.Normalize(t, &config.Config{Clusters: []config.Cluster{{
		Name: "secure", Brokers: brokers, SecurityProtocol: "SASL_SSL",
		TLS: &config.TLS{
			Truststore:         testutil.WriteJKSTrustStore(t, dir, "trust", ca),
			TruststorePassword: "trust",
			Keystore:           testutil.WriteJKSKeyStore(t, dir, "store", "store", client, ca),
			KeystorePassword:   "store",
		},
		SASL: &config.SASL{Mechanism: "scram-sha-512", Username: "svc", Password: "s3cret"},
	}}})

	cluster, err := NewCluster(cfg.Clusters[0])
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	if brokers, err := cluster.Ping(pingCtx(t)); err != nil || brokers != 1 {
		t.Fatalf("ping over mTLS+SCRAM: %d brokers, %v", brokers, err)
	}

	cfg.Clusters[0].SASL.Password = "wrong"

	badCluster, err := NewCluster(cfg.Clusters[0])
	if err != nil {
		t.Fatal(err)
	}
	defer badCluster.Close()

	if _, err := badCluster.Ping(pingCtx(t)); err == nil {
		t.Fatal("expected authentication failure with the wrong password")
	}
}

func TestConsumerReadsAssignedPartitionsWithoutGroup(t *testing.T) {
	brokers := testutil.FakeCluster(t, kfake.SeedTopics(2, "events"))
	now := time.Now()

	testutil.Produce(t, brokers,
		testutil.Record("events", 0, "a", "one", now),
		testutil.Record("events", 1, "b", "two", now),
		testutil.Record("events", 1, "c", "three", now),
	)

	cluster, err := NewCluster(testutil.ClusterConfig(t, "local", brokers))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	consumer, err := cluster.Consumer(map[string]map[int32]kgo.Offset{
		"events": {1: kgo.NewOffset().At(1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	fetches := consumer.PollFetches(pingCtx(t))
	records := fetches.Records()

	if len(records) != 1 || string(records[0].Value) != "three" {
		t.Fatalf("expected only partition 1 offset 1, got %d records", len(records))
	}
}

func TestRegistry(t *testing.T) {
	disabled := false
	brokers := []string{"127.0.0.1:1"}

	registry, err := NewRegistry([]config.Cluster{
		testutil.ClusterConfig(t, "b", brokers),
		testutil.ClusterConfig(t, "a", brokers),
		{Name: "off", Enabled: &disabled},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()

	all := registry.All()
	if len(all) != 2 || all[0].Name() != "a" {
		t.Fatalf("unexpected clusters: %d", len(all))
	}

	if _, err := registry.Get("off"); err == nil {
		t.Fatal("disabled cluster should not be registered")
	}
}
