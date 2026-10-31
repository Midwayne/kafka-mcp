package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, name, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestLoadMinimalAppliesDefaults(t *testing.T) {
	path := writeConfig(t, "kafka-mcp.yaml", "clusters:\n  - name: local\n    brokers: [\"a:9092,b:9092\"]\n")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	cluster := cfg.Clusters[0]
	if len(cluster.Brokers) != 2 || cluster.Access != AccessReadOnly || cluster.ClientID != DefaultClientID {
		t.Fatalf("unexpected cluster defaults: %+v", cluster)
	}

	if cfg.Server.ServeMode != "stdio" || cfg.Defaults.Scan.DefaultLimit != 20 || cfg.Defaults.Jobs.MaxConcurrent != 4 {
		t.Fatalf("unexpected server/scan defaults: %+v %+v", cfg.Server, cfg.Defaults)
	}

	if cluster.Formats.Key != FormatAuto || cluster.TLSEnabled() {
		t.Fatalf("unexpected formats/tls: %+v", cluster)
	}
}

func TestExpandPlaceholders(t *testing.T) {
	t.Setenv("KMCP_PASS", "s3cret")

	secret := writeConfig(t, "secret.txt", "from-file\n")
	input := "a=${KMCP_PASS} b=${KMCP_MISSING:-fallback} c=$$literal d=$KEEP e=${file:" + secret + "}"

	got, err := Expand(input)
	if err != nil {
		t.Fatal(err)
	}

	want := "a=s3cret b=fallback c=$literal d=$KEEP e=from-file"
	if got != want {
		t.Fatalf("Expand = %q, want %q", got, want)
	}

	if _, err := Expand("${file:/does/not/exist}"); err == nil {
		t.Fatal("expected missing file error")
	}
}

func TestUnitsParse(t *testing.T) {
	cases := map[string]int64{"1024": 1024, "64MiB": 64 << 20, "1gb": 1e9, "2k": 2048, "": 0}
	for raw, want := range cases {
		got, err := ParseByteSize(raw)
		if err != nil || got != want {
			t.Fatalf("ParseByteSize(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}

	day, err := ParseDuration("2d")
	if err != nil || day != 48*time.Hour {
		t.Fatalf("ParseDuration(2d) = %v, %v", day, err)
	}
}

func TestSecurityValidation(t *testing.T) {
	cases := []struct {
		name, body, wantErr string
	}{
		{"ssl creates tls", "security_protocol: SSL", ""},
		{"sasl requires block", "security_protocol: SASL_SSL", "requires a sasl block"},
		{"plaintext conflicts tls", "security_protocol: PLAINTEXT\n    tls: {ca_file: x}", "conflicts"},
		{"scram needs password", "sasl: {mechanism: scram-sha-512, username: u}", "requires username and password"},
		{"oauth one source", "sasl: {mechanism: oauthbearer, oauth: {token: a, token_file: b}}", "exactly one"},
		{"unknown mechanism", "sasl: {mechanism: magic}", "not supported"},
		{"half cert", "tls: {cert_file: c}", "set together"},
		{"two cert sources", "tls: {cert_file: c, key_file: k, keystore: ks.jks}", "choose one"},
		{"kerberos keytab", "sasl: {mechanism: kerberos, kerberos: {keytab: /k, username: u, realm: R}}", ""},
		{"bad format", "formats: {value: xml}", "unknown format"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := "clusters:\n  - name: c\n    brokers: [b:9092]\n    " + tc.body + "\n"
			_, err := Load(writeConfig(t, "c.yaml", body))

			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestFormatsAndTopicFilter(t *testing.T) {
	body := `clusters:
  - name: c
    brokers: [b:9092]
    topics: {include: ["orders.*", "users"], exclude: ["*.pii"]}
    formats:
      value: json
      topics:
        - {pattern: "orders.*", value: avro}
`
	cfg, err := Load(writeConfig(t, "c.yaml", body))
	if err != nil {
		t.Fatal(err)
	}

	cluster := cfg.Clusters[0]
	if _, value := cluster.FormatsFor("orders.v1"); value != FormatSchemaRegistry {
		t.Fatalf("orders format = %s", value)
	}

	if _, value := cluster.FormatsFor("users"); value != FormatJSON {
		t.Fatalf("users format = %s", value)
	}

	allowed := map[string]bool{"orders.v1": true, "users": true, "orders.pii": false, "payments": false}
	for topic, want := range allowed {
		if got := cluster.TopicAllowed(topic); got != want {
			t.Fatalf("TopicAllowed(%s) = %v", topic, got)
		}
	}
}

func TestRejectsUnknownFieldsAndDuplicates(t *testing.T) {
	if _, err := Load(writeConfig(t, "c.yaml", "clusters:\n  - name: c\n    brokerz: [x]\n")); err == nil {
		t.Fatal("expected unknown field error")
	}

	dup := "clusters:\n  - {name: c, brokers: [x]}\n  - {name: c, brokers: [y]}\n"
	if _, err := Load(writeConfig(t, "c.yaml", dup)); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}
