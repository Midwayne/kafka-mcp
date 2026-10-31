// Package config loads and validates the kafka-mcp configuration file.
package config

// Config is the root document loaded from YAML or JSON.
type Config struct {
	Server   Server     `yaml:"server" json:"server"`
	Defaults Defaults   `yaml:"defaults" json:"defaults"`
	Clusters []Cluster  `yaml:"clusters" json:"clusters"`
	Tools    NameFilter `yaml:"tools" json:"tools"`
}

// Server controls how the MCP server is exposed.
type Server struct {
	Name        string `yaml:"name" json:"name"`
	Version     string `yaml:"version" json:"version"`
	ServeMode   string `yaml:"serve_mode" json:"serve_mode"`
	Host        string `yaml:"host" json:"host"`
	Port        int    `yaml:"port" json:"port"`
	BearerToken string `yaml:"bearer_token" json:"bearer_token"`
	StateDir    string `yaml:"state_dir" json:"state_dir"`
}

// Defaults apply to every cluster unless the cluster overrides them.
type Defaults struct {
	Access         string     `yaml:"access" json:"access"`
	ClientID       string     `yaml:"client_id" json:"client_id"`
	DialTimeout    Duration   `yaml:"dial_timeout" json:"dial_timeout"`
	RequestTimeout Duration   `yaml:"request_timeout" json:"request_timeout"`
	Isolation      string     `yaml:"isolation" json:"isolation"`
	Fetch          Fetch      `yaml:"fetch" json:"fetch"`
	Scan           ScanLimits `yaml:"scan" json:"scan"`
	Jobs           JobLimits  `yaml:"jobs" json:"jobs"`
}

// Fetch sizes consumer fetch responses. Kafka's defaults (1 MiB per partition,
// 50 MiB per request) keep responses quick over slow links such as VPNs.
type Fetch struct {
	MaxBytes          ByteSize `yaml:"max_bytes" json:"max_bytes"`
	MaxPartitionBytes ByteSize `yaml:"max_partition_bytes" json:"max_partition_bytes"`
}

// ScanLimits bounds the resources a single scan may use.
type ScanLimits struct {
	Workers          int      `yaml:"workers" json:"workers"`
	DefaultLimit     int      `yaml:"default_limit" json:"default_limit"`
	MaxLimit         int      `yaml:"max_limit" json:"max_limit"`
	MaxScanned       int64    `yaml:"max_scanned" json:"max_scanned"`
	MaxBytes         ByteSize `yaml:"max_bytes" json:"max_bytes"`
	DefaultTimeout   Duration `yaml:"default_timeout" json:"default_timeout"`
	MaxTimeout       Duration `yaml:"max_timeout" json:"max_timeout"`
	RateLimit        ByteSize `yaml:"rate_limit" json:"rate_limit"`
	MaxValueChars    int      `yaml:"max_value_chars" json:"max_value_chars"`
	MaxExportRecords int64    `yaml:"max_export_records" json:"max_export_records"`
	IdleTimeout      Duration `yaml:"idle_timeout" json:"idle_timeout"`
}

// JobLimits bounds background job bookkeeping.
type JobLimits struct {
	MaxConcurrent int      `yaml:"max_concurrent" json:"max_concurrent"`
	TTL           Duration `yaml:"ttl" json:"ttl"`
}

// NameFilter is an include/exclude list. Entries may be glob patterns.
type NameFilter struct {
	Include []string `yaml:"include" json:"include"`
	Exclude []string `yaml:"exclude" json:"exclude"`
}

// Cluster is one named Kafka cluster.
type Cluster struct {
	Name             string          `yaml:"name" json:"name"`
	Brokers          []string        `yaml:"brokers" json:"brokers"`
	Access           string          `yaml:"access" json:"access"`
	ClientID         string          `yaml:"client_id" json:"client_id"`
	Rack             string          `yaml:"rack" json:"rack"`
	Isolation        string          `yaml:"isolation" json:"isolation"`
	SecurityProtocol string          `yaml:"security_protocol" json:"security_protocol"`
	TLS              *TLS            `yaml:"tls" json:"tls"`
	SASL             *SASL           `yaml:"sasl" json:"sasl"`
	SchemaRegistry   *SchemaRegistry `yaml:"schema_registry" json:"schema_registry"`
	Topics           NameFilter      `yaml:"topics" json:"topics"`
	Formats          Formats         `yaml:"formats" json:"formats"`
	Redact           []string        `yaml:"redact" json:"redact"`
	Tools            NameFilter      `yaml:"tools" json:"tools"`
	Enabled          *bool           `yaml:"enabled" json:"enabled"`
	DialTimeout      Duration        `yaml:"dial_timeout" json:"dial_timeout"`
	RequestTimeout   Duration        `yaml:"request_timeout" json:"request_timeout"`
	Fetch            Fetch           `yaml:"fetch" json:"fetch"`
}

// TLS configures transport security. PEM files, JKS, and PKCS#12 stores are supported.
type TLS struct {
	Enabled            *bool  `yaml:"enabled" json:"enabled"`
	CAFile             string `yaml:"ca_file" json:"ca_file"`
	CAPEM              string `yaml:"ca_pem" json:"ca_pem"`
	CertFile           string `yaml:"cert_file" json:"cert_file"`
	KeyFile            string `yaml:"key_file" json:"key_file"`
	CertPEM            string `yaml:"cert_pem" json:"cert_pem"`
	KeyPEM             string `yaml:"key_pem" json:"key_pem"`
	Truststore         string `yaml:"truststore" json:"truststore"`
	TruststorePassword string `yaml:"truststore_password" json:"truststore_password"`
	TruststoreType     string `yaml:"truststore_type" json:"truststore_type"`
	Keystore           string `yaml:"keystore" json:"keystore"`
	KeystorePassword   string `yaml:"keystore_password" json:"keystore_password"`
	KeystoreType       string `yaml:"keystore_type" json:"keystore_type"`
	KeyPassword        string `yaml:"key_password" json:"key_password"`
	KeyAlias           string `yaml:"key_alias" json:"key_alias"`
	IncludeSystemCAs   bool   `yaml:"include_system_cas" json:"include_system_cas"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify" json:"insecure_skip_verify"`
	VerifyHostname     *bool  `yaml:"verify_hostname" json:"verify_hostname"`
	ServerName         string `yaml:"server_name" json:"server_name"`
	MinVersion         string `yaml:"min_version" json:"min_version"`
}

// SASL selects one authentication mechanism.
type SASL struct {
	Mechanism string    `yaml:"mechanism" json:"mechanism"`
	Username  string    `yaml:"username" json:"username"`
	Password  string    `yaml:"password" json:"password"`
	OAuth     *OAuth    `yaml:"oauth" json:"oauth"`
	AWS       *AWS      `yaml:"aws" json:"aws"`
	Kerberos  *Kerberos `yaml:"kerberos" json:"kerberos"`
}

// OAuth configures OAUTHBEARER with a static token, a token file, or client credentials.
type OAuth struct {
	Token          string            `yaml:"token" json:"token"`
	TokenFile      string            `yaml:"token_file" json:"token_file"`
	TokenURL       string            `yaml:"token_url" json:"token_url"`
	ClientID       string            `yaml:"client_id" json:"client_id"`
	ClientSecret   string            `yaml:"client_secret" json:"client_secret"`
	Scopes         []string          `yaml:"scopes" json:"scopes"`
	EndpointParams map[string]string `yaml:"endpoint_params" json:"endpoint_params"`
	Extensions     map[string]string `yaml:"extensions" json:"extensions"`
}

// AWS configures AWS_MSK_IAM. Empty fields fall back to the default AWS credential chain.
type AWS struct {
	Region          string `yaml:"region" json:"region"`
	Profile         string `yaml:"profile" json:"profile"`
	AccessKeyID     string `yaml:"access_key_id" json:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key" json:"secret_access_key"`
	SessionToken    string `yaml:"session_token" json:"session_token"`
}

// Kerberos configures GSSAPI with a keytab, password, or credential cache.
type Kerberos struct {
	ServiceName     string `yaml:"service_name" json:"service_name"`
	Realm           string `yaml:"realm" json:"realm"`
	Username        string `yaml:"username" json:"username"`
	Password        string `yaml:"password" json:"password"`
	Keytab          string `yaml:"keytab" json:"keytab"`
	CCache          string `yaml:"ccache" json:"ccache"`
	Krb5Conf        string `yaml:"krb5_conf" json:"krb5_conf"`
	DisablePAFXFAST bool   `yaml:"disable_pa_fx_fast" json:"disable_pa_fx_fast"`
}

// SchemaRegistry configures Confluent-compatible Schema Registry access.
type SchemaRegistry struct {
	URLs        []string `yaml:"urls" json:"urls"`
	URL         string   `yaml:"url" json:"url"`
	Username    string   `yaml:"username" json:"username"`
	Password    string   `yaml:"password" json:"password"`
	BearerToken string   `yaml:"bearer_token" json:"bearer_token"`
	TLS         *TLS     `yaml:"tls" json:"tls"`
}

// Formats sets default key/value decoders, optionally per topic glob.
type Formats struct {
	Key    string        `yaml:"key" json:"key"`
	Value  string        `yaml:"value" json:"value"`
	Topics []TopicFormat `yaml:"topics" json:"topics"`
}

// TopicFormat overrides decoders for topics matching Pattern.
type TopicFormat struct {
	Pattern string `yaml:"pattern" json:"pattern"`
	Key     string `yaml:"key" json:"key"`
	Value   string `yaml:"value" json:"value"`
}
