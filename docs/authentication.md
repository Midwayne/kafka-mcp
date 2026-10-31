# Authentication

kafka-mcp supports every common Kafka authentication setup: TLS from PEM, JKS, or PKCS#12, and SASL with PLAIN, SCRAM, OAUTHBEARER, AWS MSK IAM, or Kerberos (GSSAPI), each optionally combined with mutual TLS. Credentials are resolved lazily and refreshed automatically where the mechanism allows (OAuth tokens, AWS credentials, token files).

- [TLS](#tls)
  - [Trust (verifying brokers)](#trust-verifying-brokers)
  - [Client certificates (mutual TLS)](#client-certificates-mutual-tls)
  - [Java keystores](#java-keystores)
  - [Hostname verification](#hostname-verification)
- [SASL](#sasl)
- [Schema Registry](#schema-registry)
- [Troubleshooting](#troubleshooting)

Always confirm with `kafka-mcp -check`. It connects to **every** broker the cluster advertises, not just the bootstrap address, and adds a hint to every failure.

## TLS

A `tls` block enables TLS. An empty block (`tls: {}`) means "TLS with the system's root CAs", which suits public endpoints such as Confluent Cloud or MSK.

### Trust (verifying brokers)

Combine any of these. When any are set, only they are trusted, as with Java truststores.

| Key | Meaning |
|---|---|
| `ca_file` | PEM file with one or more CA certificates |
| `ca_pem` | The same, inline |
| `truststore` | JKS, PKCS#12, or PEM truststore (format detected from the content) |
| `truststore_password` | Truststore password. May be empty for JKS truststores (integrity check skipped, as in Java). |
| `truststore_type` | Only needed when detection is impossible: `jks`, `pkcs12`, or `pem` |
| `include_system_cas` | Also trust the OS root store |

### Client certificates (mutual TLS)

Pick one source:

| Keys | Format |
|---|---|
| `keystore`, `keystore_password`, optional `key_password`, `key_alias`, `keystore_type` | JKS or PKCS#12 (`.jks`, `.p12`, `.pfx`) |
| `cert_file` + `key_file` | PEM files (unencrypted key, PKCS#1, PKCS#8, or EC) |
| `cert_pem` + `key_pem` | PEM, inline |

### Java keystores

The typical Java client properties translate directly:

| Java client property | kafka-mcp |
|---|---|
| `security.protocol=SSL` | `security_protocol: SSL` |
| `ssl.truststore.location` / `.password` | `tls.truststore` / `tls.truststore_password` |
| `ssl.keystore.location` / `.password` | `tls.keystore` / `tls.keystore_password` |
| `ssl.key.password` | `tls.key_password` (defaults to the keystore password) |
| `ssl.keystore.type` / `ssl.truststore.type` | Detected automatically |
| `ssl.endpoint.identification.algorithm=` (empty) | `tls.verify_hostname: false` |

```yaml
clusters:
  - name: orders
    brokers: ["kafka.example.com:9093"]
    security_protocol: SSL
    tls:
      truststore: ~/certs/truststore/truststore.jks
      truststore_password: ${file:~/certs/truststore/pw.txt}
      keystore: ~/certs/client/keystore.jks
      keystore_password: ${file:~/certs/client/pw.txt}
```

> Since Java 9, `keytool` writes PKCS#12 by default, **even into files named `.jks`**. kafka-mcp inspects the file contents, like Java does, so the extension doesn't matter. JCEKS stores are not supported; convert them with `keytool -importkeystore -srcstoretype JCEKS -deststoretype PKCS12`.

### Hostname verification

By default the broker certificate must match the address being dialed. Many corporate clusters **advertise broker IP addresses or internal names that are not in their certificates**. Bootstrap succeeds, but connecting to individual brokers then fails with:

```text
x509: cannot validate certificate for 10.1.2.3 because it doesn't contain any IP SANs
```

Java clients usually work around this with `ssl.endpoint.identification.algorithm=`. The equivalent here is:

```yaml
tls:
  verify_hostname: false
```

This still verifies the full certificate chain, expiry, and key usage against your trust settings; only the name match is skipped. By contrast, `insecure_skip_verify: true` disables **all** verification and should only be used against throwaway test clusters.

Other options: `server_name` (override SNI and the expected name) and `min_version` (`1.2` by default).

## SASL

Set `security_protocol` to `SASL_SSL` (with a `tls` block) or `SASL_PLAINTEXT`, and add a `sasl` block.

### PLAIN and SCRAM

```yaml
sasl:
  mechanism: SCRAM-SHA-512        # PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512
  username: ${KAFKA_USER}
  password: ${KAFKA_PASSWORD}
```

Confluent Cloud uses `PLAIN` with an API key and secret over `SASL_SSL`; see [examples/confluent-cloud.yaml](../examples/confluent-cloud.yaml).

### OAUTHBEARER

Exactly one token source:

```yaml
sasl:
  mechanism: OAUTHBEARER
  oauth:
    # 1. Client credentials. Tokens are cached and refreshed before expiry.
    token_url: https://login.example.com/oauth2/token
    client_id: ${OAUTH_CLIENT_ID}
    client_secret: ${OAUTH_CLIENT_SECRET}
    scopes: [kafka]
    endpoint_params: {audience: kafka}
    # 2. Or a token file, re-read on every authentication (works with sidecars that rotate it).
    # token_file: /var/run/secrets/kafka/token
    # 3. Or a static token.
    # token: ${KAFKA_TOKEN}
    extensions: {logicalCluster: lkc-123}   # optional SASL extensions
```

### AWS MSK IAM

```yaml
security_protocol: SASL_SSL
tls: {}
sasl:
  mechanism: AWS_MSK_IAM
  aws:
    profile: my-profile      # optional
    region: us-east-1        # optional
    # access_key_id / secret_access_key / session_token for static credentials
```

With no static keys, the standard AWS credential chain is used: environment variables, shared config and credentials files, SSO, web identity (EKS IRSA), and instance or task roles. Credentials refresh automatically.

### Kerberos (GSSAPI)

```yaml
sasl:
  mechanism: GSSAPI
  kerberos:
    service_name: kafka                 # default "kafka"
    realm: CORP.EXAMPLE.COM
    username: svc-reader
    keytab: /etc/security/keytabs/svc-reader.keytab
    # password: ${KRB_PASSWORD}         # instead of a keytab
    # ccache: /tmp/krb5cc_1000          # or reuse a `kinit` ticket cache
    krb5_conf: /etc/krb5.conf           # default: $KRB5_CONFIG or /etc/krb5.conf
    disable_pa_fx_fast: false           # set true for Active Directory setups that require it
```

If you omit `keytab`, `password`, and `ccache`, the cache named by `KRB5CCNAME` is used.

## Schema Registry

Needed to decode Avro, Protobuf, and JSON Schema payloads (Confluent wire format), and for `list_schema_subjects` and `get_schema`.

```yaml
schema_registry:
  url: https://schema-registry.example.com
  username: ${SR_USER}          # basic auth (Confluent Cloud: API key and secret)
  password: ${SR_PASSWORD}
  # bearer_token: ${SR_TOKEN}
  # tls: {...}                  # same keys as the cluster tls block
```

Without its own `tls` block, the registry **inherits the cluster's TLS settings** (CA and client certificate) minus any `server_name` override. That matches deployments where one corporate CA and keystore serve both.

Decoded schemas are cached per schema ID. Lookups that fail are retried at most every 30 seconds, so a scan over millions of records never hammers the registry.

| Schema type | Decoding |
|---|---|
| Avro | Rendered as standard JSON; unions appear as plain values. Schemas that use references are not yet supported. |
| Protobuf | Compiled from the registered schema, including references and well-known imports. Follows the proto3 JSON mapping, so 64-bit integers appear as strings (use `int(value.x)` in filters). |
| JSON Schema | Plain JSON |

## Troubleshooting

Tool errors and `-check` output end with a `Hint:` line for these cases:

| Symptom | Fix |
|---|---|
| `doesn't contain any IP SANs` / `certificate is valid for X, not Y` | `tls.verify_hostname: false` (see [Hostname verification](#hostname-verification)) |
| `certificate signed by unknown authority` | Point `truststore` or `ca_file` at the issuing CA, or set `include_system_cas: true` |
| `got invalid digest` / `pkcs12: decryption password incorrect` | Wrong store password; check the `${file:...}` path |
| `SASL_AUTHENTICATION_FAILED` | Wrong mechanism or credentials |
| `TOPIC_AUTHORIZATION_FAILED` | The principal lacks a broker ACL (e.g. `Read` on the topic); listing may still work because it only needs `Describe` |
| `connection refused` / `i/o timeout` / `no such host` | Network path, VPN, or port (TLS listeners are often `9093`) |
| `first record does not look like a TLS handshake` | Use `SSL`/`SASL_SSL` on TLS ports and `PLAINTEXT`/`SASL_PLAINTEXT` on others |
