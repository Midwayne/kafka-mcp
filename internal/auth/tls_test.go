package auth

import (
	"context"
	"crypto/tls"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Midwayne/kafka-mcp/internal/config"
	"github.com/Midwayne/kafka-mcp/internal/testutil"
)

type pki struct {
	dir                    string
	ca, server, clientLeaf testutil.Identity
}

func newPKI(t *testing.T) pki {
	t.Helper()

	ca := testutil.NewCA(t)

	return pki{dir: t.TempDir(), ca: ca, server: ca.Issue(t, "localhost"), clientLeaf: ca.Issue(t, "client")}
}

// handshake runs a mutual-TLS handshake against a server that requires client certs.
func handshake(t *testing.T, p pki, client *tls.Config) error {
	t.Helper()

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{p.server.TLSCertificate()},
		ClientCAs:    p.ca.Pool(),
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}

		if serverConn, ok := conn.(*tls.Conn); ok {
			_ = serverConn.HandshakeContext(ctx)
		}
		_, _ = io.Copy(io.Discard, conn)
	}()

	dialer := tls.Dialer{Config: client}

	conn, err := dialer.DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		return err
	}

	return conn.Close()
}

func TestTLSConfigSourcesHandshake(t *testing.T) {
	p := newPKI(t)

	cases := map[string]*config.TLS{
		"jks with separate key password": {
			Truststore: testutil.WriteJKSTrustStore(t, p.dir, "trustpw", p.ca), TruststorePassword: "trustpw",
			Keystore:         testutil.WriteJKSKeyStore(t, p.dir, "storepw", "keypw", p.clientLeaf, p.ca),
			KeystorePassword: "storepw", KeyPassword: "keypw",
		},
		"pkcs12": {
			Truststore: testutil.WritePKCS12TrustStore(t, p.dir, "pw", p.ca), TruststorePassword: "pw",
			Keystore: testutil.WritePKCS12KeyStore(t, p.dir, "pw", p.clientLeaf, p.ca), KeystorePassword: "pw",
		},
		"pem files": {
			CAFile:   testutil.WriteFile(t, p.dir, "ca.pem", p.ca.CertPEM()),
			CertFile: testutil.WriteFile(t, p.dir, "client.pem", p.clientLeaf.CertPEM()),
			KeyFile:  testutil.WriteFile(t, p.dir, "client.key", p.clientLeaf.KeyPEM(t)),
		},
		"inline pem": {
			CAPEM: string(p.ca.CertPEM()), CertPEM: string(p.clientLeaf.CertPEM()), KeyPEM: string(p.clientLeaf.KeyPEM(t)),
		},
	}

	for name, settings := range cases {
		t.Run(name, func(t *testing.T) {
			cfg, err := TLSConfig(settings)
			if err != nil {
				t.Fatalf("TLSConfig: %v", err)
			}

			if err := handshake(t, p, cfg); err != nil {
				t.Fatalf("handshake: %v", err)
			}
		})
	}
}

func TestTLSConfigRejectsUntrustedServer(t *testing.T) {
	p := newPKI(t)
	other := testutil.NewCA(t)

	cfg, err := TLSConfig(&config.TLS{
		CAPEM: string(other.CertPEM()), CertPEM: string(p.clientLeaf.CertPEM()), KeyPEM: string(p.clientLeaf.KeyPEM(t)),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := handshake(t, p, cfg); err == nil {
		t.Fatal("expected handshake failure with an untrusted CA")
	}
}

func TestVerifyHostnameFalseStillChecksChain(t *testing.T) {
	p := newPKI(t)
	noHostnameCheck := false

	settings := func(ca testutil.Identity, verify *bool) *config.TLS {
		return &config.TLS{
			ServerName: "wrong.example", VerifyHostname: verify,
			CAPEM: string(ca.CertPEM()), CertPEM: string(p.clientLeaf.CertPEM()), KeyPEM: string(p.clientLeaf.KeyPEM(t)),
		}
	}

	cases := []struct {
		name    string
		tls     *config.TLS
		wantErr bool
	}{
		{"hostname mismatch fails by default", settings(p.ca, nil), true},
		{"hostname check disabled", settings(p.ca, &noHostnameCheck), false},
		{"untrusted chain still fails", settings(testutil.NewCA(t), &noHostnameCheck), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := TLSConfig(tc.tls)
			if err != nil {
				t.Fatal(err)
			}

			if err := handshake(t, p, cfg); (err != nil) != tc.wantErr {
				t.Fatalf("handshake error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestPKCS12NamedJKSIsDetectedByContent(t *testing.T) {
	p := newPKI(t)

	source := testutil.WritePKCS12TrustStore(t, p.dir, "pw", p.ca)
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}

	renamed := testutil.WriteFile(t, p.dir, "truststore.jks", data)

	for _, declared := range []string{"", "jks"} {
		certs, err := LoadTrustStore(renamed, "pw", declared)
		if err != nil || len(certs) != 1 {
			t.Fatalf("declared %q: %d certs, %v", declared, len(certs), err)
		}
	}
}

func TestTrustStoreWithoutPasswordSkipsIntegrityCheck(t *testing.T) {
	p := newPKI(t)
	path := testutil.WriteJKSTrustStore(t, p.dir, "changeit", p.ca)

	certs, err := LoadTrustStore(path, "", "")
	if err != nil || len(certs) != 1 {
		t.Fatalf("LoadTrustStore without password = %d certs, %v", len(certs), err)
	}

	if _, err := LoadTrustStore(path, "wrong", ""); err == nil {
		t.Fatal("expected wrong password to fail the integrity check")
	}
}

func TestStoreErrors(t *testing.T) {
	p := newPKI(t)
	jceks := testutil.WriteFile(t, p.dir, "store.jceks", []byte{0xCE, 0xCE, 0xCE, 0xCE, 0, 0})
	keystorePath := testutil.WriteJKSKeyStore(t, p.dir, "pw", "pw", p.clientLeaf, p.ca)

	cases := map[string]func() error{
		"jceks unsupported": func() error { _, err := LoadTrustStore(jceks, "", ""); return err },
		"unknown type":      func() error { _, err := LoadTrustStore(jceks, "", "bks"); return err },
		"missing file":      func() error { _, err := LoadTrustStore(p.dir+"/nope.jks", "", ""); return err },
		"wrong alias": func() error {
			_, err := LoadKeyStore(keystorePath, "pw", "pw", "missing", "")
			return err
		},
		"wrong key password": func() error {
			_, err := LoadKeyStore(keystorePath, "pw", "bad", "", "")
			return err
		},
	}

	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestTLSConfigDisabledAndVersions(t *testing.T) {
	disabled := false

	cfg, err := TLSConfig(&config.TLS{Enabled: &disabled})
	if err != nil || cfg != nil {
		t.Fatalf("disabled TLS = %v, %v", cfg, err)
	}

	cfg, err = TLSConfig(&config.TLS{MinVersion: "1.3", ServerName: "kafka.internal"})
	if err != nil || cfg.MinVersion != tls.VersionTLS13 || cfg.ServerName != "kafka.internal" || cfg.RootCAs != nil {
		t.Fatalf("unexpected config: %+v, %v", cfg, err)
	}

	if _, err := TLSConfig(&config.TLS{CAPEM: "not a cert"}); err == nil || !strings.Contains(err.Error(), "no PEM") {
		t.Fatalf("expected empty CA error, got %v", err)
	}
}
