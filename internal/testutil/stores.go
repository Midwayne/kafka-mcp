package testutil

import (
	"bytes"
	"crypto/x509"
	"testing"
	"time"

	keystore "github.com/pavlo-v-chernykh/keystore-go/v4"
	"software.sslmate.com/src/go-pkcs12"
)

// WriteJKSTrustStore writes a JKS truststore containing the given CAs.
func WriteJKSTrustStore(tb testing.TB, dir, password string, cas ...Identity) string {
	tb.Helper()

	ks := keystore.New()
	for i, ca := range cas {
		entry := keystore.TrustedCertificateEntry{
			CreationTime: time.Now(),
			Certificate:  keystore.Certificate{Type: "X509", Content: ca.Cert.Raw},
		}

		if err := ks.SetTrustedCertificateEntry(aliasFor(i), entry); err != nil {
			tb.Fatal(err)
		}
	}

	return storeJKS(tb, ks, dir, "truststore.jks", password)
}

// WriteJKSKeyStore writes a JKS keystore holding the leaf key and its chain.
func WriteJKSKeyStore(tb testing.TB, dir, storePassword, keyPassword string, leaf, ca Identity) string {
	tb.Helper()

	der, err := x509.MarshalPKCS8PrivateKey(leaf.Key)
	if err != nil {
		tb.Fatal(err)
	}

	entry := keystore.PrivateKeyEntry{
		CreationTime: time.Now(),
		PrivateKey:   der,
		CertificateChain: []keystore.Certificate{
			{Type: "X509", Content: leaf.Cert.Raw},
			{Type: "X509", Content: ca.Cert.Raw},
		},
	}

	ks := keystore.New()
	if err := ks.SetPrivateKeyEntry("client", entry, []byte(keyPassword)); err != nil {
		tb.Fatal(err)
	}

	return storeJKS(tb, ks, dir, "keystore.jks", storePassword)
}

// WritePKCS12TrustStore writes a PKCS#12 truststore containing the given CAs.
func WritePKCS12TrustStore(tb testing.TB, dir, password string, cas ...Identity) string {
	tb.Helper()

	certs := make([]*x509.Certificate, 0, len(cas))
	for _, ca := range cas {
		certs = append(certs, ca.Cert)
	}

	data, err := pkcs12.Modern.EncodeTrustStore(certs, password)
	if err != nil {
		tb.Fatal(err)
	}

	return WriteFile(tb, dir, "truststore.p12", data)
}

// WritePKCS12KeyStore writes a PKCS#12 keystore holding the leaf key and its chain.
func WritePKCS12KeyStore(tb testing.TB, dir, password string, leaf, ca Identity) string {
	tb.Helper()

	data, err := pkcs12.Modern.Encode(leaf.Key, leaf.Cert, []*x509.Certificate{ca.Cert}, password)
	if err != nil {
		tb.Fatal(err)
	}

	return WriteFile(tb, dir, "keystore.p12", data)
}

func storeJKS(tb testing.TB, ks keystore.KeyStore, dir, name, password string) string {
	tb.Helper()

	var buf bytes.Buffer
	if err := ks.Store(&buf, []byte(password)); err != nil {
		tb.Fatal(err)
	}

	return WriteFile(tb, dir, name, buf.Bytes())
}

func aliasFor(i int) string {
	return "ca-" + string(rune('a'+i))
}
