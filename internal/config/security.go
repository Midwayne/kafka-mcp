package config

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
)

// SASL mechanism names after normalization.
const (
	MechPlain       = "PLAIN"
	MechScram256    = "SCRAM-SHA-256"
	MechScram512    = "SCRAM-SHA-512"
	MechOAuthBearer = "OAUTHBEARER"
	MechAWSMSKIAM   = "AWS_MSK_IAM"
	MechGSSAPI      = "GSSAPI"
)

var mechanismAliases = map[string]string{
	"PLAIN": MechPlain, "SCRAM-SHA-256": MechScram256, "SCRAM-SHA-512": MechScram512,
	"SCRAM_SHA_256": MechScram256, "SCRAM_SHA_512": MechScram512,
	"OAUTHBEARER": MechOAuthBearer, "OAUTH": MechOAuthBearer,
	"AWS_MSK_IAM": MechAWSMSKIAM, "AWS-MSK-IAM": MechAWSMSKIAM, "MSK_IAM": MechAWSMSKIAM, "IAM": MechAWSMSKIAM,
	"GSSAPI": MechGSSAPI, "KERBEROS": MechGSSAPI,
}

func (t *TLS) validate() error {
	if t == nil {
		return nil
	}

	if (t.CertFile == "") != (t.KeyFile == "") {
		return errors.New("cert_file and key_file must be set together")
	}

	if (t.CertPEM == "") != (t.KeyPEM == "") {
		return errors.New("cert_pem and key_pem must be set together")
	}

	clientSources := 0
	for _, set := range []bool{t.CertFile != "", t.CertPEM != "", t.Keystore != ""} {
		if set {
			clientSources++
		}
	}

	if clientSources > 1 {
		return errors.New("choose one client certificate source: cert_file, cert_pem, or keystore")
	}

	switch t.MinVersion {
	case "", "1.0", "1.1", "1.2", "1.3":
	default:
		return fmt.Errorf("min_version %q must be 1.0, 1.1, 1.2, or 1.3", t.MinVersion)
	}

	return nil
}

func (s *SASL) normalize() error {
	if s == nil {
		return nil
	}

	mechanism, ok := mechanismAliases[strings.ToUpper(strings.TrimSpace(s.Mechanism))]
	if !ok {
		return fmt.Errorf("sasl.mechanism %q is not supported (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, AWS_MSK_IAM, GSSAPI)", s.Mechanism)
	}
	s.Mechanism = mechanism

	switch mechanism {
	case MechPlain, MechScram256, MechScram512:
		if s.Username == "" || s.Password == "" {
			return fmt.Errorf("sasl %s requires username and password", mechanism)
		}
	case MechOAuthBearer:
		return s.OAuth.validate()
	case MechAWSMSKIAM:
		if s.AWS != nil && (s.AWS.AccessKeyID == "") != (s.AWS.SecretAccessKey == "") {
			return errors.New("sasl.aws access_key_id and secret_access_key must be set together")
		}
	case MechGSSAPI:
		return s.normalizeKerberos()
	}

	return nil
}

func (o *OAuth) validate() error {
	if o == nil {
		return errors.New("sasl OAUTHBEARER requires an oauth block")
	}

	sources := 0
	if o.Token != "" {
		sources++
	}
	if o.TokenFile != "" {
		sources++
	}
	if o.TokenURL != "" {
		sources++
		if o.ClientID == "" || o.ClientSecret == "" {
			return errors.New("sasl.oauth token_url requires client_id and client_secret")
		}
	}

	if sources != 1 {
		return errors.New("sasl.oauth needs exactly one of token, token_file, or token_url")
	}

	return nil
}

func (s *SASL) normalizeKerberos() error {
	if s.Kerberos == nil {
		s.Kerberos = &Kerberos{}
	}
	k := s.Kerberos

	k.ServiceName = orString(k.ServiceName, "kafka")
	k.Krb5Conf = ExpandHome(orString(k.Krb5Conf, orString(os.Getenv("KRB5_CONFIG"), "/etc/krb5.conf")))
	k.Username = orString(k.Username, s.Username)
	k.Password = orString(k.Password, s.Password)

	if k.Keytab == "" && k.Password == "" && k.CCache == "" {
		k.CCache = strings.TrimPrefix(os.Getenv("KRB5CCNAME"), "FILE:")
	}

	if k.Keytab == "" && k.Password == "" && k.CCache == "" {
		return errors.New("sasl.kerberos needs keytab, password, or ccache (or KRB5CCNAME)")
	}

	if k.CCache == "" && (k.Username == "" || k.Realm == "") {
		return errors.New("sasl.kerberos keytab/password login requires username and realm")
	}

	return nil
}

func (r *SchemaRegistry) normalize(cluster string) error {
	if r == nil {
		return nil
	}

	if r.URL != "" {
		r.URLs = append([]string{r.URL}, r.URLs...)
	}

	if len(r.URLs) == 0 {
		return fmt.Errorf("cluster %q schema_registry requires url", cluster)
	}

	if r.BearerToken != "" && r.Username != "" {
		return fmt.Errorf("cluster %q schema_registry: choose basic auth or bearer_token", cluster)
	}

	return r.TLS.validate()
}

func validateGlobs(filter NameFilter) error {
	for _, pattern := range append(append([]string{}, filter.Include...), filter.Exclude...) {
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid glob %q", pattern)
		}
	}

	return nil
}

// Allows reports whether name passes the include/exclude globs.
// An empty include list allows everything not excluded.
func (f NameFilter) Allows(name string) bool {
	for _, pattern := range f.Exclude {
		if ok, _ := path.Match(pattern, name); ok {
			return false
		}
	}

	if len(f.Include) == 0 {
		return true
	}

	for _, pattern := range f.Include {
		if ok, _ := path.Match(pattern, name); ok {
			return true
		}
	}

	return false
}
