package auth

import (
	"context"
	"fmt"

	krbclient "github.com/jcmturner/gokrb5/v8/client"
	krbconfig "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

func buildKerberos(s *config.SASL) (sasl.Mechanism, error) {
	settings := *s.Kerberos

	client := newLazy(func(context.Context) (*krbclient.Client, error) {
		return kerberosClient(settings)
	})

	return kerberos.Kerberos(func(ctx context.Context) (kerberos.Auth, error) {
		cl, err := client.get(ctx)
		if err != nil {
			return kerberos.Auth{}, err
		}

		return kerberos.Auth{Client: cl, Service: settings.ServiceName, PersistAfterAuth: true}, nil
	}), nil
}

// kerberosClient logs in with a keytab, a password, or an existing kinit credential cache.
func kerberosClient(k config.Kerberos) (*krbclient.Client, error) {
	krb5, err := krbconfig.Load(k.Krb5Conf)
	if err != nil {
		return nil, fmt.Errorf("load krb5 config %s: %w", k.Krb5Conf, err)
	}

	opts := []func(*krbclient.Settings){krbclient.DisablePAFXFAST(k.DisablePAFXFAST)}

	var cl *krbclient.Client

	switch {
	case k.Keytab != "":
		kt, err := keytab.Load(config.ExpandHome(k.Keytab))
		if err != nil {
			return nil, fmt.Errorf("load keytab: %w", err)
		}
		cl = krbclient.NewWithKeytab(k.Username, k.Realm, kt, krb5, opts...)

	case k.Password != "":
		cl = krbclient.NewWithPassword(k.Username, k.Realm, k.Password, krb5, opts...)

	default:
		return ccacheClient(k.CCache, krb5, opts)
	}

	if err := cl.Login(); err != nil {
		return nil, fmt.Errorf("kerberos login: %w", err)
	}

	return cl, nil
}

func ccacheClient(path string, krb5 *krbconfig.Config, opts []func(*krbclient.Settings)) (*krbclient.Client, error) {
	cache, err := credentials.LoadCCache(config.ExpandHome(path))
	if err != nil {
		return nil, fmt.Errorf("load kerberos ccache %s (run kinit?): %w", path, err)
	}

	cl, err := krbclient.NewFromCCache(cache, krb5, opts...)
	if err != nil {
		return nil, fmt.Errorf("kerberos ccache client: %w", err)
	}

	return cl, nil
}
