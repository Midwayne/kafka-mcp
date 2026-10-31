package auth

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

// TokenSource yields bearer tokens for OAUTHBEARER.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

type staticToken string

func (t staticToken) Token(context.Context) (string, error) { return string(t), nil }

// fileToken re-reads the file on every authentication so rotated tokens are picked up.
type fileToken string

func (f fileToken) Token(context.Context) (string, error) {
	data, err := os.ReadFile(config.ExpandHome(string(f)))
	if err != nil {
		return "", fmt.Errorf("read token_file: %w", err)
	}

	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", errors.New("token_file is empty")
	}

	return token, nil
}

// cachedToken wraps an oauth2 source that caches and refreshes tokens.
type cachedToken struct{ source oauth2.TokenSource }

func (c cachedToken) Token(context.Context) (string, error) {
	token, err := c.source.Token()
	if err != nil {
		return "", fmt.Errorf("fetch client-credentials token: %w", err)
	}

	return token.AccessToken, nil
}

// NewTokenSource picks the token source described by an oauth block.
func NewTokenSource(o *config.OAuth) TokenSource {
	switch {
	case o.Token != "":
		return staticToken(o.Token)
	case o.TokenFile != "":
		return fileToken(o.TokenFile)
	default:
		cc := clientcredentials.Config{
			ClientID:       o.ClientID,
			ClientSecret:   o.ClientSecret,
			TokenURL:       o.TokenURL,
			Scopes:         o.Scopes,
			EndpointParams: endpointParams(o.EndpointParams),
		}

		return cachedToken{source: cc.TokenSource(context.Background())}
	}
}

func endpointParams(params map[string]string) url.Values {
	if len(params) == 0 {
		return nil
	}

	values := url.Values{}
	for key, value := range params {
		values.Set(key, value)
	}

	return values
}

func buildOAuth(s *config.SASL) (sasl.Mechanism, error) {
	source := NewTokenSource(s.OAuth)
	extensions := s.OAuth.Extensions

	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		token, err := source.Token(ctx)
		if err != nil {
			return oauth.Auth{}, err
		}

		return oauth.Auth{Token: token, Extensions: extensions}, nil
	}), nil
}
