package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

func TestMechanismNames(t *testing.T) {
	cases := map[string]*config.SASL{
		config.MechPlain:       {Mechanism: config.MechPlain, Username: "u", Password: "p"},
		config.MechScram256:    {Mechanism: config.MechScram256, Username: "u", Password: "p"},
		config.MechScram512:    {Mechanism: config.MechScram512, Username: "u", Password: "p"},
		config.MechOAuthBearer: {Mechanism: config.MechOAuthBearer, OAuth: &config.OAuth{Token: "t"}},
		config.MechAWSMSKIAM:   {Mechanism: config.MechAWSMSKIAM},
		config.MechGSSAPI:      {Mechanism: config.MechGSSAPI, Kerberos: &config.Kerberos{CCache: "/tmp/none"}},
	}

	for want, settings := range cases {
		mechanism, err := Mechanism(settings)
		if err != nil {
			t.Fatalf("%s: %v", want, err)
		}

		if mechanism.Name() != want {
			t.Fatalf("Mechanism(%s).Name() = %s", want, mechanism.Name())
		}
	}
}

func TestMechanismNilAndUnknown(t *testing.T) {
	if mechanism, err := Mechanism(nil); mechanism != nil || err != nil {
		t.Fatalf("Mechanism(nil) = %v, %v", mechanism, err)
	}

	if _, err := Mechanism(&config.SASL{Mechanism: "MAGIC"}); err == nil {
		t.Fatal("expected unsupported mechanism error")
	}
}

func TestFileTokenIsReReadEachTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	source := NewTokenSource(&config.OAuth{TokenFile: path})

	for _, want := range []string{"first", "rotated"} {
		if err := os.WriteFile(path, []byte(want+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}

		got, err := source.Token(context.Background())
		if err != nil || got != want {
			t.Fatalf("Token() = %q, %v; want %q", got, err, want)
		}
	}

	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := source.Token(context.Background()); err == nil {
		t.Fatal("expected empty token file error")
	}
}

func TestClientCredentialsTokenIsCached(t *testing.T) {
	var requests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)

		if err := r.ParseForm(); err != nil || r.Form.Get("audience") != "kafka" {
			http.Error(w, "missing audience", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"abc","token_type":"bearer","expires_in":3600}`))
	}))
	defer server.Close()

	source := NewTokenSource(&config.OAuth{
		TokenURL: server.URL, ClientID: "id", ClientSecret: "secret",
		EndpointParams: map[string]string{"audience": "kafka"},
	})

	for range 3 {
		token, err := source.Token(context.Background())
		if err != nil || token != "abc" {
			t.Fatalf("Token() = %q, %v", token, err)
		}
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("token endpoint called %d times, want 1", got)
	}
}

func TestLazyRetriesFailures(t *testing.T) {
	calls := 0

	value := newLazy(func(context.Context) (int, error) {
		calls++
		if calls == 1 {
			return 0, os.ErrNotExist
		}

		return 42, nil
	})

	if _, err := value.get(context.Background()); err == nil {
		t.Fatal("expected first load to fail")
	}

	for range 2 {
		if got, err := value.get(context.Background()); err != nil || got != 42 {
			t.Fatalf("get() = %d, %v", got, err)
		}
	}

	if calls != 2 {
		t.Fatalf("load called %d times, want 2", calls)
	}
}
