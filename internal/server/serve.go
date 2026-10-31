package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/Midwayne/kafka-mcp/internal/config"
)

const shutdownGrace = 5 * time.Second

// Serve runs the MCP server over the configured transport until ctx ends.
func Serve(ctx context.Context, server *mcpserver.MCPServer, cfg config.Server) error {
	if cfg.ServeMode == "stdio" {
		return mcpserver.ServeStdio(server)
	}

	var handler http.Handler = mcpserver.NewStreamableHTTPServer(server)
	if cfg.ServeMode == "sse" {
		handler = mcpserver.NewSSEServer(server)
	}

	return serveHTTP(ctx, requireBearer(cfg.BearerToken, handler), net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
}

func serveHTTP(ctx context.Context, handler http.Handler, address string) error {
	httpServer := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 10 * time.Second}

	errs := make(chan error, 1)
	go func() { errs <- httpServer.ListenAndServe() }()

	select {
	case err := <-errs:
		return fmt.Errorf("serve %s: %w", address, err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("shutdown: %w", err)
		}

		return nil
	}
}

// requireBearer rejects requests without the configured token. An empty token disables the check.
func requireBearer(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}

	expected := []byte("Bearer " + token)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), expected) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)

			return
		}

		next.ServeHTTP(w, r)
	})
}
