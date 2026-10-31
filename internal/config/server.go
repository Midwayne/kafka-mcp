package config

import (
	"fmt"
	"net"
	"strings"
)

func (s *Server) normalize() error {
	s.Name = orString(s.Name, DefaultServerName)
	s.Version = orString(s.Version, DefaultServerVersion)
	s.Host = orString(s.Host, DefaultHost)
	s.StateDir = ExpandHome(orString(s.StateDir, DefaultStateDir))

	if s.Port == 0 {
		s.Port = DefaultPort
	}

	s.ServeMode = strings.ToLower(orString(s.ServeMode, DefaultServeMode))
	switch s.ServeMode {
	case "stdio":
		return nil
	case "http", "sse":
		return s.validateNetwork()
	default:
		return fmt.Errorf("server.serve_mode %q must be stdio, http, or sse", s.ServeMode)
	}
}

// validateNetwork refuses to expose the server beyond loopback without a token.
func (s *Server) validateNetwork() error {
	if s.BearerToken != "" {
		return nil
	}

	if ip := net.ParseIP(s.Host); s.Host == "localhost" || (ip != nil && ip.IsLoopback()) {
		return nil
	}

	return fmt.Errorf("server.host %q is reachable from the network; set server.bearer_token", s.Host)
}
