// Copyright (C) 2017 ScyllaDB

package main

import (
	"net"
	"net/url"

	"github.com/scylladb/mermaid/pkg/cmd/scylla-manager/config"
)

func urlFromConfig(cfg *config.ServerConfig) string {
	const ipv4Zero, ipv6Zero1, ipv6Zero2 = "0.0.0.0", "::0", "::"
	const ipv4Localhost, ipv6Localhost = "127.0.0.1", "::1"

	addr := cfg.HTTP
	scheme := "http"
	if cfg.HTTPS != "" {
		addr = cfg.HTTPS
		scheme = "https"
	}
	if addr == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}

	switch host {
	case "":
		host = ipv4Localhost
	case ipv6Zero1, ipv6Zero2:
		host = ipv6Localhost
	case ipv4Zero:
		host = ipv4Localhost
	}

	return (&url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, port),
		Path:   "/api/v1",
	}).String()
}
