// Copyright (C) 2017 ScyllaDB

package main

import (
	"net"
	"net/url"

	"github.com/scylladb/scylla-manager/pkg/config"
)

func urlFromConfig(c config.ServerConfig) string {
	const ipv4Zero, ipv6Zero1, ipv6Zero2 = "0.0.0.0", "::0", "::"
	const ipv4Localhost, ipv6Localhost = "127.0.0.1", "::1"

	var addr, scheme string
	if c.HTTP != "" {
		addr, scheme = c.HTTP, "http"
	} else {
		addr, scheme = c.HTTPS, "https"
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
