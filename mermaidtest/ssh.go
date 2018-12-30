// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/scylladb/mermaid/internal/ssh"
)

// NewSSHTransport returns http.RoundTripper suitable for usage with test cluster.
func NewSSHTransport() *http.Transport {
	dialer := ssh.NewProxyDialer(sshConfig(), ssh.ContextDialer(&net.Dialer{}))

	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
	}

	return transport
}

func sshConfig() ssh.Config {
	return ssh.DefaultConfig().WithPasswordAuth("root", "root")
}
