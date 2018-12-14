// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"net/http"

	"github.com/scylladb/mermaid/internal/ssh"
)

// NewSSHTransport returns http.RoundTripper suitable for usage with test cluster.
func NewSSHTransport() *http.Transport {
	return ssh.NewTransport(sshConfig())
}

func sshConfig() ssh.Config {
	return ssh.DefaultConfig().WithPasswordAuth("root", "root")
}
