// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"io/ioutil"
	"net"
	"net/http"
	"path"
	"runtime"
	"time"

	"github.com/scylladb/mermaid/internal/fsutil"
	"github.com/scylladb/mermaid/internal/ssh"
)

// NewSSHTransport returns http.RoundTripper suitable for usage with test cluster.
func NewSSHTransport() *http.Transport {
	dialer := ssh.NewProxyDialer(sshAsScyllaManager(), ssh.ContextDialer(&net.Dialer{}))

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

func sshAsScyllaManager() ssh.Config {
	b, err := ioutil.ReadFile(path.Join(fsutil.HomeDir(), "/.ssh/scylla-manager.pem"))
	if err != nil {
		panic(err)
	}
	config, err := ssh.DefaultConfig().WithIdentityFileAuth("scylla-manager", b)
	if err != nil {
		panic(err)
	}
	return config
}
