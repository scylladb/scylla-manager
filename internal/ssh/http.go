// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"net/http"
	"time"
)

// NewTransport returns a modified version of http.Transport that uses SSH
// tunnelling to connect to host. When connecting to HOST:PORT we establish
// SSH connection to HOST and then proxy the connection to localhost:PORT.
func NewTransport(config Config) *http.Transport {
	return &http.Transport{
		DialContext: proxyDialer{
			dialContext: ContextDialer(DefaultDialer),
			config:      config,
		}.DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}
