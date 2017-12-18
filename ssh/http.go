// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"net/http"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

// Transport is a convenience function that returns a modified version of
// http.Transport that uses ProxyDialer.
func Transport(config *ssh.ClientConfig) *http.Transport {
	return &http.Transport{
		Dial: ProxyDialer{
			Pool:   DefaultPool,
			Config: config,
		}.Dial,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// NewProductionTransport returns Transport for NewProductionClientConfig.
func NewProductionTransport(user, identityFile string) (*http.Transport, error) {
	cfg, err := NewProductionClientConfig(user, identityFile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create SSH client config")
	}

	return Transport(cfg), nil
}
