// Copyright (C) 2017 ScyllaDB

package config

import (
	"crypto/tls"

	"github.com/pkg/errors"
)

// TLSVersion specifies TLS version and cipher suites providing a human
// friendly way of configuring TLS for HTTPS servers.
// It is based on https://ssl-config.mozilla.org/#server=go.
type TLSVersion string

// TLSVersion enumeration.
const (
	TLSv13 TLSVersion = "TLSv1.3"
	TLSv12 TLSVersion = "TLSv1.2"
	TLSv10 TLSVersion = "TLSv1.0"
)

func (m TLSVersion) MarshalText() (text []byte, err error) {
	return []byte(m), nil
}

func (m *TLSVersion) UnmarshalText(text []byte) error {
	switch TLSVersion(text) {
	case TLSv13:
		*m = TLSv13
	case TLSv12:
		*m = TLSv12
	case TLSv10:
		*m = TLSv10
	default:
		return errors.Errorf("unsupported TLS version %q", string(text))
	}

	return nil
}

// TLSConfig returns real TLS config for a given mode.
func (m TLSVersion) TLSConfig() *tls.Config {
	var cfg *tls.Config

	switch m {
	case TLSv13:
		cfg = &tls.Config{
			MinVersion: tls.VersionTLS13,
		}
	case TLSv12:
		cfg = &tls.Config{
			MinVersion: tls.VersionTLS12,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			},
		}
	case TLSv10:
		cfg = &tls.Config{
			MinVersion:               tls.VersionTLS10,
			PreferServerCipherSuites: true,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_RSA_WITH_AES_128_CBC_SHA,
				tls.TLS_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
			},
		}
	}

	return cfg
}
