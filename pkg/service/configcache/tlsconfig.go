// Copyright (C) 2024 ScyllaDB

package configcache

import (
	"crypto/tls"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
)

// TLSConfigWithAddress is a concatenation of tls.Config and Address.
type TLSConfigWithAddress struct {
	*tls.Config

	Address string
}

func newCQLTLSConfigIfEnabled(c *cluster.Cluster, nodeInfo *scyllaclient.NodeInfo, secretsStore store.Store,
	host string,
) (*TLSConfigWithAddress, error) {
	cqlTLSEnabled, cqlClientCertAuth := nodeInfo.CQLTLSEnabled()
	if !cqlTLSEnabled || c.ForceTLSDisabled {
		return nil, nil // nolint: nilnil
	}
	cqlAddress := nodeInfo.CQLAddr(host, c.ForceTLSDisabled || c.ForceNonSSLSessionPort)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	if cqlClientCertAuth {
		cert, err := prepareCertificates(c, secretsStore)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create TLS configuration for CQL session")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return &TLSConfigWithAddress{
		Address: cqlAddress,
		Config:  tlsConfig,
	}, nil
}

func newAlternatorTLSConfigIfEnabled(c *cluster.Cluster, nodeInfo *scyllaclient.NodeInfo, secretsStore store.Store,
	host string,
) (*TLSConfigWithAddress, error) {
	alternatorTLSEnabled, alternatorClientCertAuth := nodeInfo.AlternatorTLSEnabled()
	if !alternatorTLSEnabled {
		return nil, nil // nolint: nilnil
	}
	alternatorAddress := nodeInfo.AlternatorAddr(host)

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	if alternatorClientCertAuth {
		cert, err := prepareCertificates(c, secretsStore)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create TLS configuration for Alternator session")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return &TLSConfigWithAddress{
		Address: alternatorAddress,
		Config:  tlsConfig,
	}, nil
}

func prepareCertificates(c *cluster.Cluster, secretsStore store.Store) (cert tls.Certificate, err error) {
	id := &secrets.TLSIdentity{
		ClusterID: c.ID,
	}
	if err := secretsStore.Get(id); err != nil {
		if !errors.Is(err, util.ErrNotFound) {
			return tls.Certificate{}, errors.Wrap(err, "fetch TLS config")
		}
		return tls.Certificate{}, errors.Wrap(err, "client encryption is enabled, but certificate is missing")
	}

	keyPair, err := tls.X509KeyPair(id.Cert, id.PrivateKey)
	if err != nil {
		return tls.Certificate{}, errors.Wrap(err, "invalid SSL user key pair")
	}
	return keyPair, nil
}
