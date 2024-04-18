// Copyright (C) 2024 ScyllaDB

package configcache

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// ConnectionType defines an enum for different types of configuration.
type ConnectionType int

const (
	// CQL defines cql connection type.
	CQL ConnectionType = iota
	// Alternator defines alternator connection type.
	Alternator

	updateFrequency = 5 * time.Minute
)

type tlsConfigWithAddress struct {
	*tls.Config
	Address string
}

// NodeConfig keeps the node current node configuration together with the TLS details per different type of connection.
type NodeConfig struct {
	*scyllaclient.NodeInfo
	TLSConfig map[ConnectionType]*tlsConfigWithAddress
}

// ConfigCacher is the interface defining the cache behavior.
type ConfigCacher interface {
	Read(clusterID uuid.UUID, host string) (NodeConfig, error)
}

// Service is responsible for handling all cluster configuration cache related operations.
// Use svc.Read(clusterID, host) to read the configuration of particular host in given cluster.
// Use svc.Run() to let the cache update itself periodically with the current configuration.
//
// Current implementation doesn't care about changing IP, what means that the configuration for given cluster and host
// will stay there forever.
type Service struct {
	clusterSvc   cluster.Servicer
	scyllaClient scyllaclient.ProviderFunc
	secretsStore store.Store

	configs sync.Map
	logger  log.Logger
}

// Read returns either the host configuration that is currently stored in the cache,
// ErrNoClusterConfig if config for the whole cluster doesn't exist,
// or ErrNoHostConfig if config of the particular host doesn't exist.
func (svc *Service) Read(clusterID uuid.UUID, host string) (NodeConfig, error) {
	emptyConfig := NodeConfig{}

	clusterConfig, err := svc.readClusterConfig(clusterID)
	if err != nil {
		return emptyConfig, err
	}

	rawHostConfig, ok := clusterConfig.Load(host)
	if !ok {
		return emptyConfig, ErrNoHostConfig
	}
	hostConfig, ok := rawHostConfig.(NodeConfig)
	if !ok {
		panic("cluster host emptyConfig cache stores unexpected type")
	}
	return hostConfig, nil
}

func (svc *Service) readClusterConfig(clusterID uuid.UUID) (*sync.Map, error) {
	emptyConfig := &sync.Map{}

	rawClusterConfig, ok := svc.configs.Load(clusterID)
	if !ok {
		return emptyConfig, ErrNoClusterConfig
	}
	clusterConfig, ok := rawClusterConfig.(*sync.Map)
	if !ok {
		panic("cluster cache emptyConfig stores unexpected type")
	}
	return clusterConfig, nil
}

// Run starts the infinity loop responsible for updating the clusters configuration periodically.
func (svc *Service) Run(ctx context.Context) {
	freq := time.NewTicker(updateFrequency)

	for {
		// make sure to shut down when the context is cancelled
		select {
		case <-ctx.Done():
			return
		default:
		}

		select {
		case <-freq.C:
			svc.update(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (svc *Service) update(ctx context.Context) {
	clusters, err := svc.clusterSvc.ListClusters(ctx, nil)
	if err != nil {
		svc.logger.Error(ctx, "Couldn't list clusters.", "error", err)
		return
	}

	for _, c := range clusters {
		c := c
		perClusterLogger := svc.logger.Named("Cluster config update").With("cluster", c.ID)
		currentClusterConfig, err := svc.readClusterConfig(c.ID)
		if err != nil && errors.Is(err, ErrNoClusterConfig) {
			perClusterLogger.Error(ctx, "Couldn't read cluster config from cache.", "error", err)
			continue
		}

		go func() {
			client, err := svc.scyllaClient(ctx, c.ID)
			if err != nil {
				perClusterLogger.Error(ctx, "Couldn't create scylla client.", "cluster", c.ID, "error", err)
				return
			}

			// Hosts that are going to be asked about the configuration are exactly the same as
			// the ones used by the scylla client.
			hostsWg := sync.WaitGroup{}
			for _, host := range client.Config().Hosts {
				hostsWg.Add(1)
				host := host
				perHostLogger := perClusterLogger.Named("Cluster host config update").With("host", host)
				go func() {
					defer hostsWg.Done()

					config, err := svc.retrieveClusterHostConfig(ctx, host, client, c)
					if err != nil {
						perHostLogger.Error(ctx, "Couldn't read cluster host config.", "error", err)
						return
					}
					currentClusterConfig.Store(host, config)
				}()
			}
			hostsWg.Wait()
		}()
	}
}

func (svc *Service) retrieveClusterHostConfig(ctx context.Context, host string, client *scyllaclient.Client,
	c *cluster.Cluster,
) (*NodeConfig, error) {
	config := &NodeConfig{}

	nodeInfoResp, err := client.NodeInfo(ctx, host)
	if err != nil {
		return config, errors.Wrap(err, "fetch node info")
	}

	config.TLSConfig = make(map[ConnectionType]*tlsConfigWithAddress, 2)
	for _, p := range []ConnectionType{CQL, Alternator} {
		var tlsEnabled, clientCertAuth bool
		var address string
		if p == CQL {
			address = nodeInfoResp.CQLAddr(host)
			tlsEnabled, clientCertAuth = nodeInfoResp.CQLTLSEnabled()
			tlsEnabled = tlsEnabled && !c.ForceTLSDisabled
			if tlsEnabled && !c.ForceNonSSLSessionPort {
				address = nodeInfoResp.CQLSSLAddr(host)
			}
		} else if p == Alternator {
			tlsEnabled, clientCertAuth = nodeInfoResp.AlternatorTLSEnabled()
			address = nodeInfoResp.AlternatorAddr(host)
		}
		if tlsEnabled {
			tlsConfig, err := svc.tlsConfig(c.ID, clientCertAuth)
			if err != nil && !errors.Is(err, service.ErrNotFound) {
				return config, errors.Wrap(err, "fetch TLS config")
			}
			if clientCertAuth && errors.Is(err, service.ErrNotFound) {
				return config, errors.Wrap(err, "client encryption is enabled, but certificate is missing")
			}
			config.TLSConfig[p] = &tlsConfigWithAddress{
				Config:  tlsConfig,
				Address: address,
			}
		}
	}

	return config, nil
}

func (svc *Service) tlsConfig(clusterID uuid.UUID, clientCertAuth bool) (*tls.Config, error) {
	cfg := tls.Config{
		InsecureSkipVerify: true,
	}

	if clientCertAuth {
		id := &secrets.TLSIdentity{
			ClusterID: clusterID,
		}
		if err := svc.secretsStore.Get(id); err != nil {
			return nil, errors.Wrap(err, "get SSL user cert from secrets store")
		}
		keyPair, err := tls.X509KeyPair(id.Cert, id.PrivateKey)
		if err != nil {
			return nil, errors.Wrap(err, "invalid SSL user key pair")
		}
		cfg.Certificates = []tls.Certificate{keyPair}
	}

	return &cfg, nil
}
