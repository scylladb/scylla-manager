// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/cqlping"
	"github.com/scylladb/mermaid/internal/kv"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

const (
	pingLaps = 3
)

// ClusterNameFunc returns name for a given ID.
type ClusterNameFunc func(ctx context.Context, clusterID uuid.UUID) (string, error)

// Service manages health checks.
type Service struct {
	config       Config
	clusterName  ClusterNameFunc
	scyllaClient scyllaclient.ProviderFunc
	sslCertStore kv.Store
	sslKeyStore  kv.Store
	cache        map[uuid.UUID]*tls.Config
	cacheMu      sync.Mutex
	logger       log.Logger
}

func NewService(config Config, clusterName ClusterNameFunc, scyllaClient scyllaclient.ProviderFunc,
	sslCertStore, sslKeyStore kv.Store, logger log.Logger) (*Service, error) {
	if clusterName == nil {
		return nil, errors.New("invalid cluster name provider")
	}
	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}
	if sslCertStore == nil {
		return nil, errors.New("missing SSL cert store")
	}
	if sslKeyStore == nil {
		return nil, errors.New("missing SSL key store")
	}

	return &Service{
		config:       config,
		clusterName:  clusterName,
		scyllaClient: scyllaClient,
		sslCertStore: sslCertStore,
		sslKeyStore:  sslKeyStore,
		cache:        make(map[uuid.UUID]*tls.Config),
		logger:       logger,
	}, nil
}

// CQLRunner creates a Runner that performs health checks for CQL connectivity.
func (s *Service) CQLRunner() Runner {
	return Runner{
		scyllaClient: s.scyllaClient,
		clusterName:  s.clusterName,
		status:       cqlStatus,
		rtt:          cqlRTT,
		ping:         s.pingCQL,
	}
}

// RESTRunner creates a Runner that performs health checks for REST API
// connectivity.
func (s *Service) RESTRunner() Runner {
	return Runner{
		scyllaClient: s.scyllaClient,
		clusterName:  s.clusterName,
		status:       restStatus,
		rtt:          restRTT,
		ping:         s.pingREST,
	}
}

type pingStat struct {
	status string
	rtt    float64
}

type pingFunc func(ctx context.Context, clusterID uuid.UUID, host string) (rtt time.Duration, err error)

// GetStatus returns the current status of the supplied cluster.
func (s *Service) GetStatus(ctx context.Context, clusterID uuid.UUID) ([]Status, error) {
	s.logger.Debug(ctx, "GetStatus", "cluster_id", clusterID)

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get client for cluster with id %s", clusterID)
	}

	dcs, err := client.Datacenters(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get dcs for cluster with id %s", clusterID)
	}

	check := func(host, name string, ping pingFunc, q chan pingStat) {
		status := pingStat{
			status: statusUp,
		}
		s.logger.Debug(ctx, fmt.Sprintf("pinging %s", name),
			"cluster_id", clusterID,
			"host", host,
		)
		rtt, err := ping(ctx, clusterID, host)
		if err != nil {
			s.logger.Error(ctx, fmt.Sprintf("%s ping failed", name),
				"cluster_id", clusterID,
				"host", host,
				"error", err,
			)
			status.status = statusDown
		}
		status.rtt = float64(rtt / 1000000)

		select {
		case q <- status:
		case <-ctx.Done():
		}
	}

	out := make(chan Status, runtime.NumCPU()+1)

	size := 0
	for dc, hosts := range dcs {
		for _, h := range hosts {
			size++

			cqlQ := make(chan pingStat)
			restQ := make(chan pingStat)
			go check(h, "CQL", s.pingCQL, cqlQ)
			go check(h, "REST", s.pingREST, restQ)

			go func(dc, h string) {
				var cqlStatus, restStatus pingStat
				for cqlStatus.status == "" || restStatus.status == "" {
					select {
					case cqlStatus = <-cqlQ:
						continue
					case restStatus = <-restQ:
						continue
					case <-ctx.Done():
						s.logger.Error(ctx, "status check canceled",
							"cluster_id", clusterID,
							"host", h,
							"error", ctx.Err(),
						)
						return
					}
				}
				out <- Status{
					DC:         dc,
					Host:       h,
					SSL:        s.hasTLSConfig(clusterID),
					CQLStatus:  cqlStatus.status,
					CQLRtt:     cqlStatus.rtt,
					RESTStatus: restStatus.status,
					RESTRtt:    restStatus.rtt,
				}
			}(dc, h)
		}
	}

	statuses := make([]Status, size)
	for i := 0; i < size; i++ {
		select {
		case statuses[i] = <-out:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].DC == statuses[j].DC {
			return statuses[i].Host < statuses[j].Host
		}
		return statuses[i].DC < statuses[j].DC
	})

	return statuses, nil
}

func (s *Service) pingCQL(ctx context.Context, clusterID uuid.UUID, host string) (rtt time.Duration, err error) {
	tlsConfig, err := s.tlsConfig(ctx, clusterID)
	if err != nil {
		return 0, err
	}

	config := cqlping.Config{
		Addr:      fmt.Sprint(host, ":", DefaultPort),
		Timeout:   s.config.Timeout,
		TLSConfig: tlsConfig,
	}
	if tlsConfig != nil {
		config.Timeout = s.config.SSLTimeout
	}
	rtt, err = cqlping.Ping(ctx, config)

	// If connection was cut by the server try upgrading to TLS.
	if errors.Cause(err) == io.EOF && config.TLSConfig == nil {
		s.logger.Info(ctx, "Upgrading connection to TLS",
			"cluster_id", clusterID,
			"host", host,
		)
		config.TLSConfig = DefaultTLSConfig
		rtt, err = cqlping.Ping(ctx, config)
		if err == nil {
			s.setTLSConfig(clusterID, DefaultTLSConfig)
		}
	}

	return
}

func (s *Service) pingREST(ctx context.Context, clusterID uuid.UUID, host string) (time.Duration, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get client for cluster with id %s", clusterID)
	}

	return client.PingN(ctx, host, pingLaps)
}

func (s *Service) tlsConfig(ctx context.Context, clusterID uuid.UUID) (*tls.Config, error) {
	// Try loading from cache.
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	if c, ok := s.cache[clusterID]; ok {
		return c, nil
	}

	s.logger.Info(ctx, "Loading SSL certificate from secure store", "cluster_id", clusterID)
	cert, err := s.sslCertStore.Get(clusterID)
	// If there is no user certificate record no TLS config to avoid rereading
	// from a secure store.
	if err == mermaid.ErrNotFound {
		s.cache[clusterID] = nil
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to get SSL user cert from a secure store")
	}
	key, err := s.sslKeyStore.Get(clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get SSL user key from a secure store")
	}
	keyPair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, errors.Wrap(err, "invalid SSL user key pair")
	}

	// Create a new TLS configuration with user certificate and cache
	// the configuration.
	cfg := &tls.Config{
		Certificates:       []tls.Certificate{keyPair},
		InsecureSkipVerify: true,
	}
	s.cache[clusterID] = cfg

	return cfg, nil
}

func (s *Service) setTLSConfig(clusterID uuid.UUID, config *tls.Config) {
	s.cacheMu.Lock()
	s.cache[clusterID] = config
	s.cacheMu.Unlock()
}

func (s *Service) hasTLSConfig(clusterID uuid.UUID) bool {
	s.cacheMu.Lock()
	c := s.cache[clusterID]
	s.cacheMu.Unlock()
	return c != nil
}

// InvalidateTLSConfigCache frees all in-memory TLS configuration associated
// with a given cluster forcing reload from a key store with next usage.
func (s *Service) InvalidateTLSConfigCache(clusterID uuid.UUID) {
	s.cacheMu.Lock()
	delete(s.cache, clusterID)
	s.cacheMu.Unlock()
}
