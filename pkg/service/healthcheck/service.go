// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/cqlping"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/service/secrets"
	"github.com/scylladb/mermaid/pkg/util/httpmw"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

const (
	pingLaps = 3
)

// ClusterNameFunc returns name for a given ID.
type ClusterNameFunc func(ctx context.Context, clusterID uuid.UUID) (string, error)

type clusterIDHost struct {
	ClusterID uuid.UUID
	Host      string
}

// Service manages health checks.
type Service struct {
	config       Config
	clusterName  ClusterNameFunc
	scyllaClient scyllaclient.ProviderFunc
	secretsStore secrets.Store

	cacheMu sync.Mutex
	// fields below are protected by cacheMu
	tlsCache      map[uuid.UUID]*tls.Config
	nodeInfoCache map[clusterIDHost]*scyllaclient.NodeInfo

	logger log.Logger
}

func NewService(config Config, clusterName ClusterNameFunc, scyllaClient scyllaclient.ProviderFunc,
	secretsStore secrets.Store, logger log.Logger) (*Service, error) {
	if clusterName == nil {
		return nil, errors.New("invalid cluster name provider")
	}
	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		config:        config,
		clusterName:   clusterName,
		scyllaClient:  scyllaClient,
		secretsStore:  secretsStore,
		tlsCache:      make(map[uuid.UUID]*tls.Config),
		nodeInfoCache: make(map[clusterIDHost]*scyllaclient.NodeInfo),
		logger:        logger,
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
		return nil, errors.Wrapf(err, "get client for cluster with id %s", clusterID)
	}

	dcs, err := client.Datacenters(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "get hosts for cluster with id %s", clusterID)
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
						s.logger.Error(ctx, "Status check canceled",
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
		return 0, errors.Wrap(err, "create TLS config")
	}

	cidHost := clusterIDHost{
		ClusterID: clusterID,
		Host:      host,
	}

	addr := net.JoinHostPort(host, DefaultPort)
	if ni, ok := s.hasNodeInfo(cidHost); ok {
		addr = ni.CQLAddr(host)
	}

	// Try to connect directly to host address and default port
	config := cqlping.Config{
		Addr:      addr,
		Timeout:   s.config.Timeout,
		TLSConfig: tlsConfig,
	}
	if tlsConfig != nil {
		config.Timeout = s.config.SSLTimeout
	}
	rtt, err = cqlping.Ping(ctx, config)

	// If connection was cut by the server try upgrading to TLS.
	if errors.Cause(err) == io.EOF && config.TLSConfig == nil {
		s.logger.Info(ctx, "Upgrading CQL connection to TLS",
			"cluster_id", clusterID,
			"host", host,
		)
		config.TLSConfig = DefaultTLSConfig
		rtt, err = cqlping.Ping(ctx, config)
		if err == nil {
			s.setTLSConfig(clusterID, DefaultTLSConfig)
		}
	}

	// In case any network error try to pull NodeInfo and use configured address and port.
	if _, ok := errors.Cause(err).(*net.OpError); ok {
		ni, niErr := s.fetchNodeInfo(ctx, cidHost)
		if niErr != nil {
			s.logger.Error(ctx, "Failed to fetch node info", "error", niErr)
			return 0, err
		}

		if addr := ni.CQLAddr(host); addr != config.Addr {
			s.logger.Info(ctx, "Changing CQL IP/port based on information from Scylla API",
				"cluster_id", cidHost.ClusterID,
				"host", cidHost.Host,
				"from", config.Addr,
				"to", addr,
			)

			config.Addr = addr
			rtt, err = cqlping.Ping(ctx, config)
		}
	}

	return rtt, err
}

func (s *Service) pingREST(ctx context.Context, clusterID uuid.UUID, host string) (time.Duration, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrapf(err, "get client for cluster with id %s", clusterID)
	}

	return client.PingN(ctx, host, pingLaps, 0)
}

func (s *Service) hasNodeInfo(cidHost clusterIDHost) (*scyllaclient.NodeInfo, bool) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	if ni, ok := s.nodeInfoCache[cidHost]; ok {
		return ni, true
	}
	return nil, false
}

func (s *Service) fetchNodeInfo(ctx context.Context, cidHost clusterIDHost) (*scyllaclient.NodeInfo, error) {
	s.invalidateHostNodeInfoCache(cidHost)

	client, err := s.scyllaClient(ctx, cidHost.ClusterID)
	if err != nil {
		return nil, errors.Wrap(err, "create scylla client")
	}

	ni, err := client.NodeInfo(httpmw.NoRetry(ctx), cidHost.Host)
	if err != nil {
		return nil, errors.Wrap(err, "fetch node info")
	}

	s.cacheMu.Lock()
	s.nodeInfoCache[cidHost] = ni
	s.cacheMu.Unlock()

	return ni, nil
}

func (s *Service) tlsConfig(ctx context.Context, clusterID uuid.UUID) (*tls.Config, error) {
	// Try loading from tlsCache.
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	if c, ok := s.tlsCache[clusterID]; ok {
		return c, nil
	}

	s.logger.Info(ctx, "Loading SSL certificate from secrets store", "cluster_id", clusterID)
	tlsIdentity := &secrets.TLSIdentity{
		ClusterID: clusterID,
	}
	err := s.secretsStore.Get(tlsIdentity)
	// If there is no user certificate record no TLS config to avoid rereading
	// from a secure store.
	if err == service.ErrNotFound {
		s.tlsCache[clusterID] = nil
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "get SSL user cert from secrets store")
	}
	keyPair, err := tls.X509KeyPair(tlsIdentity.Cert, tlsIdentity.PrivateKey)
	if err != nil {
		return nil, errors.Wrap(err, "invalid SSL user key pair")
	}

	// Create a new TLS configuration with user certificate and tlsCache
	// the configuration.
	cfg := &tls.Config{
		Certificates:       []tls.Certificate{keyPair},
		InsecureSkipVerify: true,
	}
	s.tlsCache[clusterID] = cfg

	return cfg, nil
}

func (s *Service) setTLSConfig(clusterID uuid.UUID, config *tls.Config) {
	s.cacheMu.Lock()
	s.tlsCache[clusterID] = config
	s.cacheMu.Unlock()
}

func (s *Service) hasTLSConfig(clusterID uuid.UUID) bool {
	s.cacheMu.Lock()
	c := s.tlsCache[clusterID]
	s.cacheMu.Unlock()
	return c != nil
}

func (s *Service) invalidateHostNodeInfoCache(cidHost clusterIDHost) {
	s.cacheMu.Lock()
	delete(s.nodeInfoCache, cidHost)
	s.cacheMu.Unlock()
}

// InvalidateCache frees all in-memory NodeInfo and TLS configuration
// associated with a given cluster forcing reload from Scylla nodes with next usage.
func (s *Service) InvalidateCache(clusterID uuid.UUID) {
	s.cacheMu.Lock()
	for cidHost := range s.nodeInfoCache {
		if cidHost.ClusterID.String() == clusterID.String() {
			delete(s.nodeInfoCache, cidHost)
		}
	}
	delete(s.tlsCache, clusterID)
	s.cacheMu.Unlock()
}
