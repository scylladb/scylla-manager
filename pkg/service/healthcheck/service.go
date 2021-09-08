// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/ping"
	"github.com/scylladb/scylla-manager/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/pkg/ping/dynamoping"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/secrets"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/store"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"golang.org/x/sync/errgroup"
)

type clusterIDHost struct {
	ClusterID uuid.UUID
	Host      string
}

type pingType int

const (
	cqlPing pingType = iota
	restPing
	alternatorPing
)

func (pt pingType) String() string {
	switch pt {
	case cqlPing:
		return "cql"
	case restPing:
		return "rest"
	case alternatorPing:
		return "alternator"
	}
	return "unknown"
}

type clusterIDDCPingType struct {
	ClusterID uuid.UUID
	DC        string
	PingType  pingType
}

type nodeInfo struct {
	*scyllaclient.NodeInfo
	TLSConfig map[pingType]*tls.Config
	Expires   time.Time
}

// Service manages health checks.
type Service struct {
	config       Config
	scyllaClient scyllaclient.ProviderFunc
	secretsStore store.Store

	cacheMu sync.Mutex
	// fields below are protected by cacheMu
	nodeInfoCache   map[clusterIDHost]nodeInfo
	dynamicTimeouts map[clusterIDDCPingType]*dynamicTimeout

	logger log.Logger
}

func NewService(config Config, scyllaClient scyllaclient.ProviderFunc, secretsStore store.Store, logger log.Logger) (*Service, error) {
	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		config:          config,
		scyllaClient:    scyllaClient,
		secretsStore:    secretsStore,
		nodeInfoCache:   make(map[clusterIDHost]nodeInfo),
		dynamicTimeouts: make(map[clusterIDDCPingType]*dynamicTimeout),
		logger:          logger,
	}, nil
}

// CQLRunner creates a Runner that performs health checks for CQL connectivity.
func (s *Service) CQLRunner() Runner {
	return Runner{
		scyllaClient: s.scyllaClient,
		timeout:      s.cqlTimeout,
		metrics: &runnerMetrics{
			status:  cqlStatus,
			rtt:     cqlRTT,
			timeout: cqlTimeout,
		},
		ping: s.pingCQL,
	}
}

// RESTRunner creates a Runner that performs health checks for REST API
// connectivity.
func (s *Service) RESTRunner() Runner {
	return Runner{
		scyllaClient: s.scyllaClient,
		timeout:      s.restTimeout,
		metrics: &runnerMetrics{
			status:  restStatus,
			rtt:     restRTT,
			timeout: restTimeout,
		},
		ping: s.pingREST,
	}
}

// AlternatorRunner creates a Runner that performs health checks for Alternator API
// connectivity.
func (s *Service) AlternatorRunner() Runner {
	return Runner{
		scyllaClient: s.scyllaClient,
		timeout:      s.alternatorTimeout,
		metrics: &runnerMetrics{
			status:  alternatorStatus,
			rtt:     alternatorRTT,
			timeout: alternatorTimeout,
		},
		ping: s.pingAlternator,
	}
}

// Status returns the current status of the supplied cluster.
func (s *Service) Status(ctx context.Context, clusterID uuid.UUID) ([]NodeStatus, error) {
	s.logger.Debug(ctx, "Status", "cluster_id", clusterID)

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "get client")
	}

	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "status")
	}

	out := makeNodeStatus(status)

	g := new(errgroup.Group)
	g.Go(s.parallelAlternatorPingFunc(ctx, clusterID, status, out))
	g.Go(s.parallelCQLPingFunc(ctx, clusterID, status, out))
	g.Go(s.parallelRESTPingFunc(ctx, clusterID, status, out))
	g.Go(s.parallelNodeInfoFunc(ctx, clusterID, status, out))

	return out, g.Wait()
}

func (s *Service) parallelNodeInfoFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) (_ error) {
			// Ignore check if node is not Un and Normal
			if !status[i].IsUN() {
				return
			}

			ni, err := s.nodeInfo(ctx, clusterID, status[i].Addr)
			if err != nil {
				s.logger.Error(ctx, "Node info fetch failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)
			}
			if ni.NodeInfo != nil {
				s.decorateNodeStatus(&out[i], ni)
			}
			return
		})
	}
}

func (s *Service) parallelRESTPingFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) (_ error) {
			o := &out[i]

			// Ignore check if node is not Un and Normal
			if !status[i].IsUN() {
				return
			}

			timeout, saveNext := s.restTimeout(clusterID, status[i].Datacenter)
			rtt, err := s.pingREST(ctx, clusterID, status[i].Addr, timeout)
			o.RESTRtt = float64(rtt.Milliseconds())
			if err != nil {
				s.logger.Error(ctx, "REST ping failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)
				switch {
				case errors.Is(err, scyllaclient.ErrTimeout):
					o.RESTStatus = statusTimeout
				case scyllaclient.StatusCodeOf(err) == http.StatusUnauthorized:
					o.RESTStatus = statusUnauthorized
				case scyllaclient.StatusCodeOf(err) != 0:
					o.RESTStatus = fmt.Sprintf("%s %d", statusHTTP, scyllaclient.StatusCodeOf(err))
				default:
					o.RESTStatus = statusDown
					o.RESTCause = err.Error()
				}
			} else {
				o.RESTStatus = statusUp
			}

			saveNext(rtt)

			return
		})
	}
}

func (s *Service) parallelCQLPingFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) (_ error) {
			o := &out[i]

			// Ignore check if node is not Un and Normal.
			if !status[i].IsUN() {
				return
			}

			timeout, saveNext := s.cqlTimeout(clusterID, status[i].Datacenter)
			rtt, err := s.pingCQL(ctx, clusterID, status[i].Addr, timeout)
			o.CQLRtt = float64(rtt.Milliseconds())
			if err != nil {
				s.logger.Error(ctx, "CQL ping failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)

				o := &out[i]
				switch {
				case rtt == 0:
					o.CQLStatus = statusError
					o.CQLCause = err.Error()
				case errors.Is(err, ping.ErrTimeout):
					o.CQLStatus = statusTimeout
				case errors.Is(err, ping.ErrUnauthorised):
					o.CQLStatus = statusUnauthorized
				default:
					o.CQLStatus = statusDown
					o.CQLCause = err.Error()
				}
			} else {
				o.CQLStatus = statusUp
			}

			ni, err := s.nodeInfo(ctx, clusterID, status[i].Addr)
			if err != nil {
				s.logger.Error(ctx, "Unable to fetch node information",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)
				o.SSL = false
			} else {
				o.SSL = ni.hasTLSConfig(cqlPing)
			}

			saveNext(rtt)

			return
		})
	}
}

func (s *Service) parallelAlternatorPingFunc(ctx context.Context, clusterID uuid.UUID,
	status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) (_ error) {
			o := &out[i]

			// Ignore check if node is not Un and Normal.
			if !status[i].IsUN() {
				return
			}

			timeout, saveNext := s.alternatorTimeout(clusterID, status[i].Datacenter)
			rtt, err := s.pingAlternator(ctx, clusterID, status[i].Addr, timeout)
			if err != nil {
				s.logger.Error(ctx, "Alternator ping failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)

				switch {
				case rtt == 0:
					o.AlternatorStatus = statusError
					o.AlternatorCause = err.Error()
				case errors.Is(err, ping.ErrTimeout):
					o.AlternatorStatus = statusTimeout
				case errors.Is(err, ping.ErrUnauthorised):
					o.AlternatorStatus = statusUnauthorized
				default:
					o.AlternatorStatus = statusDown
					o.AlternatorCause = err.Error()
				}
			} else if rtt != 0 {
				o.AlternatorStatus = statusUp
			}
			if rtt != 0 {
				o.AlternatorRtt = float64(rtt.Milliseconds())
			}

			saveNext(rtt)

			return
		})
	}
}

// pingAlternator sends ping probe and returns RTT.
// When Alternator frontend is disabled, it returns 0 and nil error.
func (s *Service) pingAlternator(ctx context.Context, clusterID uuid.UUID, host string,
	timeout time.Duration) (rtt time.Duration, err error) {
	ni, err := s.nodeInfo(ctx, clusterID, host)
	// Proceed if we managed to get required information.
	if err != nil && ni.NodeInfo == nil {
		return 0, errors.Wrap(err, "get node info")
	}
	if !ni.AlternatorEnabled() {
		return 0, nil
	}

	pingFunc := dynamoping.SimplePing
	if queryPing, err := ni.SupportsAlternatorQuery(); err == nil && queryPing {
		pingFunc = dynamoping.QueryPing
	}

	addr := ni.AlternatorAddr(host)
	config := dynamoping.Config{
		Addr:    addr,
		Timeout: s.config.Timeout,
	}

	tlsConfig := ni.tlsConfig(alternatorPing)
	if tlsConfig != nil {
		config.TLSConfig = tlsConfig.Clone()
		config.Timeout = s.config.SSLTimeout
	}
	if timeout != 0 {
		config.Timeout = timeout
	}

	return pingFunc(ctx, config)
}

func (s *Service) decorateNodeStatus(status *NodeStatus, ni nodeInfo) {
	status.TotalRAM = ni.MemoryTotal
	status.Uptime = ni.Uptime
	status.CPUCount = ni.CPUCount
	status.ScyllaVersion = ni.ScyllaVersion
	status.AgentVersion = ni.AgentVersion
}

func (s *Service) pingCQL(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (rtt time.Duration, err error) {
	ni, err := s.nodeInfo(ctx, clusterID, host)
	if err != nil {
		return 0, err
	}
	// Try to connect directly to host address.
	config := cqlping.Config{
		Addr:    ni.CQLAddr(host),
		Timeout: s.config.Timeout,
	}

	tlsConfig := ni.tlsConfig(cqlPing)
	if tlsConfig != nil {
		config.TLSConfig = tlsConfig.Clone()
		config.Timeout = s.config.SSLTimeout
	}

	if timeout != 0 {
		config.Timeout = timeout
	}

	rtt, err = cqlping.Ping(ctx, config)

	// If CQL credentials are available try executing a query.
	if err == nil {
		if c := s.cqlCreds(ctx, clusterID); c != nil {
			config.Username = c.Username
			config.Password = c.Password
			rtt, err = cqlping.Ping(ctx, config)
		}
	}

	return rtt, err
}

func (s *Service) pingREST(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (time.Duration, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrapf(err, "get client for cluster with id %s", clusterID)
	}

	return client.Ping(ctx, host, timeout)
}

func (s *Service) nodeInfo(ctx context.Context, clusterID uuid.UUID, host string) (nodeInfo, error) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	key := clusterIDHost{clusterID, host}
	now := timeutc.Now()

	if ni, ok := s.nodeInfoCache[key]; ok && now.Before(ni.Expires) {
		return ni, nil
	}
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nodeInfo{}, errors.Wrap(err, "create scylla client")
	}

	ni := nodeInfo{}
	if ni.NodeInfo, err = client.NodeInfo(ctx, host); err != nil {
		return nodeInfo{}, errors.Wrap(err, "fetch node info")
	}

	ni.TLSConfig = make(map[pingType]*tls.Config, 2)
	for _, p := range []pingType{alternatorPing, cqlPing} {
		var tlsEnabled, clientCertAuth bool
		if p == cqlPing {
			tlsEnabled, clientCertAuth = ni.CQLTLSEnabled()
		} else if p == alternatorPing {
			tlsEnabled, clientCertAuth = ni.AlternatorTLSEnabled()
		}
		if tlsEnabled {
			tlsConfig, err := s.tlsConfig(clusterID, clientCertAuth)
			if err != nil {
				return ni, errors.Wrap(err, "fetch TLS config")
			}
			ni.TLSConfig[p] = tlsConfig
		}
	}

	ni.Expires = now.Add(s.config.NodeInfoTTL)
	s.nodeInfoCache[key] = ni

	return ni, nil
}

func (s *Service) tlsConfig(clusterID uuid.UUID, clientCertAuth bool) (*tls.Config, error) {
	cfg := &tls.Config{
		InsecureSkipVerify: true,
	}

	if clientCertAuth {
		id := &secrets.TLSIdentity{
			ClusterID: clusterID,
		}
		if err := s.secretsStore.Get(id); err != nil {
			return nil, errors.Wrap(err, "get SSL user cert from secrets store")
		}
		keyPair, err := tls.X509KeyPair(id.Cert, id.PrivateKey)
		if err != nil {
			return nil, errors.Wrap(err, "invalid SSL user key pair")
		}
		cfg.Certificates = []tls.Certificate{keyPair}
	}

	return cfg, nil
}

func (s *Service) cqlCreds(ctx context.Context, clusterID uuid.UUID) *secrets.CQLCreds {
	cqlCreds := &secrets.CQLCreds{
		ClusterID: clusterID,
	}
	err := s.secretsStore.Get(cqlCreds)
	if err != nil {
		cqlCreds = nil
		if !errors.Is(err, service.ErrNotFound) {
			s.logger.Error(ctx, "Failed to load CQL credentials from secrets store", "cluster_id", clusterID, "error", err)
		}
	}
	return cqlCreds
}

func (s *Service) cqlTimeout(clusterID uuid.UUID, dc string) (timeout time.Duration, saveNext func(time.Duration)) {
	return s.timeout(clusterID, dc, cqlPing)
}

func (s *Service) restTimeout(clusterID uuid.UUID, dc string) (timeout time.Duration, saveNext func(time.Duration)) {
	return s.timeout(clusterID, dc, restPing)
}

func (s *Service) alternatorTimeout(clusterID uuid.UUID, dc string) (timeout time.Duration, saveNext func(time.Duration)) {
	return s.timeout(clusterID, dc, alternatorPing)
}

// dcTimeouts returns timeout measured for given clusterID and DC.
func (s *Service) timeout(clusterID uuid.UUID, dc string, pt pingType) (timeout time.Duration, saveNext func(time.Duration)) {
	if !s.config.DynamicTimeout.Enabled {
		return s.config.Timeout, func(time.Duration) {}
	}

	// Try loading from cache.
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	key := clusterIDDCPingType{clusterID, dc, pt}
	var dt *dynamicTimeout
	if t, ok := s.dynamicTimeouts[key]; ok {
		dt = t
	} else {
		dt = newDynamicTimeout(s.config.DynamicTimeout)
		s.dynamicTimeouts[key] = dt
	}

	t := dt.Timeout()
	return t, dt.SaveProbe
}

// InvalidateCache frees all in-memory NodeInfo and TLS configuration
// associated with a given cluster forcing reload from Scylla nodes with next usage.
func (s *Service) InvalidateCache(clusterID uuid.UUID) {
	s.cacheMu.Lock()
	for cidHost := range s.nodeInfoCache {
		if cidHost.ClusterID == clusterID {
			delete(s.nodeInfoCache, cidHost)
		}
	}
	for cidDC := range s.dynamicTimeouts {
		if cidDC.ClusterID == clusterID {
			delete(s.dynamicTimeouts, cidDC)
		}
	}
	s.cacheMu.Unlock()
}

func (ni nodeInfo) tlsConfig(pt pingType) *tls.Config {
	return ni.TLSConfig[pt]
}

func (ni nodeInfo) hasTLSConfig(pt pingType) bool {
	return ni.TLSConfig[pt] != nil
}
