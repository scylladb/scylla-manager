// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/ping"
	"github.com/scylladb/mermaid/pkg/ping/cqlping"
	"github.com/scylladb/mermaid/pkg/ping/dynamoping"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/service/secrets"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"golang.org/x/sync/errgroup"
)

// ClusterNameFunc returns name for a given ID.
type ClusterNameFunc func(ctx context.Context, clusterID uuid.UUID) (string, error)

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

type clusterIDDCPingType struct {
	ClusterID uuid.UUID
	DC        string
	PingType  pingType
}

type nodeInfoTTL struct {
	NodeInfo *scyllaclient.NodeInfo
	Expires  time.Time
}

// Service manages health checks.
type Service struct {
	config       Config
	clusterName  ClusterNameFunc
	scyllaClient scyllaclient.ProviderFunc
	secretsStore secrets.Store

	cacheMu sync.Mutex
	// fields below are protected by cacheMu
	tlsCache        map[uuid.UUID]*tls.Config
	nodeInfoCache   map[clusterIDHost]nodeInfoTTL
	dynamicTimeouts map[clusterIDDCPingType]*dynamicTimeout

	logger log.Logger
}

func NewService(config Config, clusterName ClusterNameFunc,
	scyllaClient scyllaclient.ProviderFunc, secretsStore secrets.Store, logger log.Logger) (*Service, error) {
	if clusterName == nil {
		return nil, errors.New("invalid cluster name provider")
	}
	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		config:          config,
		clusterName:     clusterName,
		scyllaClient:    scyllaClient,
		secretsStore:    secretsStore,
		tlsCache:        make(map[uuid.UUID]*tls.Config),
		nodeInfoCache:   make(map[clusterIDHost]nodeInfoTTL),
		dynamicTimeouts: make(map[clusterIDDCPingType]*dynamicTimeout),
		logger:          logger,
	}, nil
}

// CQLRunner creates a Runner that performs health checks for CQL connectivity.
func (s *Service) CQLRunner() Runner {
	return Runner{
		scyllaClient: s.scyllaClient,
		timeout:      s.cqlTimeout,
		clusterName:  s.clusterName,
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
		clusterName:  s.clusterName,
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
		clusterName:  s.clusterName,
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

			ni, err := s.getNodeInfo(ctx, clusterID, status[i].Addr)
			if err != nil {
				s.logger.Error(ctx, "Node info fetch failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)
				return
			}

			s.decorateNodeStatus(&out[i], ni)
			return
		})
	}
}

func (s *Service) parallelRESTPingFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) (_ error) {
			// Ignore check if node is not Un and Normal
			if !status[i].IsUN() {
				return
			}

			timeout, saveNext := s.restTimeout(clusterID, status[i].Datacenter)
			rtt, err := s.pingREST(ctx, clusterID, status[i].Addr, timeout)
			out[i].RESTRtt = float64(rtt.Milliseconds())
			if err != nil {
				s.logger.Error(ctx, "REST ping failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)
				switch {
				case errors.Is(err, scyllaclient.ErrTimeout):
					out[i].RESTStatus = statusTimeout
				case scyllaclient.StatusCodeOf(err) == http.StatusUnauthorized:
					out[i].RESTStatus = statusUnauthorized
				case scyllaclient.StatusCodeOf(err) != 0:
					out[i].RESTStatus = fmt.Sprintf("%s %d", statusHTTP, scyllaclient.StatusCodeOf(err))
				default:
					out[i].RESTStatus = statusDown
				}
			} else {
				out[i].RESTStatus = statusUp
			}

			saveNext(rtt)

			return
		})
	}
}

func (s *Service) parallelCQLPingFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) (_ error) {
			// Ignore check if node is not Un and Normal.
			if !status[i].IsUN() {
				return
			}

			timeout, saveNext := s.cqlTimeout(clusterID, status[i].Datacenter)
			rtt, err := s.pingCQL(ctx, clusterID, status[i].Addr, timeout)
			out[i].CQLRtt = float64(rtt.Milliseconds())
			out[i].SSL = s.hasTLSConfig(clusterID)
			if err != nil {
				s.logger.Error(ctx, "CQL ping failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)

				switch {
				case errors.Is(err, ping.ErrTimeout):
					out[i].CQLStatus = statusTimeout
				case errors.Is(err, ping.ErrUnauthorised):
					out[i].CQLStatus = statusUnauthorized
				default:
					out[i].CQLStatus = statusDown
				}
			} else {
				out[i].CQLStatus = statusUp
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
				case errors.Is(err, ping.ErrTimeout):
					out[i].AlternatorStatus = statusTimeout
				case errors.Is(err, ping.ErrUnauthorised):
					out[i].AlternatorStatus = statusUnauthorized
				default:
					out[i].AlternatorStatus = statusDown
				}
			} else if rtt != 0 {
				out[i].AlternatorStatus = statusUp
			}
			if rtt != 0 {
				out[i].AlternatorRtt = float64(rtt.Milliseconds())
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
	tlsConfig, err := s.tlsConfig(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrap(err, "create TLS config")
	}

	cidHost := clusterIDHost{
		ClusterID: clusterID,
		Host:      host,
	}

	ni, err := s.getNodeInfo(ctx, clusterID, host)
	if err != nil {
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
	if tlsConfig != nil {
		config.Timeout = s.config.SSLTimeout
		config.TLSConfig = tlsConfig.Clone()
	}
	if timeout != 0 {
		config.Timeout = timeout
	}

	rtt, err = pingFunc(ctx, config)

	// In case any error try to pull NodeInfo and use configured address and port.
	// AWS SDK doesn't return any network errors - it just times out - we have to
	// react to any of them, it's not possible to distinguish timeout caused by
	// service down and caused by wrong address.
	if err != nil {
		if ni := s.updateNodeInfo(ctx, cidHost); ni != nil {
			if !ni.AlternatorEnabled() {
				return 0, nil
			}
			if addr = ni.AlternatorAddr(host); addr != config.Addr {
				s.logger.Info(ctx, "Changing Alternator IP/port based on information from Scylla API",
					"cluster_id", cidHost.ClusterID,
					"host", cidHost.Host,
					"from", config.Addr,
					"to", addr,
				)

				config.Addr = addr
				rtt, err = pingFunc(ctx, config)
			}
		}
	}

	// If connection was cut by the server try upgrading to TLS.
	if err != nil && config.TLSConfig == nil {
		ni, err := s.getNodeInfo(ctx, clusterID, host)
		if err != nil {
			return 0, err
		}

		if ni.AlternatorEncryptionEnabled() {
			s.logger.Info(ctx, "Upgrading Alternator connection to TLS",
				"cluster_id", clusterID,
				"host", host,
			)

			config.TLSConfig = DefaultTLSConfig.Clone()
			rtt, err = pingFunc(ctx, config)
			if err == nil {
				s.setTLSConfig(clusterID, DefaultTLSConfig.Clone())
			}
		}
	}

	return rtt, err
}

func (s *Service) decorateNodeStatus(status *NodeStatus, ni *scyllaclient.NodeInfo) {
	status.TotalRAM = ni.MemoryTotal
	status.Uptime = ni.Uptime
	status.CPUCount = ni.CPUCount
	status.ScyllaVersion = ni.ScyllaVersion
	status.AgentVersion = ni.AgentVersion
}

func (s *Service) pingCQL(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (rtt time.Duration, err error) {
	tlsConfig, err := s.tlsConfig(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrap(err, "create TLS config")
	}

	cidHost := clusterIDHost{
		ClusterID: clusterID,
		Host:      host,
	}

	addr := net.JoinHostPort(host, DefaultCQLPort)
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

	if timeout != 0 {
		config.Timeout = timeout
	}

	rtt, err = cqlping.Ping(ctx, config)

	// If connection was cut by the server try upgrading to TLS.
	if errors.Cause(err) == io.EOF && config.TLSConfig == nil {
		ni, err := s.getNodeInfo(ctx, clusterID, host)
		if err != nil {
			return 0, err
		}
		if ni.ClientEncryptionEnabled {
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
	}

	// In case any network error try to pull NodeInfo and use configured address and port.
	if _, ok := errors.Cause(err).(*net.OpError); ok {
		if ni := s.updateNodeInfo(ctx, cidHost); ni != nil {
			if addr = ni.CQLAddr(host); addr != config.Addr {
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
	}

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

func (s *Service) hasNodeInfo(cidHost clusterIDHost) (*scyllaclient.NodeInfo, bool) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	now := timeutc.Now()
	if ni, ok := s.nodeInfoCache[cidHost]; ok && now.Before(ni.Expires) {
		return ni.NodeInfo, true
	}
	return nil, false
}

func (s *Service) getNodeInfo(ctx context.Context, clusterID uuid.UUID, host string) (*scyllaclient.NodeInfo, error) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	key := clusterIDHost{clusterID, host}
	now := timeutc.Now()

	if ni, ok := s.nodeInfoCache[key]; ok && now.Before(ni.Expires) {
		return ni.NodeInfo, nil
	}
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "create scylla client")
	}

	ni, err := client.NodeInfo(ctx, host)
	if err != nil {
		return nil, errors.Wrap(err, "fetch node info")
	}

	expires := now.Add(s.config.NodeInfoTTL)
	s.nodeInfoCache[key] = nodeInfoTTL{NodeInfo: ni, Expires: expires}

	return ni, nil
}

func (s *Service) updateNodeInfo(ctx context.Context, cidHost clusterIDHost) *scyllaclient.NodeInfo {
	s.invalidateHostNodeInfoCache(cidHost)

	client, err := s.scyllaClient(ctx, cidHost.ClusterID)
	if err != nil {
		s.logger.Error(ctx, "Failed to create scylla client", "error", err)
		return nil
	}

	ctx = scyllaclient.Interactive(ctx)
	ni, err := client.NodeInfo(ctx, cidHost.Host)
	if err != nil {
		s.logger.Error(ctx, "Failed to fetch node info", "error", err)
		return nil
	}

	expires := timeutc.Now().Add(s.config.NodeInfoTTL)

	s.cacheMu.Lock()
	s.nodeInfoCache[cidHost] = nodeInfoTTL{NodeInfo: ni, Expires: expires}
	s.cacheMu.Unlock()

	return ni
}

func (s *Service) cqlCreds(ctx context.Context, clusterID uuid.UUID) *secrets.CQLCreds {
	cqlCreds := &secrets.CQLCreds{
		ClusterID: clusterID,
	}
	err := s.secretsStore.Get(cqlCreds)
	if err != nil {
		cqlCreds = nil
		if err != service.ErrNotFound {
			s.logger.Error(ctx, "Failed to load CQL credentials from secrets store", "cluster_id", clusterID, "error", err)
		}
	}
	return cqlCreds
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
		if cidHost.ClusterID == clusterID {
			delete(s.nodeInfoCache, cidHost)
		}
	}
	for cidDC := range s.dynamicTimeouts {
		if cidDC.ClusterID == clusterID {
			delete(s.dynamicTimeouts, cidDC)
		}
	}
	delete(s.tlsCache, clusterID)
	s.cacheMu.Unlock()
}
