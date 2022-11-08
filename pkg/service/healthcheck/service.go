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
	"golang.org/x/sync/errgroup"

	"github.com/scylladb/scylla-manager/v3/pkg/ping"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/dynamoping"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
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
	nodeInfoCache map[clusterIDHost]nodeInfo

	logger log.Logger
}

func NewService(config Config, scyllaClient scyllaclient.ProviderFunc, secretsStore store.Store, logger log.Logger) (*Service, error) {
	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		config:        config,
		scyllaClient:  scyllaClient,
		secretsStore:  secretsStore,
		nodeInfoCache: make(map[clusterIDHost]nodeInfo),
		logger:        logger,
	}, nil
}

func (s *Service) Runner() Runner {
	return Runner{
		cql: runner{
			scyllaClient: s.scyllaClient,
			timeout:      s.config.RelativeTimeout,
			metrics: &runnerMetrics{
				status:  cqlStatus,
				rtt:     cqlRTT,
				timeout: cqlTimeout,
			},
			ping: s.pingCQL,
		},
		rest: runner{
			scyllaClient: s.scyllaClient,
			timeout:      s.config.RelativeTimeout,
			metrics: &runnerMetrics{
				status:  restStatus,
				rtt:     restRTT,
				timeout: restTimeout,
			},
			ping: s.pingREST,
		},
		alternator: runner{
			scyllaClient: s.scyllaClient,
			timeout:      s.config.RelativeTimeout,
			metrics: &runnerMetrics{
				status:  alternatorStatus,
				rtt:     alternatorRTT,
				timeout: alternatorTimeout,
			},
			ping: s.pingAlternator,
		},
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

			rtt, err := s.pingREST(ctx, clusterID, status[i].Addr, s.config.RelativeTimeout)
			o.RESTRtt = float64(rtt.Milliseconds())
			if err != nil {
				s.logger.Error(ctx, "REST ping failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)
				switch {
				case rtt == 0:
					o.CQLStatus = statusError
					o.CQLCause = err.Error()
				case errors.Is(err, context.DeadlineExceeded):
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

			rtt, err := s.pingCQL(ctx, clusterID, status[i].Addr, s.config.RelativeTimeout)
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

			return
		})
	}
}

func (s *Service) parallelAlternatorPingFunc(ctx context.Context, clusterID uuid.UUID,
	status scyllaclient.NodeStatusInfoSlice, out []NodeStatus,
) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) (_ error) {
			o := &out[i]

			// Ignore check if node is not Un and Normal.
			if !status[i].IsUN() {
				return
			}

			rtt, err := s.pingAlternator(ctx, clusterID, status[i].Addr, s.config.RelativeTimeout)
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

			return
		})
	}
}

// pingAlternator sends ping probe and returns RTT.
// When Alternator frontend is disabled, it returns 0 and nil error.
func (s *Service) pingAlternator(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (rtt time.Duration, err error) {
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
		Timeout: timeout,
	}

	tlsConfig := ni.tlsConfig(alternatorPing)
	if tlsConfig != nil {
		config.TLSConfig = tlsConfig.Clone()
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
		Timeout: timeout,
	}

	tlsConfig := ni.tlsConfig(cqlPing)
	if tlsConfig != nil {
		config.TLSConfig = tlsConfig.Clone()
	}

	if c := s.cqlCreds(ctx, clusterID); c != nil {
		rtt, err = cqlping.QueryPing(ctx, config, c.Username, c.Password)
	} else {
		rtt, err = cqlping.NativeCQLPing(ctx, config)
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

// InvalidateCache frees all in-memory NodeInfo and TLS configuration
// associated with a given cluster forcing reload from Scylla nodes with next usage.
func (s *Service) InvalidateCache(clusterID uuid.UUID) {
	s.cacheMu.Lock()
	for cidHost := range s.nodeInfoCache {
		if cidHost.ClusterID == clusterID {
			delete(s.nodeInfoCache, cidHost)
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
