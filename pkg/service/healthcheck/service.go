// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"golang.org/x/sync/errgroup"

	"github.com/scylladb/scylla-manager/v3/pkg/ping"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/dynamoping"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Service manages health checks.
type Service struct {
	config          Config
	scyllaClient    scyllaclient.ProviderFunc
	secretsStore    store.Store
	clusterProvider cluster.ProviderFunc
	configCache     configcache.ConfigCacher

	logger log.Logger
}

func NewService(config Config, scyllaClient scyllaclient.ProviderFunc, secretsStore store.Store,
	clusterProvider cluster.ProviderFunc, configCache configcache.ConfigCacher, logger log.Logger,
) (*Service, error) {
	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		config:          config,
		scyllaClient:    scyllaClient,
		secretsStore:    secretsStore,
		clusterProvider: clusterProvider,
		configCache:     configCache,
		logger:          logger,
	}, nil
}

func (s *Service) Runner() Runner {
	return Runner{
		cql: runner{
			logger:       s.logger.Named("CQL healthcheck"),
			scyllaClient: s.scyllaClient,
			timeout:      s.config.MaxTimeout,
			metrics: &runnerMetrics{
				status: cqlStatus,
				rtt:    cqlRTT,
			},
			ping:      s.pingCQL,
			pingAgent: s.pingAgent,
		},
		rest: runner{
			logger:       s.logger.Named("REST healthcheck"),
			scyllaClient: s.scyllaClient,
			timeout:      s.config.MaxTimeout,
			metrics: &runnerMetrics{
				status: restStatus,
				rtt:    restRTT,
			},
			ping:      s.pingREST,
			pingAgent: s.pingAgent,
		},
		alternator: runner{
			logger:       s.logger.Named("Alternator healthcheck"),
			scyllaClient: s.scyllaClient,
			timeout:      s.config.MaxTimeout,
			metrics: &runnerMetrics{
				status: alternatorStatus,
				rtt:    alternatorRTT,
			},
			ping:      s.pingAlternator,
			pingAgent: s.pingAgent,
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
		return parallel.Run(len(status), parallel.NoLimit, func(i int) error {
			// Ignore check if node is not Un and Normal
			if !status[i].IsUN() {
				return nil
			}

			ni, err := s.configCache.Read(clusterID, status[i].Addr)
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
			return nil
		}, parallel.NopNotify)
	}
}

func (s *Service) parallelRESTPingFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) error {
			o := &out[i]

			// Ignore check if node is not Un and Normal
			if !status[i].IsUN() {
				return nil
			}

			rtt, err := s.pingREST(ctx, clusterID, status[i].Addr, s.config.MaxTimeout)
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

			return nil
		}, parallel.NopNotify)
	}
}

func (s *Service) parallelCQLPingFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) error {
			o := &out[i]

			// Ignore check if node is not Un and Normal.
			if !status[i].IsUN() {
				return nil
			}

			rtt, err := s.pingCQL(ctx, clusterID, status[i].Addr, s.config.MaxTimeout)
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

			ni, err := s.configCache.Read(clusterID, status[i].Addr)
			if err != nil {
				s.logger.Error(ctx, "Unable to fetch node information", "error", err)
				o.SSL = false
			} else {
				o.SSL = ni.CQLTLSConfig() != nil
			}

			return nil
		}, parallel.NopNotify)
	}
}

func (s *Service) parallelAlternatorPingFunc(ctx context.Context, clusterID uuid.UUID,
	status scyllaclient.NodeStatusInfoSlice, out []NodeStatus,
) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) error {
			o := &out[i]

			// Ignore check if node is not Un and Normal.
			if !status[i].IsUN() {
				return nil
			}

			rtt, err := s.pingAlternator(ctx, clusterID, status[i].Addr, s.config.MaxTimeout)
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

			return nil
		}, parallel.NopNotify)
	}
}

// pingAlternator sends ping probe and returns RTT.
// When Alternator frontend is disabled, it returns 0 and nil error.
func (s *Service) pingAlternator(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (rtt time.Duration, err error) {
	ni, err := s.configCache.Read(clusterID, host)
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

	tlsConfig := ni.AlternatorTLSConfig()
	if tlsConfig != nil {
		config.TLSConfig = tlsConfig.Clone()
	}

	return pingFunc(ctx, config)
}

func (s *Service) decorateNodeStatus(status *NodeStatus, ni configcache.NodeConfig) {
	status.TotalRAM = ni.MemoryTotal
	status.Uptime = ni.Uptime
	status.CPUCount = ni.CPUCount
	status.ScyllaVersion = ni.ScyllaVersion
	status.AgentVersion = ni.AgentVersion
}

func (s *Service) pingCQL(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (rtt time.Duration, err error) {
	ni, err := s.configCache.Read(clusterID, host)
	if err != nil {
		return 0, err
	}
	// Try to connect directly to host address.
	config := cqlping.Config{
		Addr:    ni.CQLAddr(host),
		Timeout: timeout,
	}

	tlsConfig := ni.CQLTLSConfig()
	if tlsConfig != nil {
		config.Addr = tlsConfig.Address
		config.TLSConfig = tlsConfig.Clone()
	}

	if c := s.cqlCreds(ctx, clusterID); c != nil {
		rtt, err = cqlping.QueryPing(ctx, config, c.Username, c.Password)
	} else {
		logger := s.logger.With(
			"cluster_id", clusterID,
			"host", host,
		)
		rtt, err = cqlping.NativeCQLPing(ctx, config, logger)
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

func (s *Service) pingAgent(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (time.Duration, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrapf(err, "get client for cluster with id %s", clusterID)
	}

	return client.PingAgent(ctx, host, timeout)
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
