// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"runtime"
	"sort"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/cqlping"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

// Service manages health checks.
type Service struct {
	cluster cluster.ProviderFunc
	client  scyllaclient.ProviderFunc
	logger  log.Logger
}

// NewService creates a new health check service.
func NewService(cp cluster.ProviderFunc, sp scyllaclient.ProviderFunc, logger log.Logger) *Service {
	return &Service{
		cluster: cp,
		client:  sp,
		logger:  logger,
	}
}

// Runner creates a runner.Runner that performs health checks.
func (s *Service) Runner() runner.Runner {
	return healthCheckRunner{
		client:  s.client,
		cluster: s.cluster,
	}
}

// GetStatus returns the current status of the supplied cluster.
func (s *Service) GetStatus(ctx context.Context, clusterID uuid.UUID) ([]Status, error) {
	s.logger.Debug(ctx, "GetStatus", "cluster_id", clusterID)

	client, err := s.client(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get client for cluster with id %s", clusterID)
	}

	dcs, err := client.Datacenters(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get dcs for cluster with id %s", clusterID)
	}

	out := make(chan Status, runtime.NumCPU()+1)
	size := 0
	for dc, hosts := range dcs {
		for _, h := range hosts {
			v := Status{
				DC:   dc,
				Host: h,
			}
			size++

			go func() {
				rtt, err := cqlping.Ping(ctx, pingTimeout, v.Host)
				if err != nil {
					s.logger.Info(ctx, "Ping failed",
						"cluster_id", clusterID,
						"dc", v.DC,
						"host", v.Host,
						"error", err,
					)
					v.CQLStatus = statusDown
				} else {
					v.CQLStatus = statusUp
				}
				v.RTT = float64(rtt / 1000000)

				out <- v
			}()
		}
	}

	statuses := make([]Status, size)
	for i := 0; i < size; i++ {
		statuses[i] = <-out
	}
	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].DC == statuses[j].DC {
			return statuses[i].Host < statuses[j].Host
		}
		return statuses[i].DC < statuses[j].DC
	})

	return statuses, nil
}
