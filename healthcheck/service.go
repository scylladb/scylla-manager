// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/cluster"
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

	c, err := s.cluster(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to find cluster with id %s", clusterID)
	}

	hostStatus := make(map[string]Status)
	apply(collect(cqlStatus), func(cluster, host string, v float64) {
		if c.String() != cluster {
			return
		}
		var st string
		switch v {
		case 1:
			st = statusUp
		case -1:
			st = statusDown
		default:
			st = statusUnknown
		}
		hostStatus[host] = Status{
			CQLStatus: st,
		}
	})
	apply(collect(cqlRTT), func(cluster, host string, v float64) {
		if c.String() != cluster {
			return
		}
		status := hostStatus[host]
		status.RTT = v
		hostStatus[host] = status
	})

	statuses := make([]Status, 0, len(hostStatus))
	for host, status := range hostStatus {
		status.Host = host
		statuses = append(statuses, status)
	}

	return statuses, nil
}
