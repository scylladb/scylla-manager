// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Runner implements scheduler.Runner.
type Runner struct {
	cql        runner
	rest       runner
	alternator runner
}

func (r Runner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	p := taskProperties{}
	if err := json.Unmarshal(properties, &p); err != nil {
		return util.ErrValidate(err)
	}

	switch p.Mode {
	case CQLMode:
		return r.cql.Run(ctx, clusterID, taskID, runID, properties)
	case RESTMode:
		return r.rest.Run(ctx, clusterID, taskID, runID, properties)
	case AlternatorMode:
		return r.alternator.Run(ctx, clusterID, taskID, runID, properties)
	default:
		return errors.Errorf("unspecified mode")
	}
}

type runner struct {
	logger       log.Logger
	configCache  configcache.ConfigCacher
	scyllaClient scyllaclient.ProviderFunc
	timeout      time.Duration
	metrics      *runnerMetrics
	ping         func(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration, nodeConf configcache.NodeConfig) (rtt time.Duration, err error)
	pingAgent    func(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (rtt time.Duration, err error)
}

type runnerMetrics struct {
	status *prometheus.GaugeVec
	rtt    *prometheus.GaugeVec
}

func (r runner) Run(ctx context.Context, clusterID, _, _ uuid.UUID, _ json.RawMessage) (err error) {
	defer func() {
		if err != nil {
			r.removeMetricsForCluster(clusterID)
		}
	}()

	// Enable interactive mode for fast backoff
	ctx = scyllaclient.Interactive(ctx)

	nodes, err := r.configCache.AvailableHosts(ctx, clusterID)
	if err != nil {
		return err
	}
	r.removeMetricsForMissingHosts(clusterID, nodes)
	r.checkHosts(ctx, clusterID, nodes)

	return nil
}

func (r runner) checkHosts(ctx context.Context, clusterID uuid.UUID, addresses []string) {
	f := func(i int) error {
		rtt := time.Duration(0)
		ni, err := r.configCache.Read(clusterID, addresses[i])
		if err == nil {
			rtt, err = r.ping(ctx, clusterID, addresses[i], r.timeout, ni)
		}
		hl := prometheus.Labels{
			clusterKey: clusterID.String(),
			hostKey:    addresses[i],
			rackKey:    ni.Rack,
			dcKey:      ni.Datacenter,
		}
		if err != nil {
			r.metrics.status.With(hl).Set(-1)
		} else {
			r.metrics.status.With(hl).Set(1)
		}
		r.metrics.rtt.With(hl).Set(float64(rtt.Milliseconds()))

		return err
	}

	_ = parallel.Run(len(addresses), parallel.NoLimit, f, func(i int, err error) { // nolint: errcheck
		r.logger.Error(ctx, "Parallel hosts check failed", "host", addresses[i], "error", err)
	})
}

func (r runner) removeMetricsForCluster(clusterID uuid.UUID) {
	apply(collect(r.metrics.status), func(cluster, _, host, _ string, _ float64) {
		if clusterID.String() != cluster {
			return
		}

		hl := prometheus.Labels{
			clusterKey: clusterID.String(),
			hostKey:    host,
		}
		r.metrics.status.Delete(hl)
		r.metrics.rtt.Delete(hl)
	})
}

func (r runner) removeMetricsForMissingHosts(clusterID uuid.UUID, addresses []string) {
	apply(collect(r.metrics.status), func(cluster, _, host, _ string, _ float64) {
		if clusterID.String() != cluster {
			return
		}
		if slice.ContainsString(addresses, host) {
			return
		}

		l := prometheus.Labels{
			clusterKey: clusterID.String(),
			hostKey:    host,
		}
		r.metrics.status.Delete(l)
		r.metrics.rtt.Delete(l)
	})
}
