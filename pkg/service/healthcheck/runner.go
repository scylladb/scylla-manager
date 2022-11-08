// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-set/strset"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
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
		return service.ErrValidate(err)
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
	scyllaClient scyllaclient.ProviderFunc
	timeout      time.Duration
	metrics      *runnerMetrics
	ping         func(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (rtt time.Duration, err error)
}

type runnerMetrics struct {
	status  *prometheus.GaugeVec
	rtt     *prometheus.GaugeVec
	timeout *prometheus.GaugeVec
}

func (r runner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) (err error) {
	defer func() {
		if err != nil {
			r.removeMetricsForCluster(clusterID)
		}
	}()

	// Enable interactive mode for fast backoff
	ctx = scyllaclient.Interactive(ctx)

	client, err := r.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "get client")
	}

	status, err := client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "status")
	}

	live := status.Live()
	r.removeMetricsForMissingHosts(clusterID, live)
	r.checkHosts(ctx, clusterID, live)

	return nil
}

func (r runner) checkHosts(ctx context.Context, clusterID uuid.UUID, status []scyllaclient.NodeStatusInfo) {
	parallel.Run(len(status), parallel.NoLimit, func(i int) error { // nolint: errcheck
		hl := prometheus.Labels{
			clusterKey: clusterID.String(),
			hostKey:    status[i].Addr,
		}

		rtt, err := r.ping(ctx, clusterID, status[i].Addr, r.timeout)
		if err != nil {
			r.metrics.status.With(hl).Set(-1)
		} else {
			r.metrics.status.With(hl).Set(1)
		}
		r.metrics.rtt.With(hl).Set(float64(rtt.Milliseconds()))

		return nil
	})
}

func (r runner) removeMetricsForCluster(clusterID uuid.UUID) {
	apply(collect(r.metrics.status), func(cluster, dc, host, pt string, v float64) {
		if clusterID.String() != cluster {
			return
		}

		hl := prometheus.Labels{
			clusterKey: clusterID.String(),
			hostKey:    host,
		}
		dl := prometheus.Labels{
			clusterKey: clusterID.String(),
			dcKey:      dc,
		}
		r.metrics.status.Delete(hl)
		r.metrics.rtt.Delete(hl)
		r.metrics.timeout.Delete(dl)
	})
}

func (r runner) removeMetricsForMissingHosts(clusterID uuid.UUID, status []scyllaclient.NodeStatusInfo) {
	m := strset.New()
	for _, node := range status {
		m.Add(node.Addr)
	}

	apply(collect(r.metrics.status), func(cluster, dc, host, pt string, v float64) {
		if clusterID.String() != cluster {
			return
		}
		if m.Has(host) {
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
