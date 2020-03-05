// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

// Runner implements sched.Runner.
type Runner struct {
	clusterName  ClusterNameFunc
	scyllaClient scyllaclient.ProviderFunc
	status       *prometheus.GaugeVec
	rtt          *prometheus.GaugeVec
	ping         func(ctx context.Context, clusterID uuid.UUID, host string) (rtt time.Duration, err error)
}

func (r Runner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	clusterName, err := r.clusterName(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "get cluster")
	}

	defer func() {
		if err != nil {
			r.removeMetricsForCluster(clusterName)
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

	hosts := status.LiveHosts()

	r.removeMetricsForMissingHosts(clusterName, hosts)
	r.checkHosts(ctx, clusterID, clusterName, hosts)

	return nil
}

func (r Runner) checkHosts(ctx context.Context, clusterID uuid.UUID, clusterName string, hosts []string) {
	parallel.Run(len(hosts), parallel.NoLimit, func(i int) error { // nolint: errcheck
		l := prometheus.Labels{
			clusterKey: clusterName,
			hostKey:    hosts[i],
		}

		rtt, err := r.ping(ctx, clusterID, hosts[i])
		if err != nil {
			r.status.With(l).Set(-1)
			r.rtt.With(l).Set(0)
		} else {
			r.status.With(l).Set(1)
			r.rtt.With(l).Set(float64(rtt) / 1000000)
		}

		return nil
	})
}

func (r Runner) removeMetricsForCluster(clusterName string) {
	apply(collect(r.status), func(cluster, host string, v float64) {
		if clusterName != cluster {
			return
		}

		l := prometheus.Labels{
			clusterKey: clusterName,
			hostKey:    host,
		}
		r.status.Delete(l)
		r.rtt.Delete(l)
	})
}

func (r Runner) removeMetricsForMissingHosts(clusterName string, hosts []string) {
	m := strset.New(hosts...)

	apply(collect(r.status), func(cluster, host string, v float64) {
		if clusterName != cluster {
			return
		}
		if m.Has(host) {
			return
		}

		l := prometheus.Labels{
			clusterKey: clusterName,
			hostKey:    host,
		}
		r.status.Delete(l)
		r.rtt.Delete(l)
	})
}
