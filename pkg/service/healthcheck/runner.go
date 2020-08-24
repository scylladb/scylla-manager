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
	timeout      timeoutProviderFunc
	metrics      *runnerMetrics
	ping         func(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (rtt time.Duration, err error)
}

type runnerMetrics struct {
	status  *prometheus.GaugeVec
	rtt     *prometheus.GaugeVec
	timeout *prometheus.GaugeVec
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

	hostDCs := status.LiveHostDCs()

	r.removeMetricsForMissingHosts(clusterName, hostDCs)
	r.checkHosts(ctx, clusterID, clusterName, hostDCs)

	return nil
}

func (r Runner) checkHosts(ctx context.Context, clusterID uuid.UUID, clusterName string, hostDCs []scyllaclient.HostDC) {
	parallel.Run(len(hostDCs), parallel.NoLimit, func(i int) error { // nolint: errcheck
		hl := prometheus.Labels{
			clusterKey: clusterName,
			hostKey:    hostDCs[i].Host,
		}
		dl := prometheus.Labels{
			clusterKey: clusterName,
			dcKey:      hostDCs[i].Datacenter,
		}

		timeout, saveNext := r.timeout(clusterID, hostDCs[i].Datacenter)
		rtt, err := r.ping(ctx, clusterID, hostDCs[i].Host, timeout)
		if err != nil {
			r.metrics.status.With(hl).Set(-1)
		} else {
			r.metrics.status.With(hl).Set(1)
		}
		r.metrics.rtt.With(hl).Set(float64(rtt.Milliseconds()))
		r.metrics.timeout.With(dl).Set(float64(timeout.Milliseconds()))
		saveNext(rtt)

		return nil
	})
}

func (r Runner) removeMetricsForCluster(clusterName string) {
	apply(collect(r.metrics.status), func(cluster, dc, host, pt string, v float64) {
		if clusterName != cluster {
			return
		}

		hl := prometheus.Labels{
			clusterKey: clusterName,
			hostKey:    host,
		}
		dl := prometheus.Labels{
			clusterKey: clusterName,
			dcKey:      dc,
		}
		dpl := prometheus.Labels{
			clusterKey:  clusterName,
			dcKey:       dc,
			pingTypeKey: pt,
		}
		r.metrics.status.Delete(hl)
		r.metrics.rtt.Delete(hl)
		r.metrics.timeout.Delete(dl)
		rttMean.Delete(dpl)
		rttStandardDeviation.Delete(dpl)
		rttNoise.Delete(dpl)
	})
}

func (r Runner) removeMetricsForMissingHosts(clusterName string, hostDCs []scyllaclient.HostDC) {
	m := strset.New()
	for _, hd := range hostDCs {
		m.Add(hd.Host)
	}

	apply(collect(r.metrics.status), func(cluster, dc, host, pt string, v float64) {
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
		r.metrics.status.Delete(l)
		r.metrics.rtt.Delete(l)
	})
}
