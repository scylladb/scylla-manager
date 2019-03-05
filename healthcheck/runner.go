// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

type healthCheckRunner struct {
	cluster cluster.ProviderFunc
	client  scyllaclient.ProviderFunc
	status  *prometheus.GaugeVec
	rtt     *prometheus.GaugeVec
	ping    func(ctx context.Context, clusterID uuid.UUID, host string) (rtt time.Duration, err error)
}

// Run implements runner.Runner.
func (r healthCheckRunner) Run(ctx context.Context, d runner.Descriptor, p runner.Properties) (err error) {
	c, err := r.cluster(ctx, d.ClusterID)
	if err != nil {
		return errors.Wrap(err, "failed to get cluster")
	}

	defer func() {
		if err != nil {
			r.removeAll(c)
		}
	}()

	client, err := r.client(ctx, d.ClusterID)
	if err != nil {
		return errors.Wrap(err, "failed to get client")
	}

	hosts, err := client.Hosts(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get hosts")
	}

	r.removeDecommissionedHosts(c, hosts)

	r.run(ctx, hosts, c)

	return nil
}

type hostRTT struct {
	host string
	rtt  time.Duration
	err  error
}

func (r healthCheckRunner) run(ctx context.Context, hosts []string, c *cluster.Cluster) {
	out := make(chan hostRTT, runtime.NumCPU()+1)
	for _, h := range hosts {
		v := hostRTT{host: h}
		go func() {
			v.rtt, v.err = r.ping(ctx, c.ID, v.host)
			out <- v
		}()
	}

	for range hosts {
		v := <-out

		l := prometheus.Labels{
			clusterKey: c.String(),
			hostKey:    v.host,
		}

		if v.err != nil {
			r.status.With(l).Set(-1)
			r.rtt.With(l).Set(0)
		} else {
			r.status.With(l).Set(1)
			r.rtt.With(l).Set(float64(v.rtt) / 1000000)
		}
	}
}

func (r healthCheckRunner) removeAll(c *cluster.Cluster) {
	apply(collect(r.status), func(cluster, host string, v float64) {
		if c.String() != cluster {
			return
		}

		l := prometheus.Labels{
			clusterKey: c.String(),
			hostKey:    host,
		}
		r.status.Delete(l)
		r.rtt.Delete(l)
	})
}

func (r healthCheckRunner) removeDecommissionedHosts(c *cluster.Cluster, hosts []string) {
	m := strset.New(hosts...)

	apply(collect(r.status), func(cluster, host string, v float64) {
		if c.String() != cluster {
			return
		}
		if m.Has(host) {
			return
		}

		l := prometheus.Labels{
			clusterKey: c.String(),
			hostKey:    host,
		}
		r.status.Delete(l)
		r.rtt.Delete(l)
	})
}

// Stop implements runner.Runner.
func (r healthCheckRunner) Stop(ctx context.Context, d runner.Descriptor) error {
	return nil
}

// Status implements runner.Runner.
func (r healthCheckRunner) Status(ctx context.Context, d runner.Descriptor) (runner.Status, string, error) {
	return runner.StatusDone, "", nil
}
