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
	"github.com/scylladb/mermaid/internal/cqlping"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/scyllaclient"
)

const pingTimeout = 250 * time.Millisecond

type healthCheckRunner struct {
	cluster cluster.ProviderFunc
	client  scyllaclient.ProviderFunc
}

type hostRTT struct {
	host string
	rtt  time.Duration
	err  error
}

const pingTimeout = 250 * time.Millisecond

// Run implements runner.Runner.
func (r healthCheckRunner) Run(ctx context.Context, d runner.Descriptor, p runner.Properties) error {
	c, err := r.cluster(ctx, d.ClusterID)
	if err != nil {
		return errors.Wrap(err, "failed to get cluster")
	}

	client, err := r.client(ctx, d.ClusterID)
	if err != nil {
		return errors.Wrap(err, "failed to get client")
	}

	hosts, err := client.Hosts(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get hosts")
	}

	r.removeDecommissionedHosts(c, hosts)

	out := make(chan hostRTT, runtime.NumCPU()+1)
	for _, h := range hosts {
		v := hostRTT{host: h}
		go func() {
			v.rtt, v.err = cqlping.Ping(ctx, pingTimeout, v.host)
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
			cqlStatus.With(l).Set(-1)
			cqlRTT.With(l).Set(0)
		} else {
			cqlStatus.With(l).Set(1)
			cqlRTT.With(l).Set(float64(v.rtt) / 1000000)
		}
	}

	return nil
}

func (r healthCheckRunner) removeDecommissionedHosts(c *cluster.Cluster, hosts []string) {
	m := strset.New(hosts...)

	apply(collect(cqlStatus), func(cluster, host string, v float64) {
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
		cqlStatus.Delete(l)
		cqlRTT.Delete(l)
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
