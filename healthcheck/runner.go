// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/cqlping"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

type healthCheckRunner struct {
	cluster cluster.ProviderFunc
	client  scyllaclient.ProviderFunc
}

type hostRTT struct {
	host string
	rtt  time.Duration
	err  error
}

// Run implements runner.Runner.
func (r healthCheckRunner) Run(ctx context.Context, d runner.Descriptor, p runner.Properties) error {
	// get cluster name
	c, hosts, err := r.getClusterAndHosts(ctx, d.ClusterID)
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}

	out := make(chan hostRTT, runtime.NumCPU()+1)
	for _, h := range hosts {
		v := hostRTT{host: h}
		go func() {
			const pingTimeout = 250 * time.Millisecond
			v.rtt, v.err = cqlping.Ping(ctx, pingTimeout, v.host)
			out <- v
		}()
	}

	cqlStatus.Reset()
	cqlRTT.Reset()

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

// Stop implements runner.Runner.
func (r healthCheckRunner) Stop(ctx context.Context, d runner.Descriptor) error {
	return nil
}

// Status implements runner.Runner.
func (r healthCheckRunner) Status(ctx context.Context, d runner.Descriptor) (runner.Status, string, error) {
	return runner.StatusDone, "", nil
}

func (r healthCheckRunner) getClusterAndHosts(ctx context.Context, clusterID uuid.UUID) (*cluster.Cluster, []string, error) {
	c, err := r.cluster(ctx, clusterID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "invalid cluster")
	}

	client, err := r.client(ctx, clusterID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get client proxy")
	}

	hosts, err := client.Hosts(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get hosts")
	}

	return c, hosts, nil
}
