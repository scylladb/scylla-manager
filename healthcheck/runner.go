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
)

var cqlStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "scylla_manager",
	Subsystem: "healthcheck",
	Name:      "cql_status",
	Help:      "Host native port status",
}, []string{"cluster", "host"})

var cqlRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "scylla_manager",
	Subsystem: "cluster",
	Name:      "cql_rtt_ms",
	Help:      "Host native port RTT",
}, []string{"cluster", "host"})

func init() {
	prometheus.MustRegister(
		cqlStatus,
		cqlRTT,
	)
}

const pingTimeout = 5 * time.Second

// Runner runs health checks.
type Runner struct {
	cluster cluster.ProviderFunc
	client  scyllaclient.ProviderFunc
}

// NewRunner creates new Runner.
func NewRunner(cp cluster.ProviderFunc, sp scyllaclient.ProviderFunc) *Runner {
	return &Runner{
		cluster: cp,
		client:  sp,
	}
}

type hostRTT struct {
	host string
	rtt  time.Duration
	err  error
}

// Run implements runner.Runner.
func (r Runner) Run(ctx context.Context, d runner.Descriptor, p runner.Properties) error {
	ctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	// get cluster name
	c, err := r.cluster(ctx, d.ClusterID)
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}

	client, err := r.client(ctx, d.ClusterID)
	if err != nil {
		return errors.Wrap(err, "failed to get client proxy")
	}

	hosts, err := client.Hosts(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get hosts")
	}

	out := make(chan hostRTT, runtime.NumCPU()+1)
	for _, h := range hosts {
		v := hostRTT{host: h}
		go func() {
			v.rtt, v.err = cqlping.Ping(ctx, v.host)
			out <- v
		}()
	}

	for range hosts {
		v := <-out

		l := prometheus.Labels{
			"cluster": c.String(),
			"host":    v.host,
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
func (r Runner) Stop(ctx context.Context, d runner.Descriptor) error {
	return nil
}

// Status implements runner.Runner.
func (r Runner) Status(ctx context.Context, d runner.Descriptor) (runner.Status, string, error) {
	return runner.StatusDone, "", nil
}
