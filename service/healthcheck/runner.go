// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"encoding/json"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
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
		return errors.Wrap(err, "failed to get cluster")
	}

	defer func() {
		if err != nil {
			r.removeAll(clusterName)
		}
	}()

	client, err := r.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "failed to get client")
	}

	hosts, err := client.Hosts(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get hosts")
	}

	r.removeDecommissionedHosts(clusterName, hosts)
	r.checkHosts(ctx, clusterID, clusterName, hosts)

	return nil
}

type hostRTT struct {
	host string
	rtt  time.Duration
	err  error
}

func (r Runner) checkHosts(ctx context.Context, clusterID uuid.UUID, clusterName string, hosts []string) {
	out := make(chan hostRTT, runtime.NumCPU()+1)
	for _, h := range hosts {
		v := hostRTT{host: h}
		go func() {
			v.rtt, v.err = r.ping(ctx, clusterID, v.host)
			out <- v
		}()
	}

	for range hosts {
		v := <-out

		l := prometheus.Labels{
			clusterKey: clusterName,
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

func (r Runner) removeAll(clusterName string) {
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

func (r Runner) removeDecommissionedHosts(clusterName string, hosts []string) {
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
