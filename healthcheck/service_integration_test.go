// Copyright (C) 2017 ScyllaDB

// +build all integration

package healthcheck

import (
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/ssh"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap/zapcore"
)

type host struct {
	host   string
	rtt    float64
	status float64
}

func TestGetStatus(t *testing.T) {
	clusters := make(map[uuid.UUID]string)
	var table = []struct {
		c uuid.UUID
		h []host
		s []Status
	}{
		{
			c: uuid.MustRandom(),
			h: []host{
				{
					host:   "host_1",
					status: -1,
					rtt:    0,
				},
				{
					host:   "host_2",
					status: 1,
					rtt:    0.123,
				},
				{
					host:   "host_3",
					status: 0,
					rtt:    0,
				},
			},
			s: []Status{
				{
					Host:      "host_1",
					CQLStatus: "DOWN",
					RTT:       0,
				},
				{
					Host:      "host_2",
					CQLStatus: "UP",
					RTT:       0.123,
				},
				{
					Host:      "host_3",
					CQLStatus: "UNKNOWN",
					RTT:       0,
				},
			},
		},
		{
			c: uuid.MustRandom(),
			h: []host{
				{
					host:   "host_4",
					status: -1,
					rtt:    0,
				},
				{
					host:   "host_5",
					status: 1,
					rtt:    0.123,
				},
				{
					host:   "host_6",
					status: 0,
					rtt:    0,
				},
			},
			s: []Status{
				{
					Host:      "host_4",
					CQLStatus: "DOWN",
					RTT:       0,
				},
				{
					Host:      "host_5",
					CQLStatus: "UP",
					RTT:       0.123,
				},
				{
					Host:      "host_6",
					CQLStatus: "UNKNOWN",
					RTT:       0,
				},
			},
		},
	}

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("healthcheck")

	s := NewService(
		func(ctx context.Context, id uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{
				ID:   id,
				Host: clusters[id],
			}, nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return scyllaclient.NewClient(ManagedClusterHosts, ssh.NewDevelopmentTransport(), logger.Named("scylla"))
		},
		logger)

	for _, v := range table {
		for _, h := range v.h {
			l := prometheus.Labels{
				clusterKey: v.c.String(),
				hostKey:    h.host,
			}

			cqlStatus.With(l).Set(h.status)
			cqlRTT.With(l).Set(h.rtt)
		}
	}

	for _, v := range table {
		status, err := s.GetStatus(context.Background(), v.c)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(status, func(i, j int) bool {
			return status[i].Host < status[j].Host
		})

		if diff := cmp.Diff(status, v.s); diff != "" {
			t.Fatal(diff)
		}
	}
}
