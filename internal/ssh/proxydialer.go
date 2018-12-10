// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"context"
	"fmt"
	"net"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	sshOpenStreamsCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "ssh",
		Name:      "open_streams_count",
		Help:      "Number of active (multiplexed) connections to Scylla node.",
	}, []string{"host"})

	sshErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scylla_manager",
		Subsystem: "ssh",
		Name:      "errors_total",
		Help:      "Total number of SSH dial errors.",
	}, []string{"host"})
)

func init() {
	prometheus.MustRegister(
		sshOpenStreamsCount,
		sshErrorsTotal,
	)
}

type proxyConn struct {
	net.Conn
	free func()
}

// Close closes the connection and frees the associated resources.
func (c proxyConn) Close() error {
	defer c.free()
	return c.Conn.Close()
}

// proxyDialer is a dialler that allows for proxying connections over SSH.
type proxyDialer struct {
	dialContext DialContextFunc
	config      Config
}

// DialContext to addr HOST:PORT establishes an SSH connection to HOST and then
// proxies the connection to localhost:PORT.
func (t proxyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, _ := net.SplitHostPort(addr)
	labels := prometheus.Labels{"host": host}

	client, err := t.dialContext(ctx, network, net.JoinHostPort(host, fmt.Sprint(t.config.Port)), t.config)
	if err != nil {
		sshErrorsTotal.With(labels).Inc()
		return nil, errors.Wrap(err, "ssh: dial failed")
	}

	var (
		conn    net.Conn
		connErr error
	)
	for _, h := range []string{"localhost", host} {
		// This is a local dial and should not hang but if it does http client
		// would end up with "context deadline exceeded" error.
		// To be fixed when used with something else then http client.
		conn, connErr = client.Dial(network, net.JoinHostPort(h, port))
		if connErr == nil {
			break
		}
	}
	if connErr != nil {
		sshErrorsTotal.With(labels).Inc()
		client.Close()
		return nil, errors.Wrap(connErr, "ssh: remote dial failed")
	}

	g := sshOpenStreamsCount.With(labels)
	g.Inc()
	return proxyConn{
		Conn: conn,
		free: func() {
			g.Dec()
			client.Close()
		},
	}, nil
}
