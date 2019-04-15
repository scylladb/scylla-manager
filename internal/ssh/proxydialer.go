// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"context"
	"fmt"
	"net"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"golang.org/x/crypto/ssh"
)

type proxyConn struct {
	net.Conn
	client        *ssh.Client
	keepaliveDone chan struct{}
	free          func()
}

// Close closes the connection and frees the associated resources.
func (c proxyConn) Close() error {
	if c.keepaliveDone != nil {
		close(c.keepaliveDone)
	}
	defer c.free()

	// Close closes the underlying network connection.
	return c.client.Close()
}

// ProxyDialer is a dialler that allows for proxying connections over SSH.
type ProxyDialer struct {
	config Config
	dial   DialContextFunc
	logger log.Logger

	// OnDial is a listener that may be set to track openning SSH connection to
	// the remote host. It is called for both successful and failed trials.
	OnDial func(host string, err error)
	// OnConnClose is a listener that may be set to track closing of SSH
	// connection.
	OnConnClose func(host string)
}

func NewProxyDialer(config Config, dial DialContextFunc, logger log.Logger) *ProxyDialer {
	return &ProxyDialer{
		config: config,
		dial:   dial,
		logger: logger,
	}
}

// DialContext to addr HOST:PORT establishes an SSH connection to HOST and then
// proxies the connection to localhost:PORT.
func (p *ProxyDialer) DialContext(ctx context.Context, network, addr string) (conn net.Conn, err error) {
	host, port, _ := net.SplitHostPort(addr)

	defer func() {
		if p.OnDial != nil {
			p.OnDial(host, err)
		}
	}()

	p.logger.Info(ctx, "Connecting to remote host...", "host", host)

	client, err := p.dial(ctx, network, net.JoinHostPort(host, fmt.Sprint(p.config.Port)), &p.config.ClientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "ssh: dial failed")
	}

	// This is a local dial and should not hang but if it does http client
	// would end up with "context deadline exceeded" error.
	// To be fixed when used with something else then http client.

	p.logger.Info(ctx, "Opening tunnel", "host", host, "port", port)

	conn, connErr := client.Dial(network, net.JoinHostPort("0.0.0.0", port))

	if connErr != nil {
		client.Close()
		return nil, errors.Wrap(connErr, "ssh: remote dial failed")
	}

	p.logger.Info(ctx, "Connected!", "host", host)

	pc := proxyConn{
		Conn:   conn,
		client: client,
		free: func() {
			if p.OnConnClose != nil {
				p.OnConnClose(host)
			}
		},
	}

	// Init SSH keepalive if needed
	if p.config.KeepaliveEnabled() {
		p.logger.Debug(ctx, "Starting ssh KeepAlives", "host", host)
		pc.keepaliveDone = make(chan struct{})
		go keepalive(client, p.config.ServerAliveInterval, p.config.ServerAliveCountMax, pc.keepaliveDone)
	}

	return pc, nil
}
