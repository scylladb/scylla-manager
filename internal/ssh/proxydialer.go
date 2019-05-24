// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"context"
	"fmt"
	"net"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

type proxyConn struct {
	net.Conn
	client *ssh.Client
	done   chan struct{}
	free   func()
}

// Close closes the connection and frees the associated resources.
func (c proxyConn) Close() error {
	if c.done != nil {
		close(c.done)
	}
	defer c.free()

	// Close closes the underlying network connection.
	return c.client.Close()
}

// ProxyDialer is a dialler that allows for proxying connections over SSH.
type ProxyDialer struct {
	config Config
	dial   DialContextFunc

	OnDial      func(host string, err error)
	OnConnClose func(host string)
}

func NewProxyDialer(config Config, dial DialContextFunc) *ProxyDialer {
	return &ProxyDialer{config: config, dial: dial}
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

	client, err := p.dial(ctx, network, net.JoinHostPort(host, fmt.Sprint(p.config.Port)), p.config)
	if err != nil {
		return nil, errors.Wrap(err, "ssh: dial failed")
	}

	// This is a local dial and should not hang but if it does http client
	// would end up with "context deadline exceeded" error.
	// To be fixed when used with something else then http client.
	conn, connErr := client.Dial(network, net.JoinHostPort("0.0.0.0", port))

	if connErr != nil {
		client.Close()
		return nil, errors.Wrap(connErr, "ssh: remote dial failed")
	}

	pc := proxyConn{
		Conn:   conn,
		client: client,
		free: func() {
			if p.OnConnClose != nil {
				p.OnConnClose(host)
			}
		},
	}

	// Init SSH keepalive if needed.
	if p.config.ServerAliveInterval > 0 && p.config.ServerAliveCountMax > 0 {
		pc.done = make(chan struct{})
		go keepalive(client, p.config.ServerAliveInterval, p.config.ServerAliveCountMax, pc.done)
	}

	return pc, nil
}
