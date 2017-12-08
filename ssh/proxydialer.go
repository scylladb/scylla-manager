// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"net"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

type proxyConn struct {
	net.Conn
	free func()
}

// Close closes the connection and frees the associated resources.
func (c proxyConn) Close() error {
	defer c.free()
	return c.Conn.Close()
}

// ProxyDialer is a dialler that allows for proxying connections over SSH.
type ProxyDialer struct {
	*Pool
	Config *ssh.ClientConfig
}

// Dial to addr HOST:PORT establishes an SSH connection to HOST and then
// proxies the connection to localhost:PORT.
func (t ProxyDialer) Dial(network, addr string) (net.Conn, error) {
	host, port, _ := net.SplitHostPort(addr)

	client, err := t.Pool.Dial(network, net.JoinHostPort(host, "22"), t.Config)
	if err != nil {
		return nil, errors.Wrap(err, "SSH connection failed")
	}

	conn, err := client.Dial(network, net.JoinHostPort("localhost", port))
	if err != nil {
		return nil, errors.Wrap(err, "remote dial failed")
	}

	return proxyConn{
		Conn: conn,
		free: func() {
			t.Pool.Release(client)
		},
	}, nil
}
