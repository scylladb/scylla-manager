// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"context"
	"net"
	"time"

	"golang.org/x/crypto/ssh"
)

// DialContextFunc creates SSH connection to host with a given address.
type DialContextFunc func(ctx context.Context, network, addr string, config *ssh.ClientConfig) (*ssh.Client, error)

// DefaultDialer specifies default dial options.
var DefaultDialer = net.Dialer{
	Timeout:   3 * time.Second,
	KeepAlive: 30 * time.Second,
	DualStack: true,
}

// ContextDialer returns DialContextFunc based on dialer to make net connections.
func ContextDialer(dialer net.Dialer) DialContextFunc {
	c := contextDialer{dialer}
	return c.DialContext
}

type contextDialer struct {
	dialer net.Dialer
}

func (d *contextDialer) DialContext(ctx context.Context, network, addr string, config *ssh.ClientConfig) (*ssh.Client, error) {
	netConn, err := d.dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	type dialRes struct {
		client *ssh.Client
		err    error
	}
	dialc := make(chan dialRes, 1)

	go func() {
		sshConn, chans, reqs, err := ssh.NewClientConn(netConn, addr, config)
		if err != nil {
			dialc <- dialRes{err: err}
		} else {
			dialc <- dialRes{client: ssh.NewClient(sshConn, chans, reqs)}
		}
	}()

	select {
	case v := <-dialc:
		// Our dial finished.
		if v.client != nil {
			return v.client, nil
		}
		// Our dial failed.
		netConn.Close()
		// It wasn't an error due to cancellation, so
		// return the original error message:
		return nil, v.err
	case <-ctx.Done():
		netConn.Close()
		return nil, ctx.Err()
	}
}
