// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"go.uber.org/multierr"
	"golang.org/x/crypto/ssh"
)

// proxyConn is a net.Conn that writes to the SSH shell stdin and reads from
// the SSH shell stdout.
type proxyConn struct {
	client  *ssh.Client
	session *ssh.Session
	stdin   io.WriteCloser
	stdout  io.Reader

	free func()
}

// newProxyConn opens a new session and start the shell. When the connection is
// closed the client is closed and the free function is called.
func newProxyConn(client *ssh.Client, stderr io.Writer, free func()) (*proxyConn, error) {
	// Open new session to the agent
	session, err := client.NewSession()
	if err != nil {
		return nil, err
	}

	// Get a pipe to stdin so that we can send data down
	stdin, err := session.StdinPipe()
	if err != nil {
		return nil, err
	}

	// Get a pipe to stdout so that we can get responses back
	stdout, err := session.StdoutPipe()
	if err != nil {
		return nil, err
	}

	// Set stderr
	session.Stderr = stderr

	// Start the shell on the other side
	if err := session.Shell(); err != nil {
		return nil, err
	}

	return &proxyConn{
		client:  client,
		session: session,
		stdin:   stdin,
		stdout:  stdout,
		free:    free,
	}, nil
}

func (conn *proxyConn) Read(b []byte) (n int, err error) {
	return conn.stdout.Read(b)
}

func (conn *proxyConn) Write(b []byte) (n int, err error) {
	return conn.stdin.Write(b)
}

func (conn *proxyConn) Close() error {
	var err error
	err = multierr.Append(err, conn.session.Close())
	err = multierr.Append(err, conn.client.Close())
	if conn.free != nil {
		conn.free()
	}
	return err
}

func (conn *proxyConn) LocalAddr() net.Addr {
	return conn.client.LocalAddr()
}

func (conn *proxyConn) RemoteAddr() net.Addr {
	return conn.client.RemoteAddr()
}

func (*proxyConn) SetDeadline(t time.Time) error {
	return errors.New("ssh: deadline not supported")
}

func (*proxyConn) SetReadDeadline(t time.Time) error {
	return errors.New("ssh: deadline not supported")
}

func (*proxyConn) SetWriteDeadline(t time.Time) error {
	return errors.New("ssh: deadline not supported")
}

type logStderr struct {
	ctx    context.Context
	logger log.Logger
}

func (w *logStderr) Write(p []byte) (n int, err error) {
	w.logger.Error(w.ctx, "stderr", string(p))
	return len(p), nil
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
	host, _, _ := net.SplitHostPort(addr)

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

	p.logger.Debug(ctx, "Starting session", "host", host)

	keepaliveDone := make(chan struct{})
	free := func() {
		close(keepaliveDone)
		if p.OnConnClose != nil {
			p.OnConnClose(host)
		}
		p.logger.Info(ctx, "Connection closed", "host", host)
	}

	pconn, err := newProxyConn(client, &logStderr{ctx, p.logger.With("host", host)}, free)
	if err != nil {
		client.Close()
		return nil, errors.Wrap(err, "ssh: failed to connect to scylla-manager agent")
	}

	p.logger.Info(ctx, "Connected!", "host", host)

	// Init SSH keepalive if needed
	if p.config.KeepaliveEnabled() {
		p.logger.Debug(ctx, "Starting ssh KeepAlives", "host", host)
		go keepalive(client, p.config.ServerAliveInterval, p.config.ServerAliveCountMax, keepaliveDone)
	}

	return pconn, nil
}
