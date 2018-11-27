// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"bytes"
	"context"
	"net"
	"sync"

	"github.com/scylladb/mermaid/internal/ssh"
	"go.uber.org/multierr"
	cssh "golang.org/x/crypto/ssh"
)

const (
	// CmdBlockScyllaAPI defines the command used for blocking the Scylla REST API.
	CmdBlockScyllaAPI = "iptables -A INPUT -p tcp --destination-port 10000 -j DROP"

	// CmdUnblockScyllaAPI defines the command used for unblocking the Scylla REST API.
	CmdUnblockScyllaAPI = "iptables -D INPUT -p tcp --destination-port 10000 -j DROP"
)

// ScyllaExecutor defines a small struct that can execute commands on
// arbitrary hosts using SSH.
type ScyllaExecutor struct {
	dial       ssh.DialContextFunc
	sshConfig  *cssh.ClientConfig
	sshConns   map[string]*cssh.Client
	sshConnsMu sync.Mutex
}

// NewScyllaExecutor creates a new ScyllaExecutorIF
func NewScyllaExecutor() *ScyllaExecutor {
	return &ScyllaExecutor{
		dial:      ssh.ContextDialer(ssh.DefaultDialer),
		sshConfig: ssh.NewDevelopmentClientConfig(),
		sshConns:  make(map[string]*cssh.Client),
	}
}

// ExecOnHost executes the given command on the given host.
// It returns the stdout and stderr of the remote command and
// any error that can potentially happen.
func (se *ScyllaExecutor) ExecOnHost(ctx context.Context, host string, cmd string) (string, string, error) {
	sshConn, err := se.dialHost(ctx, host)

	if err != nil {
		return "", "", err
	}
	session, err := sshConn.NewSession()
	if err != nil {
		return "", "", err
	}
	defer session.Close()

	var outBuf, errBuf bytes.Buffer
	session.Stdout = &outBuf
	session.Stderr = &errBuf

	err = session.Run(cmd)
	return outBuf.String(), errBuf.String(), err
}

// Close releases all the resources and closes all the SSH connections.
func (se *ScyllaExecutor) Close() error {
	se.sshConnsMu.Lock()
	defer se.sshConnsMu.Unlock()

	conns := se.sshConns
	se.sshConns = nil

	var err error
	for _, conn := range conns {
		err = multierr.Append(err, conn.Close())
	}
	return err
}

func (se *ScyllaExecutor) dialHost(ctx context.Context, host string) (*cssh.Client, error) {
	se.sshConnsMu.Lock()
	defer se.sshConnsMu.Unlock()

	// Check if we have a connection to this host already
	if conn, ok := se.sshConns[host]; ok {
		return conn, nil
	}

	sshConn, err := se.dial(ctx, "tcp", net.JoinHostPort(host, "22"), se.sshConfig)
	if err != nil {
		return nil, err
	}

	se.sshConns[host] = sshConn

	return sshConn, nil
}
