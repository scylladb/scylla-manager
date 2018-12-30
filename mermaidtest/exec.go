// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"bytes"
	"context"
	"net"

	"github.com/scylladb/mermaid/internal/ssh"
)

const (
	// CmdBlockScyllaAPI defines the command used for blocking the Scylla REST API.
	CmdBlockScyllaAPI = "iptables -A INPUT -p tcp --destination-port 10000 -j DROP"

	// CmdUnblockScyllaAPI defines the command used for unblocking the Scylla REST API.
	CmdUnblockScyllaAPI = "iptables -D INPUT -p tcp --destination-port 10000 -j DROP"
)

// ExecOnHost executes the given command on the given host. It returns the
// stdout and stderr of the remote command.
func ExecOnHost(ctx context.Context, host string, cmd string) (string, string, error) {
	client, err := ssh.ContextDialer(&net.Dialer{})(ctx, "tcp", net.JoinHostPort(host, "22"), sshConfig())
	if err != nil {
		return "", "", err
	}
	defer client.Close()

	session, err := client.NewSession()
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
