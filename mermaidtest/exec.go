// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"bytes"
	"context"
	"net"

	"github.com/scylladb/mermaid/internal/ssh"
)

const (
	// CmdBlockScyllaREST defines the command used for blocking the Scylla REST REST.
	CmdBlockScyllaREST = "iptables -A INPUT -p tcp --destination-port 10000 -j DROP"

	// CmdUnblockScyllaREST defines the command used for unblocking the Scylla REST REST.
	CmdUnblockScyllaREST = "iptables -D INPUT -p tcp --destination-port 10000 -j DROP"

	// CmdBlockScyllaCQL defines the command used for blocking the Scylla CQL access.
	CmdBlockScyllaCQL = "iptables -A INPUT -p tcp --destination-port 9042 -j DROP"

	// CmdUnblockScyllaCQL defines the command used for unblocking the Scylla CQL access.
	CmdUnblockScyllaCQL = "iptables -D INPUT -p tcp --destination-port 9042 -j DROP"
)

// ExecOnHost executes the given command on the given host. It returns the
// stdout and stderr of the remote command.
func ExecOnHost(ctx context.Context, host string, cmd string) (string, string, error) {
	client, err := ssh.ContextDialer(&net.Dialer{})(ctx, "tcp", net.JoinHostPort(host, "22"), sshAsRoot())
	if err != nil {
		return "", "", err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return "", "", err
	}
	defer session.Close()

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	err = session.Run(cmd)
	return stdout.String(), stderr.String(), err
}

func sshAsRoot() ssh.Config {
	return ssh.DefaultConfig().WithPasswordAuth("root", "root")
}
