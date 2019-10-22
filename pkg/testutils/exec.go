// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"bytes"
	"net"

	"golang.org/x/crypto/ssh"
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
func ExecOnHost(host, cmd string) (stdout, stderr string, err error) {
	client, err := ssh.Dial("tcp", net.JoinHostPort(host, "22"), sshAsRoot())
	if err != nil {
		return "", "", err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return "", "", err
	}
	defer session.Close()

	var stdoutBuf, stderrBuf bytes.Buffer
	session.Stdout = &stdoutBuf
	session.Stderr = &stderrBuf

	err = session.Run(cmd)
	stdout = stdoutBuf.String()
	stderr = stderrBuf.String()
	return
}

func sshAsRoot() *ssh.ClientConfig {
	c := &ssh.ClientConfig{}
	c.User = "root"
	c.Auth = []ssh.AuthMethod{ssh.Password("root")}
	c.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	return c
}
