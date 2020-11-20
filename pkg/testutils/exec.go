// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"bytes"
	"net"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

const (
	// CmdBlockScyllaREST defines the command used for blocking the Scylla REST.
	CmdBlockScyllaREST = "iptables -A INPUT -p tcp --destination-port 10000 -j DROP"

	// CmdUnblockScyllaREST defines the command used for unblocking the Scylla REST.
	CmdUnblockScyllaREST = "iptables -D INPUT -p tcp --destination-port 10000 -j DROP"

	// CmdBlockScyllaCQL defines the command used for blocking the Scylla CQL access.
	CmdBlockScyllaCQL = "iptables -A INPUT -p tcp --destination-port 9042 -j DROP"

	// CmdUnblockScyllaCQL defines the command used for unblocking the Scylla CQL access.
	CmdUnblockScyllaCQL = "iptables -D INPUT -p tcp --destination-port 9042 -j DROP"

	// CmdBlockScyllaAlternator defines the command used for blocking the Scylla Alternator access.
	CmdBlockScyllaAlternator = "iptables -A INPUT -p tcp --destination-port 8000 -j DROP"

	// CmdUnblockScyllaAlternator defines the command used for unblocking the Scylla Alternator access.
	CmdUnblockScyllaAlternator = "iptables -D INPUT -p tcp --destination-port 8000 -j DROP"
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

// StopService allows to stop given service on the given host. Service must be
// controlled by supervisor.
func StopService(h, service string) error {
	stdout, stderr, err := ExecOnHost(h, "supervisorctl stop "+service)
	if err != nil {
		return errors.Wrapf(err, "stop agent host: %s, stdout %s, stderr %s", h, stdout, stderr)
	}
	return nil
}

// StartService allows to start given service on the given host. Service must be
// controlled by supervisor.
func StartService(h, service string) error {
	stdout, stderr, err := ExecOnHost(h, "supervisorctl start "+service)
	if err != nil {
		return errors.Wrapf(err, "start agent host: %s, stdout %s, stderr %s", h, stdout, stderr)
	}
	return nil
}
