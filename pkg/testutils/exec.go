// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
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

	// CmdBlockScyllaCQLSSL defines the command used for blocking the Scylla CQL access.
	CmdBlockScyllaCQLSSL = "iptables -A INPUT -p tcp --destination-port 9142 -j DROP"

	// CmdUnblockScyllaCQLSSL defines the command used for unblocking the Scylla CQL access.
	CmdUnblockScyllaCQLSSL = "iptables -D INPUT -p tcp --destination-port 9142 -j DROP"

	// CmdBlockScyllaAlternator defines the command used for blocking the Scylla Alternator access.
	CmdBlockScyllaAlternator = "iptables -A INPUT -p tcp --destination-port 8000 -j DROP"

	// CmdUnblockScyllaAlternator defines the command used for unblocking the Scylla Alternator access.
	CmdUnblockScyllaAlternator = "iptables -D INPUT -p tcp --destination-port 8000 -j DROP"
)

func makeIPV6Rule(rule string) string {
	return strings.Replace(rule, "iptables ", "ip6tables -6 ", 1)
}

// RunIptablesCommand executes iptables command, repeats same command for IPV6 iptables rule.
func RunIptablesCommand(host, cmd string) error {
	if IsIPV6Network() {
		return ExecOnHostStatus(host, makeIPV6Rule(cmd))
	}
	return ExecOnHostStatus(host, cmd)
}

// ExecOnHostStatus executes the given command on the given host and returns on error.
func ExecOnHostStatus(host, cmd string) error {
	_, _, err := ExecOnHost(host, cmd)
	return errors.Wrapf(err, "run command %s", cmd)
}

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

// WaitForNodeUPOrTimeout waits until nodetool status report UN status for the given node.
// The nodetool status CLI is executed on the same node.
func WaitForNodeUPOrTimeout(h string, timeout time.Duration) error {
	nodeIsReady := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(nodeIsReady)
		for {
			select {
			case <-done:
				return
			default:
				stdout, _, err := ExecOnHost(h, "nodetool status | grep "+h)
				if err != nil {
					continue
				}
				if strings.HasPrefix(stdout, "UN") {
					return
				}
				select {
				case <-done:
					return
				case <-time.After(time.Second):
				}
			}
		}
	}()

	select {
	case <-nodeIsReady:
		return nil
	case <-time.After(timeout):
		close(done)
		return fmt.Errorf("node %s haven't reach UP status", h)
	}
}

// BlockREST blocks the Scylla API ports on h machine by dropping TCP packets.
func BlockREST(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdBlockScyllaREST); err != nil {
		t.Error(err)
	}
}

// UnblockREST unblocks the Scylla API ports on []hosts machines.
func UnblockREST(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdUnblockScyllaREST); err != nil {
		t.Error(err)
	}
}

// TryUnblockREST tries to unblock the Scylla API ports on []hosts machines.
// Logs an error if the execution failed, but doesn't return it.
func TryUnblockREST(t *testing.T, hosts []string) {
	t.Helper()
	for _, host := range hosts {
		if err := RunIptablesCommand(host, CmdUnblockScyllaREST); err != nil {
			t.Log(err)
		}
	}
}

// BlockCQL blocks the CQL ports on h machine by dropping TCP packets.
func BlockCQL(t *testing.T, h string, sslEnabled bool) {
	t.Helper()
	cmd := CmdBlockScyllaCQL
	if sslEnabled {
		cmd = CmdBlockScyllaCQLSSL
	}
	if err := RunIptablesCommand(h, cmd); err != nil {
		t.Error(err)
	}
}

// UnblockCQL unblocks the CQL ports on []hosts machines.
func UnblockCQL(t *testing.T, h string, sslEnabled bool) {
	t.Helper()
	cmd := CmdUnblockScyllaCQL
	if sslEnabled {
		cmd = CmdUnblockScyllaCQLSSL
	}
	if err := RunIptablesCommand(h, cmd); err != nil {
		t.Error(err)
	}
}

// TryUnblockCQL tries to unblock the CQL ports on []hosts machines.
// Logs an error if the execution failed, but doesn't return it.
func TryUnblockCQL(t *testing.T, hosts []string) {
	t.Helper()
	for _, host := range hosts {
		if err := RunIptablesCommand(host, CmdUnblockScyllaCQL); err != nil {
			t.Log(err)
		}
	}
}

// BlockAlternator blocks the Scylla Alternator ports on h machine by dropping TCP packets.
func BlockAlternator(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdBlockScyllaAlternator); err != nil {
		t.Error(err)
	}
}

// UnblockAlternator unblocks the Alternator ports on []hosts machines.
func UnblockAlternator(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdUnblockScyllaAlternator); err != nil {
		t.Error(err)
	}
}

// TryUnblockAlternator tries to unblock the Alternator API ports on []hosts machines.
// Logs an error if the execution failed, but doesn't return it.
func TryUnblockAlternator(t *testing.T, hosts []string) {
	t.Helper()
	for _, host := range hosts {
		if err := RunIptablesCommand(host, CmdUnblockScyllaAlternator); err != nil {
			t.Log(err)
		}
	}
}

const agentService = "scylla-manager-agent"

// StopAgent stops scylla-manager-agent service on the h machine.
func StopAgent(t *testing.T, h string) {
	t.Helper()
	if err := StopService(h, agentService); err != nil {
		t.Error(err)
	}
}

// StartAgent starts scylla-manager-agent service on the h machine.
func StartAgent(t *testing.T, h string) {
	t.Helper()
	if err := StartService(h, agentService); err != nil {
		t.Error(err)
	}
}

// TryStartAgent tries to start scylla-manager-agent service on the []hosts machines.
// It logs an error on failures, but doesn't return it.
func TryStartAgent(t *testing.T, hosts []string) {
	t.Helper()
	for _, host := range hosts {
		if err := StartService(host, agentService); err != nil {
			t.Log(err)
		}
	}
}

// EnsureNodesAreUP validates if scylla-service is up and running on every []hosts and nodes are reporting their status
// correctly via `nodetool status` command.
// It waits for each node to report UN status for the duration specified in timeout parameter.
func EnsureNodesAreUP(t *testing.T, hosts []string, timeout time.Duration) error {
	t.Helper()

	var (
		allErrors error
		mu        sync.Mutex
	)

	wg := sync.WaitGroup{}
	for _, host := range hosts {
		wg.Add(1)

		go func(h string) {
			defer wg.Done()

			if err := WaitForNodeUPOrTimeout(h, timeout); err != nil {
				mu.Lock()
				allErrors = multierr.Combine(allErrors, err)
				mu.Unlock()
			}
		}(host)
	}
	wg.Wait()

	return allErrors
}
