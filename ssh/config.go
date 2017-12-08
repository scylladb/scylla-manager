// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"net"
	"os"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// NewProductionClientConfig returns configuration with a key based
// authentication that connects to known hosts only.
func NewProductionClientConfig(user, knownHostsFile string) (*ssh.ClientConfig, error) {
	cb, err := knownhosts.New(knownHostsFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse %q", knownHostsFile)
	}

	auth, err := agentAuthMethod()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SSH agent")
	}

	return &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{auth},
		HostKeyCallback: cb,
	}, nil
}

func agentAuthMethod() (ssh.AuthMethod, error) {
	agentSock, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
	if err != nil {
		return nil, err
	}

	return ssh.PublicKeysCallback(agent.NewClient(agentSock).Signers), nil
}

// NewDevelopmentClientConfig returns configuration with a password based
// authentication.
func NewDevelopmentClientConfig() *ssh.ClientConfig {
	return &ssh.ClientConfig{
		User:            "scylla-mgmt",
		Auth:            []ssh.AuthMethod{ssh.Password("test")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
}
