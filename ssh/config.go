// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// NewProductionClientConfig returns configuration with a key based
// authentication that connects to known hosts only.
func NewProductionClientConfig(user, identityFile, knownHostsFile string) (*ssh.ClientConfig, error) {
	if user == "" {
		return nil, errors.New("missing user")
	}
	if identityFile == "" {
		return nil, errors.New("missing identity file")
	}
	if knownHostsFile == "" {
		return nil, errors.New("missing known hosts")
	}

	auth, err := keyPairAuthMethod(identityFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse %q", identityFile)
	}

	cb, err := knownhosts.New(knownHostsFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse %q", knownHostsFile)
	}

	return &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{auth},
		HostKeyCallback: cb,
	}, nil
}

func keyPairAuthMethod(identityFile string) (ssh.AuthMethod, error) {
	b, err := ioutil.ReadFile(identityFile)
	if err != nil {
		return nil, err
	}

	signer, err := ssh.ParsePrivateKey(b)
	if err != nil {
		return nil, err
	}

	return ssh.PublicKeys(signer), nil
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
