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
func NewProductionClientConfig(user, pemFile, knownHostsFile string) (*ssh.ClientConfig, error) {
	if user == "" {
		return nil, errors.New("missing user")
	}
	if pemFile == "" {
		return nil, errors.New("missing pem file")
	}
	if knownHostsFile == "" {
		return nil, errors.New("missing known hosts")
	}

	cb, err := knownhosts.New(knownHostsFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse %q", knownHostsFile)
	}

	auth, err := keyPairAuthMethod(pemFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse %q", pemFile)
	}

	return &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{auth},
		HostKeyCallback: cb,
	}, nil
}

func keyPairAuthMethod(pemFile string) (ssh.AuthMethod, error) {
	pemBytes, err := ioutil.ReadFile(pemFile)
	if err != nil {
		return nil, err
	}

	signer, err := ssh.ParsePrivateKey(pemBytes)
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
