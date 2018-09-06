// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"go.uber.org/multierr"
	"golang.org/x/crypto/ssh"
)

// Config specifies SSH configuration.
type Config struct {
	User         string
	IdentityFile []byte
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() (err error) {
	if c == nil {
		return mermaid.ErrNilPtr
	}

	if c.User == "" {
		err = multierr.Append(err, errors.New("missing user"))
	}
	if len(c.IdentityFile) == 0 {
		err = multierr.Append(err, errors.New("missing identity_file"))
	}
	return
}

// NewProductionClientConfig returns configuration with a key based authentication.
func NewProductionClientConfig(c Config) (*ssh.ClientConfig, error) {
	auth, err := keyPairAuthMethod(c.IdentityFile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse identity file")
	}

	return &ssh.ClientConfig{
		User:            c.User,
		Auth:            []ssh.AuthMethod{auth},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}, nil
}

func keyPairAuthMethod(pemBytes []byte) (ssh.AuthMethod, error) {
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
		User:            "root",
		Auth:            []ssh.AuthMethod{ssh.Password("root")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
}
