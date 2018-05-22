// Copyright (C) 2017 ScyllaDB

package ssh

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/fsutil"
	"go.uber.org/multierr"
	"golang.org/x/crypto/ssh"
)

// Config specifies SSH configuration.
type Config struct {
	User         string `yaml:"user"`
	IdentityFile string `yaml:"identity_file"`
}

// Validate checks if all the fields are properly set.
func (c *Config) Validate() (err error) {
	if c == nil {
		return mermaid.ErrNilPtr
	}

	if c.User == "" {
		err = multierr.Append(err, errors.New("missing user"))
	}
	if c.IdentityFile == "" {
		err = multierr.Append(err, errors.New("missing identity_file"))
	}

	if err := fsutil.CheckPerm(c.IdentityFile, 0400); err != nil {
		err = multierr.Append(err, errors.New("identity_file has the wrong permissions"))
	}

	return
}

// NewProductionClientConfig returns configuration with a key based authentication.
func NewProductionClientConfig(c Config) (*ssh.ClientConfig, error) {
	auth, err := keyPairAuthMethod(c.IdentityFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse %q", c.IdentityFile)
	}

	return &ssh.ClientConfig{
		User:            c.User,
		Auth:            []ssh.AuthMethod{auth},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
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
		User:            "scylla-manager",
		Auth:            []ssh.AuthMethod{ssh.Password("test")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
}
