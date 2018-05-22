// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"io/ioutil"
	"net/http"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/ssh"
)

func newSSHManager(sshKeyStorageDir string) *sshManager {
	return &sshManager{
		sshKeyStorageDir: sshKeyStorageDir,
	}
}

type sshManager struct {
	sshKeyStorageDir string
}

func (m *sshManager) createTransport(c *Cluster) (http.RoundTripper, error) {
	config := ssh.Config{
		User:         c.SSHUser,
		IdentityFile: filepath.Join(m.sshKeyStorageDir, c.ID.String()),
	}

	if err := config.Validate(); err != nil {
		return http.DefaultTransport, nil
	}

	return ssh.NewProductionTransport(config)
}

func (m *sshManager) storeIdentityFile(c *Cluster) error {
	if len(c.SSHIdentityFile) == 0 {
		return nil
	}

	f := filepath.Join(m.sshKeyStorageDir, c.ID.String())

	if err := ioutil.WriteFile(f, c.SSHIdentityFile, 0400); err != nil {
		return errors.Wrapf(err, "unable to store identity file %q", f)
	}

	return nil
}
