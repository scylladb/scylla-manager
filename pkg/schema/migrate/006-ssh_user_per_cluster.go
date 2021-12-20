// Copyright (C) 2017 ScyllaDB

package migrate

import (
	"context"
	"os"
	"path/filepath"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"gopkg.in/yaml.v2"
)

func init() {
	h := copySSHInfoToCluster006{
		oldConfigFile: "/etc/scylla-manager/scylla-manager.yaml.rpmsave",
		dir:           "/var/lib/scylla-manager/.certs",
	}
	reg.Add(migrate.AfterMigration, "006-ssh_user_per_cluster.cql", h.After)
}

type copySSHInfoToCluster006 struct {
	oldConfigFile string
	dir           string
}

func (h copySSHInfoToCluster006) After(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
	type sshConfig struct {
		User         string `yaml:"user,omitempty"`
		IdentityFile string `yaml:"identity_file,omitempty"`
	}

	cfg := sshConfig{
		User:         "scylla-manager",
		IdentityFile: "/var/lib/scylla-manager/scylla_manager.pem",
	}

	if f, err := os.Open(h.oldConfigFile); err == nil {
		if err := yaml.NewDecoder(f).Decode(struct {
			SSH *sshConfig `yaml:"ssh,omitempty"`
		}{&cfg}); err != nil {
			return err
		}
	}

	_, err := os.Stat(cfg.IdentityFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	q := qb.Select("cluster").Columns("id").Query(session)
	var ids []uuid.UUID
	if err := q.SelectRelease(&ids); err != nil {
		return err
	}

	iq := qb.Insert("cluster").Columns("id", "ssh_user").Query(session)
	defer iq.Release()

	for _, id := range ids {
		identityFile := filepath.Join(h.dir, id.String())
		if err := os.Link(cfg.IdentityFile, identityFile); err != nil {
			Logger.Info(ctx, "Failed to link ssh identity file",
				"from", cfg.IdentityFile,
				"to", identityFile,
				"error", err,
			)
			continue
		}
		if err := os.Chmod(identityFile, 0o600); err != nil {
			Logger.Info(ctx, "Failed to change identity file permissions",
				"file", identityFile,
				"error", err,
			)
			continue
		}

		if err := iq.Bind(id, cfg.User).Exec(); err != nil {
			return err
		}
	}

	return nil
}
