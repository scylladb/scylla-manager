// Copyright (C) 2017 ScyllaDB

package cql

import (
	"context"
	"os"
	"path/filepath"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid/uuid"
	"gopkg.in/yaml.v2"
)

func init() {
	h := copySSHInfoToCluster006{
		oldConfigFile: "/etc/scylla-manager/scylla-manager.yaml.rpmsave",
		dir:           "/var/lib/scylla-manager/.certs",
	}
	registerMigrationCallback("006-ssh_user_per_cluster.cql", migrate.AfterMigration, h.After)
}

type copySSHInfoToCluster006 struct {
	oldConfigFile string
	dir           string
}

func (h copySSHInfoToCluster006) After(ctx context.Context, session *gocql.Session, logger log.Logger) error {
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

	stmt, names := qb.Select("cluster").Columns("id").ToCql()
	q := gocqlx.Query(session.Query(stmt), names)
	var ids []uuid.UUID
	if err := q.SelectRelease(&ids); err != nil {
		return err
	}

	const updateClusterCql = `INSERT INTO cluster(id, ssh_user) VALUES (?, ?)`
	iq := session.Query(updateClusterCql)
	defer iq.Release()

	for _, id := range ids {
		identityFile := filepath.Join(h.dir, id.String())
		if err := os.Link(cfg.IdentityFile, identityFile); err != nil {
			logger.Info(ctx, "Failed to link ssh identity file",
				"from", cfg.IdentityFile,
				"to", identityFile,
				"error", err,
			)
			continue
		}
		if err := os.Chmod(identityFile, 0600); err != nil {
			logger.Info(ctx, "Failed to change identity file permissions",
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
