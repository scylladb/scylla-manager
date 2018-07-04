// Copyright (C) 2017 ScyllaDB

package cql

import (
	"context"
	"os"
	"os/user"
	"path/filepath"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
	"github.com/scylladb/gocqlx/qb"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/schema"
	"gopkg.in/yaml.v2"
)

func init() {
	registerMigrationCallback("006-ssh_user_per_cluster.cql", migrate.AfterMigration, copySSHInfoToClusterAfter006)
}

type config struct {
	SSH sshConfig `yaml:"ssh,omitempty"`
}

type sshConfig struct {
	User         string `yaml:"user,omitempty"`
	IdentityFile string `yaml:"identity_file,omitempty"`
}

func copySSHInfoToClusterAfter006(ctx context.Context, session *gocql.Session, logger log.Logger) error {
	u, _ := user.Current()
	f := "/etc/scylla-manager.yaml.rpmsave"
	cfg := sshConfig{
		User:         "scylla-manager",
		IdentityFile: filepath.Join(u.HomeDir, "scylla_manager.pem"),
	}

	if f, err := os.Open(f); err == nil {
		if err := yaml.NewDecoder(f).Decode(&config{SSH: cfg}); err != nil {
			return err
		}
	}

	stmt, names := qb.Select(schema.Cluster.Name).ToCql()
	q := gocqlx.Query(session.Query(stmt).WithContext(ctx), names)

	var clusters []*cluster.Cluster
	if err := q.SelectRelease(&clusters); err != nil {
		return err
	}

	stmt, names = schema.Cluster.Insert()
	q = gocqlx.Query(session.Query(stmt).WithContext(ctx), names)
	defer q.Release()

	toDir := filepath.Dir(cfg.IdentityFile)
	for _, c := range clusters {
		if err := os.Link(cfg.IdentityFile, filepath.Join(toDir, c.ID.String())); err != nil {
			logger.Info(ctx, "unable to link ssh identity file",
				"identity_file", filepath.Join(toDir, c.ID.String()), "error", err)
			continue
		}
		c.SSHUser = cfg.User
		if err := q.BindStruct(c).Exec(); err != nil {
			return err
		}
	}

	return nil
}
