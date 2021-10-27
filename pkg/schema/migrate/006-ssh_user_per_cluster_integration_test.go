// Copyright (C) 2017 ScyllaDB

// +build all integration

package migrate

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/schema"
)

func TestCopySSHInfoToClusterAfter006Integration(t *testing.T) {
	saveRegister()
	defer restoreRegister()
	session := CreateSessionWithoutMigration(t)

	Print("Given: config files")
	dir, err := ioutil.TempDir("", "scylla-manager.schema.cql.TestCopySSHInfoToClusterAfter006Integration")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Remove(dir)
	}()

	pemFile := filepath.Join(dir, "scylla_manager.pem")
	if err := ioutil.WriteFile(pemFile, []byte("pem"), 0400); err != nil {
		t.Fatal(err)
	}

	oldConfig := `# Communication with Scylla nodes.
ssh:
  # SSH user name, user must exist on Scylla nodes.
  user: user
  # PEM encoded SSH private key for user.
  identity_file: ` + pemFile

	oldConfigFile := filepath.Join(dir, "scylla-manager.yaml.rpmsave")
	if err := ioutil.WriteFile(oldConfigFile, []byte(oldConfig), 0400); err != nil {
		t.Fatal(err)
	}

	registerCallback("006-ssh_user_per_cluster.cql", migrate.AfterMigration, func(ctx context.Context, session gocqlx.Session, logger log.Logger) error {
		Print("And: clusters")
		const insertClusterCql = `INSERT INTO cluster (id) VALUES (uuid())`
		ExecStmt(t, session, insertClusterCql)

		Print("When: migrate")
		h := copySSHInfoToCluster006{
			oldConfigFile: oldConfigFile,
			dir:           dir,
		}
		if err := h.After(ctx, session, logger); err != nil {
			t.Fatal(err)
		}

		Print("Then: SSH user is added")
		q := qb.Select("cluster").Columns("id", "ssh_user").Query(session)
		var (
			id      uuid.UUID
			sshUser string
		)
		if err := q.Scan(&id, &sshUser); err != nil {
			t.Fatal(err)
		}
		q.Release()
		if sshUser != "user" {
			t.Fatal(sshUser)
		}

		Print("And: file exists")
		if _, err := os.Stat(filepath.Join(dir, id.String())); err != nil {
			t.Fatal(err)
		}
		return nil
	})

	if err := migrate.FromFS(context.Background(), session, schema.Files); err != nil {
		t.Fatal("migrate:", err)
	}
}
