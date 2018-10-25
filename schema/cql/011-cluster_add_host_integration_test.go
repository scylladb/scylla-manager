// Copyright (C) 2017 ScyllaDB

// +build all integration

package cql

import (
	"context"
	"github.com/scylladb/gocqlx/qb"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/migrate"
	log "github.com/scylladb/golog"
	. "github.com/scylladb/mermaid/mermaidtest"
)

func TestClusterMoveHostsToHost011IntegrationTest(t *testing.T) {
	saveRegister()
	defer restoreRegister()
	session := CreateSessionWithoutMigration(t)

	cb := migrationCallback("011-cluster_add_host.cql", migrate.BeforeMigration)
	registerMigrationCallback("011-cluster_add_host.cql", migrate.BeforeMigration, func(ctx context.Context, session *gocql.Session, logger log.Logger) error {
		Print("Given: clusters")
		const insertClusterCql = `INSERT INTO cluster (id, hosts) VALUES (uuid(), {'host0', 'host1'})`
		ExecStmt(t, session, insertClusterCql)
		ExecStmt(t, session, insertClusterCql)

		Print("When: migrate")
		if err := cb(ctx, session, logger); err != nil {
			t.Fatal(err)
		}

		Print("Then: cluster host contains hosts[0]")
		stmt, _ := qb.Select("cluster").Columns("host").ToCql()
		q := session.Query(stmt)
		var host string
		if err := q.Scan(&host); err != nil {
			t.Fatal(err)
		}
		q.Release()
		if host != "host0" {
			t.Fatal(host)
		}

		return nil
	})

	if err := migrate.Migrate(context.Background(), session, "."); err != nil {
		t.Fatal("migrate:", err)
	}

}
