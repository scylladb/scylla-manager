// Copyright (C) 2017 ScyllaDB

// +build all integration

package cql

import (
	"context"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/migrate"
	"github.com/scylladb/gocqlx/qb"
	log "github.com/scylladb/golog"
	. "github.com/scylladb/mermaid/mermaidtest"
)

func TestClusterMoveHostsToHost011IntegrationTest(t *testing.T) {
	session := CreateSessionWithoutMigration(t)

	Print("Given: clusters")
	cb := migrationCallback("011-cluster_add_host.cql", migrate.BeforeMigration)
	registerMigrationCallback("011-cluster_add_host.cql", migrate.BeforeMigration, func(ctx context.Context, session *gocql.Session, logger log.Logger) error {
		const insertClusterCql = `INSERT INTO cluster (id, hosts) VALUES (uuid(), {'host0', 'host1'})`
		ExecStmt(t, session, insertClusterCql)
		ExecStmt(t, session, insertClusterCql)
		return cb(ctx, session, logger)
	})

	Print("When: migrate")
	if err := migrate.Migrate(context.Background(), session, "."); err != nil {
		t.Fatal("migrate:", err)
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
}
