// Copyright (C) 2017 ScyllaDB

// +build all integration

package migrate

import (
	"context"
	"github.com/scylladb/gocqlx/qb"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/migrate"
	. "github.com/scylladb/mermaid/pkg/testutils"
)

func TestClusterMoveHostToKnownHostsBefore016Integration(t *testing.T) {
	saveRegister()
	defer restoreRegister()
	session := CreateSessionWithoutMigration(t)

	cb := findCallback("016-cluster_drop_host.cql", migrate.BeforeMigration)
	registerCallback("016-cluster_drop_host.cql", migrate.BeforeMigration, func(ctx context.Context, session *gocql.Session, logger log.Logger) error {
		Print("Given: clusters")
		const insertClusterCql = `INSERT INTO cluster (id, host) VALUES (uuid(), 'host0')`
		ExecStmt(t, session, insertClusterCql)

		Print("When: migrate")
		if err := cb(ctx, session, logger); err != nil {
			t.Fatal(err)
		}

		Print("Then: cluster host contains hosts[0]")
		stmt, _ := qb.Select("cluster").Columns("known_hosts").ToCql()
		q := session.Query(stmt)

		var host []string
		if err := q.Scan(&host); err != nil {
			t.Fatal(err)
		}
		q.Release()
		if len(host) != 1 || host[0] != "host0" {
			t.Fatal(host)
		}

		return nil
	})

	if err := migrate.Migrate(context.Background(), session, SchemaDir()); err != nil {
		t.Fatal("migrate:", err)
	}
}
