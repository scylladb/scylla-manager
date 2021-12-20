// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package migrate

import (
	"context"
	"testing"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/schema"

	"github.com/scylladb/gocqlx/v2/migrate"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
)

func TestClusterMoveHostsToHost011IntegrationTest(t *testing.T) {
	saveRegister()
	defer restoreRegister()
	session := CreateSessionWithoutMigration(t)

	cbBefore := reg.Find(migrate.BeforeMigration, "011-cluster_add_host.cql")
	reg.Add(migrate.BeforeMigration, "011-cluster_add_host.cql", func(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		Print("Given: clusters")
		const insertClusterCql = `INSERT INTO cluster (id, hosts) VALUES (uuid(), {'host0', 'host1'})`
		ExecStmt(t, session, insertClusterCql)
		ExecStmt(t, session, insertClusterCql)

		return cbBefore(ctx, session, ev, name)
	})

	cbAfter := reg.Find(migrate.AfterMigration, "011-cluster_add_host.cql")
	reg.Add(migrate.AfterMigration, "011-cluster_add_host.cql", func(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		Print("When: migrate")
		if err := cbAfter(ctx, session, ev, name); err != nil {
			t.Fatal(err)
		}

		Print("Then: cluster host contains hosts[0]")
		q := qb.Select("cluster").Columns("host").Query(session)
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

	if err := migrate.FromFS(context.Background(), session, schema.Files); err != nil {
		t.Fatal("migrate:", err)
	}
}
