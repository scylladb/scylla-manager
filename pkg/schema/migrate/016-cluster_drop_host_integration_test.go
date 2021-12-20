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

func TestClusterMoveHostToKnownHostsBefore016Integration(t *testing.T) {
	saveRegister()
	defer restoreRegister()
	session := CreateSessionWithoutMigration(t)

	cb := reg.Find(migrate.BeforeMigration, "016-cluster_drop_host.cql")
	reg.Add(migrate.BeforeMigration, "016-cluster_drop_host.cql", func(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		Print("Given: clusters")
		const insertClusterCql = `INSERT INTO cluster (id, host) VALUES (uuid(), 'host0')`
		ExecStmt(t, session, insertClusterCql)

		Print("When: migrate")
		if err := cb(ctx, session, ev, name); err != nil {
			t.Fatal(err)
		}

		Print("Then: cluster host contains hosts[0]")
		q := qb.Select("cluster").Columns("known_hosts").Query(session)

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

	if err := migrate.FromFS(context.Background(), session, schema.Files); err != nil {
		t.Fatal("migrate:", err)
	}
}
