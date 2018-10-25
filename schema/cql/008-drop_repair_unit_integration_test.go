// Copyright (C) 2017 ScyllaDB

// +build all integration

package cql

import (
	"context"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/migrate"
	log "github.com/scylladb/golog"
	. "github.com/scylladb/mermaid/mermaidtest"
)

func TestCreateDefaultRepairTaskForClusterAfter008Integration(t *testing.T) {
	saveRegister()
	defer restoreRegister()
	session := CreateSessionWithoutMigration(t)

	cb := migrationCallback("008-drop_repair_unit.cql", migrate.AfterMigration)
	registerMigrationCallback("008-drop_repair_unit.cql", migrate.AfterMigration, func(ctx context.Context, session *gocql.Session, logger log.Logger) error {
		Print("Given: clusters")
		const insertClusterCql = `INSERT INTO cluster (id) VALUES (uuid())`
		ExecStmt(t, session, insertClusterCql)
		ExecStmt(t, session, insertClusterCql)

		Print("When: migrate")
		if err := cb(ctx, session, logger); err != nil {
			t.Fatal(err)
		}

		Print("When: migrate")
		const countSchedulerTask = `SELECT COUNT(*) FROM scheduler_task`
		q := session.Query(countSchedulerTask)
		defer q.Release()

		Print("Then: tasks are created")
		var count int
		if err := q.Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatal()
		}

		return nil
	})

	if err := migrate.Migrate(context.Background(), session, "."); err != nil {
		t.Fatal("migrate:", err)
	}
}
