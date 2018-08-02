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

func TestClusterMoveHostsToHost012IntegrationTest(t *testing.T) {
	saveRegister()
	defer restoreRegister()

	session := CreateSessionWithoutMigration(t)

	Print("Given: tasks")
	cb := migrationCallback("012-scheduler_rename_interval_days_to_interval_seconds.cql", migrate.AfterMigration)
	registerMigrationCallback("012-scheduler_rename_interval_days_to_interval_seconds.cql", migrate.AfterMigration, func(ctx context.Context, session *gocql.Session, logger log.Logger) error {
		const insertTaskCql = `INSERT INTO scheduler_task (cluster_id, type, id, sched) VALUES (uuid(), 'repair', uuid(),  {start_date: '2018-08-04', interval_seconds: 1, num_retries: 3});`
		ExecStmt(t, session, insertTaskCql)
		ExecStmt(t, session, insertTaskCql)
		return cb(ctx, session, logger)
	})

	Print("When: migrate")
	if err := migrate.Migrate(context.Background(), session, "."); err != nil {
		t.Fatal("migrate:", err)
	}

	Print("Then: sched.interval_seconds is updated")
	q := session.Query("SELECT sched.interval_seconds FROM scheduler_task")
	var interval int
	if err := q.Scan(&interval); err != nil {
		t.Fatal(err)
	}
	q.Release()
	if interval != 24*60*60 {
		t.Fatal(interval)
	}
}
