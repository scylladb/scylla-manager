// Copyright (C) 2017 ScyllaDB

// +build all integration

package migrate

import (
	"context"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/migrate"
	. "github.com/scylladb/mermaid/pkg/testutils"
)

func TestClusterMoveHostsToHost014IntegrationTest(t *testing.T) {
	saveRegister()
	defer restoreRegister()
	session := CreateSessionWithoutMigration(t)

	cb := findCallback("014-scheduler_rename_interval_days_to_interval_seconds.cql", migrate.AfterMigration)
	registerCallback("014-scheduler_rename_interval_days_to_interval_seconds.cql", migrate.AfterMigration, func(ctx context.Context, session *gocql.Session, logger log.Logger) error {
		Print("Given: tasks")
		const insertTaskCql = `INSERT INTO scheduler_task (cluster_id, type, id, sched) VALUES (uuid(), 'repair', uuid(),  {start_date: '2018-08-04', interval_seconds: 1, num_retries: 3});`
		ExecStmt(t, session, insertTaskCql)
		ExecStmt(t, session, insertTaskCql)

		Print("When: migrate")
		if err := cb(ctx, session, logger); err != nil {
			t.Fatal(err)
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

		return nil
	})

	if err := migrate.Migrate(context.Background(), session, SchemaDir()); err != nil {
		t.Fatal("migrate:", err)
	}
}
