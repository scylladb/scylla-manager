// Copyright (C) 2017 ScyllaDB

// +build all integration

package schema_test

import (
	"context"
	"testing"

	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/table"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/uuid"
)

func TestFixStatusIntegration(t *testing.T) {
	session := CreateSession(t)

	ExecStmt(t, session, `CREATE TABLE IF NOT EXISTS test_fix_status_run (
		task_id uuid,
		id timeuuid,
		status text,
		cause text,
		PRIMARY KEY (task_id, id)
	) WITH CLUSTERING ORDER BY (id DESC)`)
	ExecStmt(t, session, "TRUNCATE TABLE test_fix_status_run")

	runTable := table.New(table.Metadata{
		Name: "test_fix_status_run",
		Columns: []string{
			"task_id",
			"id",
			"status",
			"cause",
		},
		PartKey: []string{"task_id"},
		SortKey: []string{"id"},
	})

	type run struct {
		TaskID uuid.UUID
		ID     uuid.UUID
		Status runner.Status
		Cause  string
	}

	putRun := func(t *testing.T, r *run) {
		stmt, names := runTable.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(r).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}

	getRun := func(t *testing.T, r *run) {
		stmt, names := runTable.Get()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(r).Get(r); err != nil {
			t.Fatal(err)
		}
	}

	var (
		ctx   = context.Background()
		task0 = uuid.MustRandom()
		task1 = uuid.MustRandom()
		task2 = uuid.MustRandom()
	)

	r0 := &run{
		TaskID: task0,
		ID:     uuid.NewTime(),
		Status: runner.StatusStarting,
	}
	putRun(t, r0)

	r1 := &run{
		TaskID: task0,
		ID:     uuid.NewTime(),
		Status: runner.StatusStarting,
	}
	putRun(t, r1)

	r2 := &run{
		TaskID: task1,
		ID:     uuid.NewTime(),
		Status: runner.StatusRunning,
	}
	putRun(t, r2)

	r3 := &run{
		TaskID: task1,
		ID:     uuid.NewTime(),
		Status: runner.StatusRunning,
	}
	putRun(t, r3)

	r4 := &run{
		TaskID: task2,
		ID:     uuid.NewTime(),
		Status: runner.StatusStopping,
	}
	putRun(t, r4)

	r5 := &run{
		TaskID: task2,
		ID:     uuid.NewTime(),
		Status: runner.StatusStopping,
	}
	putRun(t, r5)

	if err := schema.FixRunStatus(ctx, session, runTable); err != nil {
		t.Fatal(err)
	}

	r := &run{}

	*r = *r1
	getRun(t, r)
	if r.Status != runner.StatusAborted {
		t.Fatal("invalid status", r.Status)
	}
	if r.Cause == "" {
		t.Fatal("missing cause")
	}

	*r = *r3
	getRun(t, r)
	if r.Status != runner.StatusAborted {
		t.Fatal("invalid status", r.Status)
	}
	if r.Cause == "" {
		t.Fatal("missing cause")
	}

	*r = *r5
	getRun(t, r)
	if r.Status != runner.StatusAborted {
		t.Fatal("invalid status", r.Status)
	}
	if r.Cause == "" {
		t.Fatal("missing cause")
	}
}
