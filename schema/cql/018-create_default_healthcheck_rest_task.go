// Copyright (C) 2017 ScyllaDB

package cql

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/uuid"
)

func init() {
	registerMigrationCallback("018-empty.cql", migrate.AfterMigration, createDefaultHealthCheckRestTaskForClusterAfter018)
}

func createDefaultHealthCheckRestTaskForClusterAfter018(ctx context.Context, session *gocql.Session, logger log.Logger) error {
	stmt, names := qb.Select("cluster").Columns("id").ToCql()
	q := gocqlx.Query(session.Query(stmt), names)
	var ids []uuid.UUID
	if err := q.SelectRelease(&ids); err != nil {
		return err
	}

	const insertTaskCql = `INSERT INTO scheduler_task(cluster_id, type, id, enabled, sched, properties) 
VALUES (?, 'healthcheck_rest', uuid(), true, {start_date: ?, interval_seconds: ?, num_retries: ?}, ?)`
	iq := session.Query(insertTaskCql)
	defer iq.Release()

	for _, id := range ids {
		iq.Bind(id, timeutc.Now().Add(1*time.Minute), 60*60, 0, []byte{'{', '}'})
		if err := iq.Exec(); err != nil {
			logger.Error(ctx, "failed to add healthcheck REST task", "cluster_id", id, "error", err.Error())
		}
	}

	return nil
}
