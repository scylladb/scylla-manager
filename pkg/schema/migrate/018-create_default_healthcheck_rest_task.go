// Copyright (C) 2017 ScyllaDB

package migrate

import (
	"context"
	"time"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func init() {
	reg.Add(migrate.AfterMigration, "018-empty.cql", createDefaultHealthCheckRestTaskForClusterAfter018)
}

func createDefaultHealthCheckRestTaskForClusterAfter018(ctx context.Context, session gocqlx.Session, _ migrate.CallbackEvent, _ string) error {
	q := qb.Select("cluster").Columns("id").Query(session)
	var ids []uuid.UUID
	if err := q.SelectRelease(&ids); err != nil {
		return err
	}

	const insertTaskCql = `INSERT INTO scheduler_task(cluster_id, type, id, enabled, sched, properties) 
VALUES (?, 'healthcheck_rest', uuid(), true, {start_date: ?, interval_seconds: ?, num_retries: ?}, ?)`
	iq := session.Query(insertTaskCql, nil)
	defer iq.Release()

	for _, id := range ids {
		iq.Bind(id, timeutc.Now().Add(1*time.Minute), 60*60, 0, []byte{'{', '}'})
		if err := iq.Exec(); err != nil {
			Logger.Error(ctx, "Failed to add healthcheck REST task", "cluster_id", id, "error", err.Error())
		}
	}

	return nil
}
