// Copyright (C) 2017 ScyllaDB

package migrate

import (
	"context"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func init() {
	registerCallback("015-cluster_add_known_hosts.cql", migrate.AfterMigration, createDefaultHealthCheckTaskForClusterAfter015)
}

func createDefaultHealthCheckTaskForClusterAfter015(ctx context.Context, session gocqlx.Session, logger log.Logger) error {
	q := qb.Select("cluster").Columns("id").Query(session)
	var ids []uuid.UUID
	if err := q.SelectRelease(&ids); err != nil {
		return err
	}

	const insertTaskCql = `INSERT INTO scheduler_task(cluster_id, type, id, enabled, sched, properties) 
VALUES (?, 'healthcheck', uuid(), true, {start_date: ?, interval_seconds: ?, num_retries: ?}, ?)`
	iq := session.Query(insertTaskCql, nil)
	defer iq.Release()

	for _, id := range ids {
		iq.Bind(id, timeutc.Now().Add(30*time.Second), 15, 0, []byte{'{', '}'})
		if err := iq.Exec(); err != nil {
			logger.Error(ctx, "Failed to add healthcheck task", "cluster_id", id, "error", err.Error())
		}
	}

	return nil
}
