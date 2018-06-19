// Copyright (C) 2017 ScyllaDB

package cql

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
	"github.com/scylladb/gocqlx/qb"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/timeutc"
)

func init() {
	registerMigrationCallback("008-drop_repair_unit.cql", migrate.AfterMigration, createDefaultRepairTaskForClusterAfter008)
}

func createDefaultRepairTaskForClusterAfter008(ctx context.Context, session *gocql.Session, logger log.Logger) error {
	stmt, names := qb.Select("cluster").Columns("id").ToCql()
	q := gocqlx.Query(session.Query(stmt).WithContext(ctx), names)
	var clusters []*cluster.Cluster
	if err := q.SelectRelease(&clusters); err != nil {
		return err
	}

	const insertTaskCql = `INSERT INTO scheduler_task(cluster_id, type, id, enabled, sched, properties) VALUES (?, 'repair', uuid(), true, {start_date: ?, interval_days: ?, num_retries: ?}, ?)`
	iq := session.Query(insertTaskCql).WithContext(ctx)
	defer iq.Release()

	for _, c := range clusters {
		iq.Bind(c.ID, timeutc.TodayMidnight(), 7, 3, []byte{'{', '}'})
		if err := iq.Exec(); err != nil {
			logger.Error(ctx, "failed to add repair task", "cluster_id", c.ID, "error", err.Error())
		}
	}

	return nil
}
