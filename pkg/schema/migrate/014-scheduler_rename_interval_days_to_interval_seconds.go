// Copyright (C) 2017 ScyllaDB

package migrate

import (
	"context"
	"time"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func init() {
	reg.Add(migrate.AfterMigration, "014-scheduler_rename_interval_days_to_interval_seconds.cql", adjustScheduleIntervalAfter014)
}

func adjustScheduleIntervalAfter014(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
	const selectSchedStmt = "SELECT cluster_id, type, id, sched FROM scheduler_task"
	q := session.Query(selectSchedStmt, nil)
	defer q.Release()

	const updateSchedCql = `INSERT INTO scheduler_task(cluster_id, type, id, sched) VALUES (?, ?, ?, ?)`
	update := session.Query(updateSchedCql, nil)
	defer update.Release()

	var (
		clusterID uuid.UUID
		t         string
		id        uuid.UUID
		sched     map[string]interface{}
	)

	iter := q.Iter()
	for iter.Scan(&clusterID, &t, &id, &sched) {
		i := sched["interval_seconds"]
		sched["interval_seconds"] = i.(int) * 24 * int(time.Hour/time.Second)

		if err := update.Bind(clusterID, t, id, sched).Exec(); err != nil {
			return err
		}
	}
	return iter.Close()
}
