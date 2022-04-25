// Copyright (C) 2021 ScyllaDB

package migrate

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func init() {
	reg.Add(migrate.CallComment, "setExistingTasksDeleted", setExistingTasksDeleted)
}

func setExistingTasksDeleted(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
	q := qb.Select("scheduler_task").Columns("id", "cluster_id", "type").Query(session)

	type Task struct {
		ClusterID uuid.UUID `db:"cluster_id"`
		ID        uuid.UUID `db:"id"`
		Type      string    `db:"type"`
	}

	var tasks []*Task
	if err := q.SelectRelease(&tasks); err != nil {
		return err
	}

	const setDeletedCql = `UPDATE scheduler_task SET deleted = false WHERE cluster_id = ? AND type = ? AND id = ?`
	q = session.Query(setDeletedCql, nil)
	defer q.Release()

	for _, t := range tasks {
		if err := q.Bind(t.ClusterID, t.Type, t.ID).Exec(); err != nil {
			return err
		}
	}

	return nil
}
