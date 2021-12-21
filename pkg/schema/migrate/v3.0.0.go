// Copyright (C) 2021 ScyllaDB

package migrate

import (
	"context"
	"encoding/json"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/dbutil"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/table"
)

func init() {
	reg.Add(migrate.CallComment, "rewriteHealthCheck30", rewriteHealthCheck30)
}

func rewriteHealthCheck30(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
	schedulerTask := table.New(table.Metadata{
		Name: "scheduler_task",
		Columns: []string{
			"cluster_id",
			"type",
			"id",
			"enabled",
			"name",
			"properties",
			"sched",
			"tags",
		},
		PartKey: []string{
			"cluster_id",
		},
		SortKey: []string{
			"type",
			"id",
		},
	})

	const (
		healthCheckCQLTask        = "healthcheck"
		healthCheckRESTTask       = "healthcheck_rest"
		healthCheckAlternatorTask = "healthcheck_alternator"

		newHealthCheck = "healthcheck"
	)

	healthCheckModeProperties := func(mode string) json.RawMessage {
		return json.RawMessage(`{"mode": "` + mode + `"}`)
	}

	var deleteKeys []map[string]interface{}
	markForDelete := func(m map[string]interface{}) {
		deleteKeys = append(deleteKeys, map[string]interface{}{
			"cluster_id": m["cluster_id"],
			"type":       m["type"],
			"id":         m["id"],
		})
	}

	err := dbutil.RewriteTable(session, schedulerTask, schedulerTask, func(m map[string]interface{}) {
		switch m["type"] {
		case healthCheckCQLTask:
			// Do not mark for delete as we reuse primary key as the task type does not change.
			m["type"] = newHealthCheck
			m["name"] = "cql"
			m["properties"] = healthCheckModeProperties("cql")
		case healthCheckRESTTask:
			markForDelete(m)
			m["type"] = newHealthCheck
			m["name"] = "rest"
			m["properties"] = healthCheckModeProperties("rest")
		case healthCheckAlternatorTask:
			markForDelete(m)
			m["type"] = newHealthCheck
			m["name"] = "alternator"
			m["properties"] = healthCheckModeProperties("alternator")
		default:
			for k := range m {
				delete(m, k)
			}
		}
	})
	if err != nil {
		return err
	}

	dq := schedulerTask.DeleteQuery(session)
	defer dq.Release()

	for _, m := range deleteKeys {
		if err := dq.BindMap(m).Exec(); err != nil {
			return err
		}
	}

	return nil
}
