// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func (w *worker) tablesToRepair(views []View) []scyllaTable {
	var result []scyllaTable
	for _, v := range views {
		if v.Type == SecondaryIndex {
			result = append(result, scyllaTable{
				keyspace: v.Keyspace,
				table:    v.BaseTable,
			})
		}
	}
	return result
}

func (w *worker) repair(ctx context.Context, tablesToRepair []scyllaTable) error {
	var keyspaceFilter []string
	for _, table := range tablesToRepair {
		keyspaceFilter = append(keyspaceFilter, table.keyspace+"."+table.table)
	}
	repairProps, err := json.Marshal(map[string]any{
		"keyspace":  keyspaceFilter,
		"intensity": 0,
		"parallel":  0,
	})
	if err != nil {
		return errors.Wrap(err, "parse repair properties")
	}
	repairTarget, err := w.repairSvc.GetTarget(ctx, w.runInfo.ClusterID, repairProps)
	if err != nil {
		if errors.Is(err, repair.ErrEmptyRepair) {
			return nil
		}
		return errors.Wrap(err, "get repair target")
	}
	start := timeutc.Now()
	defer func() {
		repairDuration := timeutc.Since(start)
		w.logger.Info(ctx, "Repair info", "took", repairDuration, "target", repairTarget)
	}()
	repairTaskID, repairRunID := uuid.NewTime(), uuid.NewTime()
	return w.repairSvc.Repair(ctx, w.runInfo.ClusterID, repairTaskID, repairRunID, repairTarget)
}
