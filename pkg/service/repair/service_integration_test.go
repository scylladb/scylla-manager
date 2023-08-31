// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package repair_test

import (
	"context"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestServiceGetLastResumableRunIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)

	putRun := func(t *testing.T, r *repair.Run) {
		if err := table.RepairRun.InsertQuery(session).BindStruct(&r).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}
	putRunProgress := func(t *testing.T, r *repair.Run, tokenRanges, tokenSuccess, tokenError int64) {
		p := repair.RunProgress{
			ClusterID:   r.ClusterID,
			TaskID:      r.TaskID,
			RunID:       r.ID,
			Host:        "172.16.1.3",
			TokenRanges: tokenRanges,
			Success:     tokenSuccess,
			Error:       tokenError,
		}
		if err := table.RepairRunProgress.InsertQuery(session).BindStruct(&p).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("no started runs", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
		}
		putRun(t, r1)

		r2 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
		}

		pm := repair.NewDBProgressManager(r2, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		if err := pm.SetPrevRunID(context.Background(), 0); err != nil {
			t.Fatal(err)
		}
		if r2.PrevID != uuid.Nil {
			t.Fatal("expected nil prev run ID")
		}
	})

	t.Run("started run before done run", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
		}
		putRun(t, r0)
		putRunProgress(t, r0, 10, 5, 0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			EndTime:   timeutc.Now(),
		}
		putRun(t, r1)
		putRunProgress(t, r1, 10, 10, 0)

		r2 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
		}

		pm := repair.NewDBProgressManager(r2, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		if err := pm.SetPrevRunID(context.Background(), 0); err != nil {
			t.Fatal(err)
		}
		if r2.PrevID != uuid.Nil {
			t.Fatal("expected nil prev run ID")
		}
	})

	t.Run("started run before not started run", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
		}
		putRun(t, r0)
		putRunProgress(t, r0, 10, 5, 1)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
		}
		putRun(t, r1)

		r2 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
		}

		pm := repair.NewDBProgressManager(r2, session, metrics.NewRepairMetrics(), log.NewDevelopment())
		if err := pm.SetPrevRunID(context.Background(), 0); err != nil {
			t.Fatal(err)
		}
		if r2.PrevID != r0.ID {
			t.Fatal("expected r0 run ID")
		}
	})
}
