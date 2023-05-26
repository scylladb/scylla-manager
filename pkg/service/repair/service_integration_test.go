// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package repair_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestServiceGetLastResumableRunIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)

	s, err := repair.NewService(
		session,
		repair.DefaultConfig(),
		metrics.NewRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return nil, errors.New("not implemented")
		},
		func(context.Context, uuid.UUID) (gocqlx.Session, error) {
			return gocqlx.Session{}, errors.New("not implemented")
		},
		log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

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

		_, err := s.GetLastResumableRun(ctx, clusterID, taskID)
		if !errors.Is(err, service.ErrNotFound) {
			t.Fatal(err)
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
		}
		putRun(t, r1)
		putRunProgress(t, r0, 10, 10, 0)

		_, err := s.GetLastResumableRun(ctx, clusterID, taskID)
		if !errors.Is(err, service.ErrNotFound) {
			t.Fatal(err)
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

		r, err := s.GetLastResumableRun(ctx, clusterID, taskID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r0, UUIDComparer(), cmp.AllowUnexported(repair.Run{})); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestServiceRepairOrderIntegration(t *testing.T) {
	ctx := context.Background()
	session := CreateScyllaManagerDBSession(t)
	clusterSession := CreateSessionAndDropAllKeyspaces(t, ManagedClusterHosts())
	h := newRepairTestHelper(t, session, clusterSession, repair.DefaultConfig())

	// system_auth keyspace won't be repaired unless it has RF > 1
	err := clusterSession.ExecStmt("ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = clusterSession.ExecStmt("ALTER KEYSPACE system_auth WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
		if err != nil {
			t.Error(err)
		}
	}()

	const (
		ks1 = "test_repair_rf_1"
		ks2 = "test_repair_rf_2"
		ks3 = "test_repair_rf_3"
		t1  = "test_table_1"
		t2  = "test_table_2"
	)
	createKeyspace(t, clusterSession, ks1, 1, 1)
	createKeyspace(t, clusterSession, ks2, 2, 2)
	createKeyspace(t, clusterSession, ks3, 3, 3)
	WriteData(t, clusterSession, ks1, 0, t1, t2)
	WriteData(t, clusterSession, ks2, 0, t1, t2)
	WriteData(t, clusterSession, ks3, 0, t1, t2)

	createMVFormat := "CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE data IS NOT NULL PRIMARY KEY (id, data)"
	ExecStmt(h.t, clusterSession, fmt.Sprintf(createMVFormat, ks1, "test_mv_1", ks1, t1))
	ExecStmt(h.t, clusterSession, fmt.Sprintf(createMVFormat, ks2, "test_mv_2", ks2, t1))
	ExecStmt(h.t, clusterSession, fmt.Sprintf(createMVFormat, ks2, "test_mv_3", ks2, t1))
	ExecStmt(h.t, clusterSession, fmt.Sprintf(createMVFormat, ks3, "test_mv_5", ks3, t2))

	createIndexFormat := "CREATE INDEX %s ON %s.%s (data)"
	ExecStmt(h.t, clusterSession, fmt.Sprintf(createIndexFormat, "test_idx_1", ks1, t1))
	ExecStmt(h.t, clusterSession, fmt.Sprintf(createIndexFormat, "test_idx_2", ks1, t2))
	ExecStmt(h.t, clusterSession, fmt.Sprintf(createIndexFormat, "test_idx_4", ks2, t2))

	testCases := []struct {
		Name      string
		KSPattern []string
	}{
		{
			Name:      "Order base user tables",
			KSPattern: []string{"test_repair_rf_1", "test_repair_rf_2", "test_repair_rf_3", "!*.test_mv*", "!*.test_idx*"},
		},
		{
			Name:      "Order base user and system tables",
			KSPattern: []string{"*", "!*.test_mv*", "!*.test_idx*"},
		},
		{
			Name: "Order all tables",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			props := map[string]any{"keyspace": tc.KSPattern}
			target := h.generateTarget(props)
			order := h.service.RepairOrder(ctx, h.clusterID, target)
			SaveGoldenJSONFileIfNeeded(t, order)

			var golden []repair.TableName
			LoadGoldenJSONFile(t, &golden)

			if diff := cmp.Diff(golden, order); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
