// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap/zapcore"
)

func TestServiceStorageIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)

	s, err := repair.NewService(
		session,
		repair.DefaultConfig(),
		func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return nil, errors.New("not implemented")
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return nil, errors.New("not implemented")
		},
		log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	putRun := func(t *testing.T, r *repair.Run) {
		stmt, names := schema.RepairRun.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(r).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}
	putRunProgress := func(t *testing.T, r *repair.Run) {
		p := repair.RunProgress{
			ClusterID: r.ClusterID,
			TaskID:    r.TaskID,
			RunID:     r.ID,
			Host:      "172.16.1.3",
			Shard:     0,
		}
		stmt, names := schema.RepairRunProgress.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(&p).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("init", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		task0 := uuid.MustRandom()
		task1 := uuid.MustRandom()

		r0 := &repair.Run{
			ID:        uuid.NewTime(),
			ClusterID: clusterID,
			TaskID:    task0,
			Status:    runner.StatusRunning,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ID:        uuid.NewTime(),
			ClusterID: clusterID,
			TaskID:    task0,
			Status:    runner.StatusRunning,
		}
		putRun(t, r1)

		r2 := &repair.Run{
			ID:        uuid.NewTime(),
			ClusterID: clusterID,
			TaskID:    task1,
			Status:    runner.StatusStopping,
		}
		putRun(t, r2)

		r3 := &repair.Run{
			ID:        uuid.NewTime(),
			ClusterID: clusterID,
			TaskID:    task1,
			Status:    runner.StatusStopping,
		}
		putRun(t, r3)

		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}

		if r, err := s.GetRun(ctx, r1.ClusterID, r1.TaskID, r1.ID); err != nil {
			t.Fatal(err)
		} else if r.Status != runner.StatusStopped {
			t.Fatal("invalid status", r.Status)
		}

		if r, err := s.GetRun(ctx, r3.ClusterID, r3.TaskID, r3.ID); err != nil {
			t.Fatal(err)
		} else if r.Status != runner.StatusStopped {
			t.Fatal("invalid status", r.Status)
		}
	})

	t.Run("get last started run nothing to return", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    runner.StatusError,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    runner.StatusError,
		}
		putRun(t, r1)

		_, err := s.GetLastStartedRun(ctx, clusterID, taskID)
		if err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})

	t.Run("get last started run return first", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    runner.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    runner.StatusStopped,
		}
		putRun(t, r1)

		r2 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    runner.StatusError,
		}
		putRun(t, r2)

		r, err := s.GetLastStartedRun(ctx, clusterID, taskID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r1, mermaidtest.UUIDComparer(), cmp.AllowUnexported(repair.Run{})); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get last started run return first with error", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    runner.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    runner.StatusError,
		}
		putRun(t, r1)
		putRunProgress(t, r1)

		r2 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    runner.StatusError,
		}
		putRun(t, r2)

		r, err := s.GetLastStartedRun(ctx, clusterID, taskID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r1, mermaidtest.UUIDComparer(), cmp.AllowUnexported(repair.Run{})); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("stop run", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    runner.StatusRunning,
		}

		putRun(t, r0)

		if err := s.StopRepair(ctx, clusterID, taskID, r0.ID); err != nil {
			t.Fatal(err)
		}

		if r1, err := s.GetRun(ctx, clusterID, taskID, r0.ID); err != nil {
			t.Fatal(err)
		} else if r1.Status != runner.StatusStopping {
			t.Fatal(r1.Status)
		}
	})
}
