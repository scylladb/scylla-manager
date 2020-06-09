// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/schema/table"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/service/repair"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

func TestServiceGetLastResumableRunIntegration(t *testing.T) {
	session := CreateSession(t)

	s, err := repair.NewService(
		session,
		repair.DefaultConfig(),
		func(context.Context, uuid.UUID) (string, error) {
			return "", errors.New("not implemented")
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
		if err := table.RepairRun.InsertQuery(session).BindStruct(r).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}
	putRunProgress := func(t *testing.T, r *repair.Run, segmentCount, segmentSuccess, segmentError int) {
		p := repair.RunProgress{
			ClusterID:      r.ClusterID,
			TaskID:         r.TaskID,
			RunID:          r.ID,
			Host:           "172.16.1.3",
			Shard:          0,
			SegmentCount:   segmentCount,
			SegmentSuccess: segmentSuccess,
			SegmentError:   segmentError,
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
			Units:     []repair.Unit{{Keyspace: "test"}},
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []repair.Unit{{Keyspace: "test"}},
		}
		putRun(t, r1)

		_, err := s.GetLastResumableRun(ctx, clusterID, taskID)
		if err != service.ErrNotFound {
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
			Units:     []repair.Unit{{Keyspace: "test"}},
		}
		putRun(t, r0)
		putRunProgress(t, r0, 10, 5, 0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []repair.Unit{{Keyspace: "test"}},
		}
		putRun(t, r1)
		putRunProgress(t, r0, 10, 10, 0)

		_, err := s.GetLastResumableRun(ctx, clusterID, taskID)
		if err != service.ErrNotFound {
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
			Units:     []repair.Unit{{Keyspace: "test"}},
		}
		putRun(t, r0)
		putRunProgress(t, r0, 10, 5, 1)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []repair.Unit{{Keyspace: "test"}},
		}
		putRun(t, r1)

		r, err := s.GetLastResumableRun(ctx, clusterID, taskID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r0, UUIDComparer(), cmp.AllowUnexported(repair.Run{}), cmp.AllowUnexported(repair.Unit{})); diff != "" {
			t.Fatal(diff)
		}
	})
}
