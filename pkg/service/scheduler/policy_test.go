// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

//go:generate mockgen -destination mock_policy_test.go -mock_names Policy=mockPolicy -package scheduler github.com/scylladb/scylla-manager/v3/pkg/service/scheduler Policy
//go:generate mockgen -destination mock_runner_test.go -mock_names Runner=mockRunner -package scheduler github.com/scylladb/scylla-manager/v3/pkg/service/scheduler Runner

func TestPolicyRunner(t *testing.T) {
	t.Run("policy error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := uuid.MustRandom()
		k := uuid.MustRandom()
		r := uuid.MustRandom()
		tt := TaskType("")
		e := errors.New("test")

		mp := NewmockPolicy(ctrl)
		mp.EXPECT().PreRun(c, k, r, tt).Return(e)
		mr := NewmockRunner(ctrl)

		p := PolicyRunner{
			Policy:   mp,
			Runner:   mr,
			TaskType: tt,
		}
		if err := p.Run(context.Background(), c, k, r, nil); err != e {
			t.Fatal("expected", e, "got", err)
		}
	})

	t.Run("runner error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := uuid.MustRandom()
		k := uuid.MustRandom()
		r := uuid.MustRandom()
		tt := TaskType("")
		e := errors.New("test")

		mp := NewmockPolicy(ctrl)
		gomock.InOrder(
			mp.EXPECT().PreRun(c, k, r, tt).Return(nil),
			mp.EXPECT().PostRun(c, k, r, tt),
		)
		mr := NewmockRunner(ctrl)
		mr.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(e)

		p := PolicyRunner{
			Policy:   mp,
			Runner:   mr,
			TaskType: tt,
		}
		if err := p.Run(context.Background(), c, k, r, nil); err != e {
			t.Fatal("expected", e, "got", err)
		}
	})
}

func TestExclusiveTaskLockPolicy(t *testing.T) {
	clusterID := uuid.MustRandom()
	runID := uuid.MustRandom()
	taskID := uuid.MustRandom()

	t.Run("when no other task is running, preRun should return nil", func(t *testing.T) {
		restoreExclusiveTask := NewTaskExclusiveLockPolicy(RestoreTask)

		err := restoreExclusiveTask.PreRun(clusterID, taskID, runID, RestoreTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}
	})

	t.Run("when exclusive task is running, other tasks are not allowed", func(t *testing.T) {
		restoreExclusiveTask := NewTaskExclusiveLockPolicy(RestoreTask)

		err := restoreExclusiveTask.PreRun(clusterID, taskID, runID, RestoreTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}

		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, BackupTask)
		if !errors.Is(err, errClusterBusy) {
			t.Fatalf("PreRun: expected errClusterBusy, got: %v", err)
		}
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, RepairTask)
		if !errors.Is(err, errClusterBusy) {
			t.Fatalf("PreRun: expected errClusterBusy, got: %v", err)
		}
	})

	t.Run("when non exclusive task is running, exclusive task is not allowed", func(t *testing.T) {
		restoreExclusiveTask := NewTaskExclusiveLockPolicy(RestoreTask)

		err := restoreExclusiveTask.PreRun(clusterID, taskID, runID, BackupTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, RepairTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}

		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, RestoreTask)
		if !errors.Is(err, errClusterBusy) {
			t.Fatalf("PreRun: expected errClusterBusy, got: %v", err)
		}
	})

	t.Run("only one instance of a task type is allowed to run at a time", func(t *testing.T) {
		restoreExclusiveTask := NewTaskExclusiveLockPolicy(RestoreTask)

		err := restoreExclusiveTask.PreRun(clusterID, taskID, runID, RestoreTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, RestoreTask)
		if !errors.Is(err, errClusterBusy) {
			t.Fatalf("PreRun: expected errClusterBusy, got: %v", err)
		}

		restoreExclusiveTask = NewTaskExclusiveLockPolicy(RestoreTask)
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, BackupTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, BackupTask)
		if !errors.Is(err, errClusterBusy) {
			t.Fatalf("PreRun: expected errClusterBusy, got: %v", err)
		}
	})

	t.Run("when there are two exclusive task types", func(t *testing.T) {
		restoreExclusiveTask := NewTaskExclusiveLockPolicy(RestoreTask, One2OneRestoreTask)

		err := restoreExclusiveTask.PreRun(clusterID, taskID, runID, RestoreTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, One2OneRestoreTask)
		if !errors.Is(err, errClusterBusy) {
			t.Fatalf("PreRun: expected errClusterBusy, got: %v", err)
		}

		restoreExclusiveTask = NewTaskExclusiveLockPolicy(RestoreTask, One2OneRestoreTask)
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, BackupTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, One2OneRestoreTask)
		if !errors.Is(err, errClusterBusy) {
			t.Fatalf("PreRun: expected errClusterBusy, got: %v", err)
		}
	})

	t.Run("PostRun on a empty cluster", func(t *testing.T) {
		restoreExclusiveTask := NewTaskExclusiveLockPolicy(RestoreTask)

		restoreExclusiveTask.PostRun(clusterID, taskID, runID, RestoreTask)
	})

	t.Run("PostRun should release lock for a given task type", func(t *testing.T) {
		restoreExclusiveTask := NewTaskExclusiveLockPolicy(RestoreTask)

		err := restoreExclusiveTask.PreRun(clusterID, taskID, runID, RestoreTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, RestoreTask)
		if !errors.Is(err, errClusterBusy) {
			t.Fatalf("PreRun: expected errClusterBusy, got: %v", err)
		}

		// Release a lock and clean up the underlying map.
		restoreExclusiveTask.PostRun(clusterID, taskID, runID, RestoreTask)

		if _, ok := restoreExclusiveTask.running[clusterID]; ok {
			t.Fatalf("t.running[clusterID] should be deleted")
		}

		// Lock can be acquried again.
		err = restoreExclusiveTask.PreRun(clusterID, taskID, runID, RestoreTask)
		if err != nil {
			t.Fatalf("PreRun: unexpected err: %v", err)
		}
	})
}
