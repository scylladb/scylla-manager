// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

func TestBackupMetricUpdater(t *testing.T) {
	t.Parallel()

	clusterID := uuid.NewTime()
	taskID := uuid.NewTime()
	runID := uuid.NewTime()
	run := &Run{
		ID:        runID,
		ClusterID: clusterID,
		TaskID:    taskID,
		Units: []Unit{
			{Keyspace: "keyspace", Tables: []string{"table1"}},
		},
		clusterName: "my-cluster",
	}
	p1 := &RunProgress{
		RunID:     runID,
		ClusterID: clusterID,
		TaskID:    taskID,
		Host:      "host",
		Unit:      0,
		TableName: "table1",
		Size:      100,
	}
	p2 := &RunProgress{
		RunID:     runID,
		ClusterID: clusterID,
		TaskID:    taskID,
		Host:      "host",
		Unit:      0,
		TableName: "table2",
		Size:      100,
	}

	visitor := &testMetricsVisitor{prog: []*RunProgress{p1, p2}}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				run.Stage = StageIndex
			}
		}
	}()

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	stop := newBackupMetricUpdater(context.Background(), func(ctx context.Context) (*Run, Progress, error) {
		run := &Run{
			ID:        runID,
			ClusterID: clusterID,
			TaskID:    taskID,
			Units: []Unit{
				{Keyspace: "keyspace", Tables: []string{"table1"}},
			},
			clusterName: "my-cluster",
			Stage:       StageInit,
		}
		pr, err := aggregateProgress(run, visitor)
		return run, pr, err
	}, logger, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond) // Wait for ten cycles before updating progress
	visitor.SetUploaded(0, 50)
	stop()
	close(done)

	expected := fmt.Sprintf(`# HELP scylla_manager_backup_bytes_left Number of bytes left for backup completion.
        # TYPE scylla_manager_backup_bytes_left gauge
		scylla_manager_backup_bytes_left{cluster="%s",host="",keyspace="",task="%s"} %d
        scylla_manager_backup_bytes_left{cluster="%s",host="host",keyspace="",task="%s"} %d
        scylla_manager_backup_bytes_left{cluster="%s",host="host",keyspace="keyspace",task="%s"} %d
`, run.clusterName, taskID, p1.Size-p1.Uploaded, run.clusterName, taskID, p1.Size-p1.Uploaded, run.clusterName, taskID, p1.Size-p1.Uploaded)
	if err := testutil.CollectAndCompare(backupBytesLeft, bytes.NewBufferString(expected)); err != nil {
		t.Fatal(err)
	}
}

type testMetricsVisitor struct {
	mu   sync.Mutex
	prog []*RunProgress
}

func (v *testMetricsVisitor) SetUploaded(index int, uploaded int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.prog[index].Uploaded = uploaded
}

func (v *testMetricsVisitor) ForEach(visit func(*RunProgress)) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, pr := range v.prog {
		visit(pr)
	}
	return nil
}
