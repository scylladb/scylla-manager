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
	"github.com/scylladb/mermaid/uuid"
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
	}
	p1 := &RunProgress{
		RunID:     runID,
		ClusterID: clusterID,
		TaskID:    taskID,
		Host:      "host",
		Unit:      0,
		TableName: "table1",
		FileName:  "file1",
		Size:      100,
	}
	p2 := &RunProgress{
		RunID:     runID,
		ClusterID: clusterID,
		TaskID:    taskID,
		Host:      "host",
		Unit:      0,
		TableName: "table2",
		FileName:  "file1",
		Size:      100,
	}

	visitor := &testMetricsVisitor{prog: []*RunProgress{p1, p2}}

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	stop := newBackupMetricUpdater(context.Background(), run, visitor, logger, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond) // Wait for two cycles before updating progress
	visitor.SetUploaded(0, 50)
	stop()

	expected := fmt.Sprintf(`# HELP scylla_manager_backup_bytes_left Number of bytes left for backup completion.
        # TYPE scylla_manager_backup_bytes_left gauge
		scylla_manager_backup_bytes_left{cluster="%s",host="",keyspace="",task="%s"} %d
        scylla_manager_backup_bytes_left{cluster="%s",host="host",keyspace="",task="%s"} %d
        scylla_manager_backup_bytes_left{cluster="%s",host="host",keyspace="keyspace",task="%s"} %d
`, clusterID, taskID, p1.Size-p1.Uploaded, clusterID, taskID, p1.Size-p1.Uploaded, clusterID, taskID, p1.Size-p1.Uploaded)
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
