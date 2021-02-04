// Copyright (C) 2017 ScyllaDB

package repair

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func TestRepairUpdateMetrics(t *testing.T) {
	t.Parallel()

	clusterID := uuid.NewTime()
	taskID := uuid.NewTime()
	runID := uuid.NewTime()
	run := &Run{
		ID:          runID,
		ClusterID:   clusterID,
		TaskID:      taskID,
		clusterName: "my-cluster",
	}
	prog := Progress{
		progress: progress{
			TokenRanges: 160,
			Success:     68,
			Error:       0,
		},
		Hosts: []HostProgress{
			{Host: "h1", progress: progress{TokenRanges: 100, Success: 40}, Tables: []TableProgress{
				{Keyspace: "k1", Table: "t1", progress: progress{TokenRanges: 60, Success: 20}},
				{Keyspace: "k1", Table: "t2", progress: progress{TokenRanges: 40, Success: 20}},
			}},
			{Host: "h2", progress: progress{TokenRanges: 60, Success: 28}, Tables: []TableProgress{
				{Keyspace: "k1", Table: "t1", progress: progress{TokenRanges: 50, Success: 25}},
				{Keyspace: "k2", Table: "t2", progress: progress{TokenRanges: 10, Success: 3}},
			}},
		},
	}

	updateMetrics(run, prog)

	golden := fmt.Sprintf(`# HELP scylla_manager_repair_progress Current repair progress.
        # TYPE scylla_manager_repair_progress gauge
        scylla_manager_repair_progress{cluster="my-cluster",host="",keyspace="",task="%s"} 42
        scylla_manager_repair_progress{cluster="my-cluster",host="h1",keyspace="k1",task="%s"} 41
        scylla_manager_repair_progress{cluster="my-cluster",host="h2",keyspace="k1",task="%s"} 50
        scylla_manager_repair_progress{cluster="my-cluster",host="h2",keyspace="k2",task="%s"} 30
`, taskID, taskID, taskID, taskID)
	if err := testutil.CollectAndCompare(repairProgress, bytes.NewBufferString(golden)); err != nil {
		t.Fatal(err)
	}
}
