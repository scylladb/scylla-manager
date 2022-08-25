// Copyright (C) 2022 ScyllaDB

//go:build all || integration
// +build all integration

package backup_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/scylladb/go-log"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

/**
Universal restore design doc:
Requirements:
	- Works with any topology
	- Can restore a subset of tables selected with glob patterns
	- Can pause and resume
	- Provides progress information - to the monitoring stack
	- Provides dry run
	- Play well with Operator and Scylla Cloud Serverless (?)
	- Should we verify if restore overwrites rows (?)
Assumptions
	- Target cluster is up and running i.e. new cluster created with Scylla Cloud UI
	- Correct schema exists in the target cluster - Scylla Manager would not attempt to restore schema.


1) Wrong schema = error (?)
*/

func TestUniversalRestore(t *testing.T) {
	const (
		testBucket   = "backuptest-restore"
		testKeyspace = "backuptest_restore"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()
	secondClusterClientConfig := scyllaclient.TestConfig(testutils.ManagedSecondClusterHosts(), testutils.AgentAuthToken())

	var (
		session                          = testutils.CreateScyllaManagerDBSession(t)
		restoreDestinationClusterSession = testutils.CreateSessionAndDropAllKeyspaces(t, testutils.ManagedClusterHosts())
		backupClusterSession             = testutils.CreateSessionAndDropAllKeyspaces(t, testutils.ManagedSecondClusterHosts())
		h                                = newBackupTestHelper(t, session, config, location, nil)
		secondClusterH                   = newBackupTestHelper(t, session, config, location, &secondClusterClientConfig)
		ctx                              = context.Background()
	)

	testutils.WriteData(t, restoreDestinationClusterSession, testKeyspace, 0)
	testutils.WriteDataToSecondCluster(t, backupClusterSession, testKeyspace, 200)
	backupTarget := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []backupspec.Location{location},
		Retention: 3,
	}
	if err := secondClusterH.service.InitTarget(ctx, secondClusterH.clusterID, &backupTarget); err != nil {
		t.Fatal(err)
	}

	testutils.Print("When: run backup on cluster = (dc1: node1)")
	if err := secondClusterH.service.Backup(ctx, secondClusterH.clusterID, secondClusterH.taskID, secondClusterH.runID, backupTarget); err != nil {
		t.Fatal(err)
	}

	testutils.Print("Then: there are two backups")
	items, err := h.service.List(ctx, h.clusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: secondClusterH.clusterID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("List() = %v, expected one item", items)
	}
	i := items[0]
	fmt.Println(i.SnapshotInfo)

	//testutils.Print("When: create the same keyspace on the restore destination cluster")
	//testutils.ExecStmt(t, restoreDestinationClusterSession, "CREATE KEYSPACE IF NOT EXISTS "+testKeyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}")

	testutils.Print("When: restore backup on different cluster = (dc1: 3 nodes, dc2: 3 nodes)")
	location.DC = "dc1"
	restoreTarget := backup.RestoreTarget{
		Location:    []backupspec.Location{location},
		Keyspace:    []string{testKeyspace},
		BatchSize:   1,
		SnapshotTag: i.SnapshotInfo[0].SnapshotTag,
		Continue:    true,
		Parallel:    2,
	}
	fmt.Println(h.service.Restore(ctx, h.clusterID, h.taskID, h.runID, restoreTarget))

	pr1, err := h.service.GetRestoreProgress(ctx, h.clusterID, h.taskID, h.runID)
	fmt.Printf("\n\n %#+v \n\n", pr1)

	//tmp := uuid.MustRandom()
	//fmt.Println(h.service.Restore(ctx, h.clusterID, h.taskID, tmp, restoreTarget))
	//
	//pr2, err := h.service.GetRestoreProgress(ctx, h.clusterID, h.taskID, tmp)
	//fmt.Printf("\n\n %#+v \n\n", pr2)

	testutils.Print("When: repair executed on restored cluster")
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	s, err := repair.NewService(
		session,
		repair.DefaultConfig(),
		metrics.NewRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return h.client, nil
		},
		logger.Named("repair"),
	)
	if err := s.Repair(ctx, h.clusterID, uuid.MustRandom(), uuid.MustRandom(), repair.Target{
		Units: []repair.Unit{
			{Keyspace: testKeyspace, Tables: []string{"big_table"}},
		},
		DC:        []string{"dc1", "dc2"},
		Continue:  true,
		Intensity: 10,
	}); err != nil {
		t.Fatal(err)
	}

	var count string
	iter := restoreDestinationClusterSession.Query("SELECT COUNT(*) FROM "+testKeyspace+".big_table", []string{}).Iter()
	for iter.Scan(&count) {
		fmt.Println(count)
	}
}
