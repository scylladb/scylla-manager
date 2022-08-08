// Copyright (C) 2022 ScyllaDB

//go:build all || integration
// +build all integration

package backup_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
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
	testutils.WriteDataToSecondCluster(t, backupClusterSession, testKeyspace, 100)
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

	//testutils.Print("When: create the same keyspace on the restore destination cluster")
	//testutils.ExecStmt(t, restoreDestinationClusterSession, "CREATE KEYSPACE IF NOT EXISTS "+testKeyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}")

	testutils.Print("When: restore backup on different cluster = (dc1: 3 nodes, dc2: 3 nodes)")
	location.DC = "dc1"
	restoreTarget := backup.RestoreTarget{
		Location:  []backupspec.Location{location},
		Keyspace:  []string{testKeyspace},
		BatchSize: 2,
	}
	fmt.Println(h.service.Restore(ctx, h.clusterID, h.taskID, h.runID, restoreTarget))

	var count string
	iter := restoreDestinationClusterSession.Query("SELECT COUNT(*) FROM "+testKeyspace+".big_table", []string{}).Iter()
	for iter.Scan(&count) {
		fmt.Println(count)
	}
}
