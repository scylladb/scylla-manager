// Copyright (C) 2022 ScyllaDB

//go:build all || integration
// +build all integration

package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
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
	- Shouldn't we verify if restore overwrites rows - it is user responsibility to truncate tables before restoring them
Assumptions
	- Target cluster is up and running i.e. new cluster created with Scylla Cloud UI
	- For restoring tables: Correct schema exists in the target cluster - Scylla Manager would not attempt to restore schema


1) Wrong schema = undefined behavior - user is responsible for using restore according to documentation

Empiric observations from running restore schema tests:
- Parallel must be set to 1 (otherwise strange issues might happen (e.g. cluster have to be restarted twice to pick up the schema))
- Destination cluster can have other keyspaces that were not included int the backup - they won't be affected
- If keyspace is already present in the destination cluster, it will be replaced with the one that's restored
  Not restored tables in this keyspace will be unconfigured
- If table is already present in the destination cluster, it will be changed to the one being restored

Observations from running restore tables tests:
- Restored table in the destination cluster can have additional columns - they will have zero values after restore
- Recently deleted rows from restored table won't be restored (delete information persists in table and is newer than restored data)

*/

type restoreTestHelper backupTestHelper

func newRestoreTestHelper(t *testing.T, session gocqlx.Session, config Config, location Location, clientConf *scyllaclient.Config) *restoreTestHelper {
	return (*restoreTestHelper)(newBackupTestHelper(t, session, config, location, clientConf))
}

func TestRestoreGetTargetIntegration(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		golden string
	}{
		{
			name:   "tables",
			input:  "testdata/restore/get_target/tables.input.json",
			golden: "testdata/restore/get_target/tables.golden.json",
		},
		{
			name:   "schema",
			input:  "testdata/restore/get_target/schema.input.json",
			golden: "testdata/restore/get_target/schema.golden.json",
		},
		{
			name:   "default values",
			input:  "testdata/restore/get_target/default_values.input.json",
			golden: "testdata/restore/get_target/default_values.golden.json",
		},
		{
			name:   "continue false",
			input:  "testdata/restore/get_target/continue_false.input.json",
			golden: "testdata/restore/get_target/continue_false.golden.json",
		},
	}

	const testBucket = "restoretest-get-target"

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newRestoreTestHelper(t, session, DefaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateSessionAndDropAllKeyspaces(t, ManagedClusterHosts()).Close()
	S3InitBucket(t, testBucket)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := os.ReadFile(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			v, err := h.service.GetRestoreTarget(ctx, h.clusterID, b)
			if err != nil {
				t.Fatal(err)
			}

			if UpdateGoldenFiles() {
				b, _ := json.Marshal(v)
				var buf bytes.Buffer
				json.Indent(&buf, b, "", "  ")
				if err := os.WriteFile(tc.golden, buf.Bytes(), 0666); err != nil {
					t.Error(err)
				}
			}

			b, err = os.ReadFile(tc.golden)
			if err != nil {
				t.Fatal(err)
			}
			var golden RestoreTarget
			if err := json.Unmarshal(b, &golden); err != nil {
				t.Error(err)
			}

			if diff := cmp.Diff(golden, v, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestRestoreGetTargetErrorIntegration(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "missing location",
			input: "testdata/restore/get_target/missing_location.input.json",
		},
		{
			name:  "duplicated locations",
			input: "testdata/restore/get_target/duplicated_locations.input.json",
		},
		{
			name:  "incorrect snapshot tag",
			input: "testdata/restore/get_target/incorrect_snapshot_tag.input.json",
		},
		{
			name:  "restore both types",
			input: "testdata/restore/get_target/restore_both_types.input.json",
		},
		{
			name:  "restore no type",
			input: "testdata/restore/get_target/restore_no_type.input.json",
		},
		{
			name:  "schema and keyspace param",
			input: "testdata/restore/get_target/schema_and_keyspace_param.input.json",
		},
		{
			name:  "schema and parallel param",
			input: "testdata/restore/get_target/schema_and_parallel_param.input.json",
		},
		{
			name:  "inaccessible bucket",
			input: "testdata/restore/get_target/inaccessible_bucket.input.json",
		},
		{
			name:  "non-positive parallel",
			input: "testdata/restore/get_target/non_positive_parallel.input.json",
		},
		{
			name:  "non-positive batch size",
			input: "testdata/restore/get_target/non_positive_batch_size.input.json",
		},
	}

	const testBucket = "restoretest-get-target-error"

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newRestoreTestHelper(t, session, DefaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateSessionAndDropAllKeyspaces(t, ManagedClusterHosts()).Close()
	S3InitBucket(t, testBucket)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := os.ReadFile(tc.input)
			if err != nil {
				t.Fatal(err)
			}

			_, err = h.service.GetRestoreTarget(ctx, h.clusterID, b)
			if err == nil {
				t.Fatal("GetRestoreTarget() expected error")
			}

			t.Log("GetRestoreTarget(): ", err)
		})
	}
}

func TestRestoreGetUnits(t *testing.T) {
	const (
		testBucket     = "restoretest-get-units"
		testKeyspace   = "restoretest_get_units"
		testBackupSize = 1
	)

	var (
		ctx            = context.Background()
		cfg            = DefaultConfig()
		mgrSession     = CreateScyllaManagerDBSession(t)
		clusterSession = CreateSessionAndDropAllKeyspaces(t, ManagedClusterHosts())
		loc            = Location{Provider: "s3", Path: testBucket}
		h              = newRestoreTestHelper(t, mgrSession, cfg, loc, nil)
	)

	WriteData(t, clusterSession, testKeyspace, testBackupSize)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{testKeyspace},
		SnapshotTag:   h.simpleBackup(loc, testKeyspace),
		RestoreTables: true,
	}

	units, err := h.service.GetRestoreUnits(ctx, h.clusterID, target)
	if err != nil {
		t.Fatal(err)
	}

	expected := []RestoreUnit{
		{
			Keyspace: testKeyspace,
			Tables: []RestoreTable{
				{
					Table: "big_table",
				},
			},
		},
	}

	if diff := cmp.Diff(units, expected, cmpopts.IgnoreFields(RestoreUnit{}, "Size"), cmpopts.IgnoreFields(RestoreTable{}, "Size")); diff != "" {
		t.Fatal(diff)
	}
}

func TestRestoreGetUnitsError(t *testing.T) {
	const (
		testBucket     = "restoretest-get-units-error"
		testKeyspace   = "restoretest_get_units_error"
		testBackupSize = 1
	)

	var (
		ctx            = context.Background()
		cfg            = DefaultConfig()
		mgrSession     = CreateScyllaManagerDBSession(t)
		clusterSession = CreateSessionAndDropAllKeyspaces(t, ManagedClusterHosts())
		loc            = Location{Provider: "s3", Path: testBucket}
		h              = newRestoreTestHelper(t, mgrSession, cfg, loc, nil)
	)

	WriteData(t, clusterSession, testKeyspace, testBackupSize)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{testKeyspace},
		SnapshotTag:   h.simpleBackup(loc, testKeyspace),
		RestoreTables: true,
	}

	t.Run("non-existent snapshot tag", func(t *testing.T) {
		target := target
		target.SnapshotTag = "sm_fake_snapshot_tagUTC"

		_, err := h.service.GetRestoreUnits(ctx, h.clusterID, target)
		if err == nil {
			t.Fatal("GetRestoreUnits() expected error")
		}
		t.Log("GetRestoreUnits(): ", err)
	})

	t.Run("no data matching keyspace pattern", func(t *testing.T) {
		target := target
		target.Keyspace = []string{"fake_keyspace"}

		_, err := h.service.GetRestoreUnits(ctx, h.clusterID, target)
		if err == nil {
			t.Fatal("GetRestoreUnits() expected error")
		}
		t.Log("GetRestoreUnits(): ", err)
	})
}

func TestRestoreTablesSmokeIntegration(t *testing.T) {
	const (
		testBucket    = "restoretest-tables-smoke"
		testKeyspace  = "restoretest_tables_smoke"
		testLoadCnt   = 5
		testLoadSize  = 5
		testBatchSize = 1
		testParallel  = 3
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{testKeyspace},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	smokeRestore(t, target, testKeyspace, testLoadCnt, testLoadSize)
}

func TestRestoreSchemaSmokeIntegration(t *testing.T) {
	const (
		testBucket    = "restoretest-schema-smoke"
		testKeyspace  = "restoretest_schema_smoke"
		testLoadCnt   = 1
		testLoadSize  = 1
		testBatchSize = 2
		testParallel  = 1 // Restoring schema can't be done in parallel
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{"system_schema"},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	smokeRestore(t, target, testKeyspace, testLoadCnt, testLoadSize)
}

func smokeRestore(t *testing.T, target RestoreTarget, keyspace string, loadCnt, loadSize int) {
	var (
		ctx          = context.Background()
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, ManagedClusterHosts())
		srcSession   = CreateSessionAndDropAllKeyspaces(t, ManagedSecondClusterHosts())
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg)
	)
	// Recreate schema on destination cluster
	if target.RestoreTables {
		WriteData(t, dstSession, keyspace, 0)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)

	target.SnapshotTag = srcH.simpleBackup(target.Location[0], keyspace)

	Print("When: restore backup on different cluster = (dc1: 3 nodes, dc2: 3 nodes)")
	if err := dstH.service.Restore(ctx, dstH.clusterID, dstH.taskID, dstH.runID, target); err != nil {
		t.Fatal(err)
	}

	dstH.validateRestoreSuccess(target, keyspace, loadCnt*loadSize, dstSession)
}

func TestRestoreTablesNodeDownIntegration(t *testing.T) {
	const (
		testBucket    = "restoretest-tables-node-down"
		testKeyspace  = "restoretest_tables_node_down"
		testLoadCnt   = 3
		testLoadSize  = 1
		testBatchSize = 1
		testParallel  = 3
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{testKeyspace},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	restoreWithNodeDown(t, target, testKeyspace, testLoadCnt, testLoadSize)
}

func TestRestoreSchemaNodeDownIntegration(t *testing.T) {
	const (
		testBucket    = "restoretest-schema-node-down"
		testKeyspace  = "restoretest_schema_node_down"
		testLoadCnt   = 1
		testLoadSize  = 1
		testBatchSize = 2
		testParallel  = 1
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{"system_schema"},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	restoreWithNodeDown(t, target, testKeyspace, testLoadCnt, testLoadSize)
}

func restoreWithNodeDown(t *testing.T, target RestoreTarget, keyspace string, loadCnt, loadSize int) {
	Print("Given: downed node")
	if stdout, stderr, err := ExecOnHost("192.168.100.11", CmdBlockScyllaREST); err != nil {
		t.Fatal(err, stdout, stderr)
	}
	// Just in case test fails before unblocking node
	defer ExecOnHost("192.168.100.11", CmdUnblockScyllaREST)

	var (
		ctx          = context.Background()
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, ManagedClusterHosts())
		srcSession   = CreateSessionAndDropAllKeyspaces(t, ManagedSecondClusterHosts())
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg)
	)
	// Recreate schema on destination cluster
	if target.RestoreTables {
		WriteData(t, dstSession, keyspace, 0)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)

	target.SnapshotTag = srcH.simpleBackup(target.Location[0], keyspace)

	Print("When: restore backup on different cluster = (dc1: 3 nodes, dc2: 3 nodes)")
	if err := dstH.service.Restore(ctx, dstH.clusterID, dstH.taskID, dstH.runID, target); err != nil {
		t.Fatal(err)
	}

	if stdout, stderr, err := ExecOnHost("192.168.100.11", CmdUnblockScyllaREST); err != nil {
		t.Fatal(err, stdout, stderr)
	}

	dstH.validateRestoreSuccess(target, keyspace, loadCnt*loadSize, dstSession)
}

func TestRestoreTablesRestartAgentsIntegration(t *testing.T) {
	const (
		testBucket    = "restoretest-tables-restart-agent"
		testKeyspace  = "restoretest_tables_restart_agent"
		testLoadCnt   = 3
		testLoadSize  = 1
		testBatchSize = 1
		testParallel  = 2
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{testKeyspace},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	restoreWithAgentRestart(t, target, testKeyspace, testLoadCnt, testLoadSize)
}

func TestRestoreSchemaRestartAgentsIntegration(t *testing.T) {
	const (
		testBucket    = "restoretest-schema-restart-agents"
		testKeyspace  = "restoretest_schema_restart_agents"
		testLoadCnt   = 1
		testLoadSize  = 1
		testBatchSize = 2
		testParallel  = 1
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{"system_schema"},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	restoreWithAgentRestart(t, target, testKeyspace, testLoadCnt, testLoadSize)
}

func restoreWithAgentRestart(t *testing.T, target RestoreTarget, keyspace string, loadCnt, loadSize int) {
	var (
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, ManagedClusterHosts())
		srcSession   = CreateSessionAndDropAllKeyspaces(t, ManagedSecondClusterHosts())
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg)
		ctx          = context.Background()
	)
	// Recreate schema on destination cluster
	if target.RestoreTables {
		WriteData(t, dstSession, keyspace, 0)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)

	target.SnapshotTag = srcH.simpleBackup(target.Location[0], keyspace)

	a := atomic.NewInt64(0)
	dstH.hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.Path, "/agent/rclone/sync/copypaths") && a.Inc() == 1 {
			Print("And: agents are restarted")
			restartAgents(t)
		}
		return nil, nil
	}))

	Print("When: Restore is running")
	if err := dstH.service.Restore(ctx, dstH.clusterID, dstH.taskID, dstH.runID, target); err != nil {
		t.Errorf("Expected no error but got %+v", err)
	}

	dstH.validateRestoreSuccess(target, keyspace, loadCnt*loadSize, dstSession)
}

func TestRestoreTablesResumeIntegration(t *testing.T) {
	const (
		testBucket    = "restoretest-tables-resume"
		testKeyspace  = "restoretest_tables_resume"
		testLoadCnt   = 5
		testLoadSize  = 2
		testBatchSize = 1
		testParallel  = 3
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{testKeyspace},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
		Continue:      true,
	}

	restoreWithResume(t, target, testKeyspace, testLoadCnt, testLoadSize)
}

func TestRestoreTablesResumeContinueFalseIntegration(t *testing.T) {
	const (
		testBucket    = "restoretest-tables-resume-continue-false"
		testKeyspace  = "restoretest_tables_resume_continue_false"
		testLoadCnt   = 5
		testLoadSize  = 2
		testBatchSize = 1
		testParallel  = 3
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		Keyspace:      []string{testKeyspace},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	restoreWithResume(t, target, testKeyspace, testLoadCnt, testLoadSize)
}

func restoreWithResume(t *testing.T, target RestoreTarget, keyspace string, loadCnt, loadSize int) {
	var (
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, ManagedClusterHosts())
		srcSession   = CreateSessionAndDropAllKeyspaces(t, ManagedSecondClusterHosts())
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg)
		ctx, cancel  = context.WithCancel(context.Background())
	)
	// Recreate schema on destination cluster
	if target.RestoreTables {
		WriteData(t, dstSession, keyspace, 0)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)

	target.SnapshotTag = srcH.simpleBackup(target.Location[0], keyspace)

	a := atomic.NewInt64(0)
	dstH.hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.Path, "/storage_service/sstables/") && a.Inc() == 1 {
			Print("And: context is canceled")
			cancel()
		}
		return nil, nil
	}))

	Print("When: Restore is running")
	err := dstH.service.Restore(ctx, dstH.clusterID, dstH.taskID, dstH.runID, target)
	if err == nil {
		t.Error("Expected error on run but got nil")
		return
	}
	if !strings.Contains(err.Error(), "context") {
		t.Errorf("Expected context error but got: %+v", err)
	}

	pr, err := dstH.service.GetRestoreProgress(context.Background(), dstH.clusterID, dstH.taskID, dstH.runID)
	if err != nil {
		t.Fatal(err)
	}
	Printf("And: restore progress: %+#v\n", pr)
	if pr.Downloaded == 0 {
		t.Fatal("Expected partial restore")
	}

	Print("When: restore is resumed with new RunID")
	dstH.runID = uuid.MustRandom()
	err = dstH.service.Restore(context.Background(), dstH.clusterID, dstH.taskID, dstH.runID, target)
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	Print("Then: data is restored")
	dstH.validateRestoreSuccess(target, keyspace, loadCnt*loadSize, dstSession)
}

func (h *restoreTestHelper) validateRestoreSuccess(target RestoreTarget, keyspace string, backupSize int, dstSession gocqlx.Session) {
	h.t.Helper()
	Print("Then: validate restore result")

	pr, err := h.service.GetRestoreProgress(context.Background(), h.clusterID, h.taskID, h.runID)
	if err != nil {
		h.t.Fatal(err)
	}
	Printf("And: restore progress: %+#v\n", pr)
	if pr.Size != pr.Restored || pr.Size != pr.Downloaded {
		h.t.Fatal("Expected complete restore")
	}
	for _, kpr := range pr.Keyspaces {
		if kpr.Size != kpr.Restored || kpr.Size != kpr.Downloaded {
			h.t.Fatalf("Expected complete keyspace restore (%s)", kpr.Keyspace)
		}
		for _, tpr := range kpr.Tables {
			if tpr.Size != tpr.Restored || tpr.Size != tpr.Downloaded {
				h.t.Fatalf("Expected complete table restore (%s)", tpr.Table)
			}
		}
	}

	var expectedRows int
	switch {
	case target.RestoreSchema:
		h.restartScylla()
	case target.RestoreTables:
		h.simpleRepair(keyspace)
		expectedRows = backupSize * 256
	}

	Print("When: query contents of restored table")
	var count int
	q := dstSession.Query("SELECT COUNT(*) FROM "+keyspace+".big_table", nil)
	if err := q.Get(&count); err != nil {
		h.t.Fatal(err)
	}
	Print(fmt.Sprintf("Then: %d rows present after restore", count))

	if count != expectedRows {
		h.t.Fatalf("COUNT(*) = %d, expected %d rows", count, expectedRows)
	}
}

// prepareRestoreBackup populates second cluster with loadCnt * loadSize MiB of data living in keyspace.big_table table.
// It writes loadCnt batches (each containing loadSize Mib of data), flushing inserted data into SSTables after each.
// It also disables compaction for keyspace.big_table so that SSTables flushed into memory are not merged together.
// This way we can efficiently test restore procedure without the need to produce big backups
// (restore functionality depends more on the amount of restored SSTables rather than on their total size).
func (h *restoreTestHelper) prepareRestoreBackup(session gocqlx.Session, keyspace string, loadCnt, loadSize int) {
	ctx := context.Background()

	// Create keyspace and table.
	WriteDataToSecondCluster(h.t, session, keyspace, 0, 0)

	if err := h.client.DisableAutoCompaction(ctx, keyspace, "big_table"); err != nil {
		h.t.Fatal(err)
	}

	var startingID int
	for i := 0; i < loadCnt; i++ {
		Printf("When: Write load nr %d to second cluster", i)

		startingID = WriteDataToSecondCluster(h.t, session, keyspace, startingID, loadSize)
		if err := h.client.FlushTable(ctx, keyspace, "big_table"); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *restoreTestHelper) simpleBackup(location Location, keyspace string) string {
	h.t.Helper()
	Print("When: backup cluster = (dc1: node1)")

	ctx := context.Background()
	backupTarget := Target{
		Units: []Unit{
			{
				Keyspace: keyspace,
			},
			{
				Keyspace: "system_schema",
			},
		},
		DC:        []string{"dc1"},
		Location:  []Location{location},
		Retention: 3,
	}

	if err := h.service.InitTarget(ctx, h.clusterID, &backupTarget); err != nil {
		h.t.Fatal(err)
	}

	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, backupTarget); err != nil {
		h.t.Fatal(err)
	}
	Print("Then: cluster is backed-up")

	Print("When: list backup")
	items, err := h.service.List(ctx, h.clusterID, []Location{location}, ListFilter{})
	if err != nil {
		h.t.Fatal(err)
	}
	if len(items) != 1 {
		h.t.Fatalf("List() = %v, expected one item", items)
	}
	i := items[0]
	Print(fmt.Sprintf("Then: backup snapshot info: %v", i.SnapshotInfo))

	return i.SnapshotInfo[0].SnapshotTag
}

func (h *restoreTestHelper) simpleRepair(keyspace string) {
	h.t.Helper()
	Print("When: repair restored cluster")

	s, err := repair.NewService(
		h.session,
		repair.DefaultConfig(),
		metrics.NewRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return h.client, nil
		},
		log.NewDevelopmentWithLevel(zapcore.ErrorLevel).Named("repair"),
	)
	if err != nil {
		h.t.Fatal(err)
	}

	ctx := context.Background()

	if err = s.Repair(ctx, h.clusterID, uuid.MustRandom(), uuid.MustRandom(), repair.Target{
		Units: []repair.Unit{
			{
				Keyspace: keyspace,
				Tables:   []string{"big_table"},
			},
		},
		DC:        []string{"dc1", "dc2"},
		Continue:  true,
		Intensity: 10,
	}); err != nil {
		h.t.Fatal(err)
	}

	Print("Then: cluster is repaired")
}

func (h *restoreTestHelper) restartScylla() {
	h.t.Helper()
	Print("When: restart cluster")

	ctx := context.Background()
	cfg := cqlping.Config{Timeout: 100 * time.Millisecond}
	const cmdRestart = "supervisorctl restart scylla"

	for _, host := range ManagedClusterHosts() {
		Print("When: restart Scylla on host: " + host)
		stdout, stderr, err := ExecOnHost(host, cmdRestart)
		if err != nil {
			h.t.Log("stdout", stdout)
			h.t.Log("stderr", stderr)
			h.t.Fatal("Command failed on host", host, err)
		}

		cfg.Addr = net.JoinHostPort(host, "9042")
		cond := func() bool {
			_, err = cqlping.NativeCQLPing(ctx, cfg)
			return err == nil
		}

		WaitCond(h.t, cond, time.Second, 60*time.Second)
		Print("Then: Scylla is restarted on host: " + host)
	}

	Print("Then: cluster is restarted")
}
