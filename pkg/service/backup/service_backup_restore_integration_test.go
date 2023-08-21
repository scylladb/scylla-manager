// Copyright (C) 2022 ScyllaDB

//go:build all || integration
// +build all integration

package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"go.uber.org/atomic"

	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
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
- Destination cluster can have other keyspaces that were not included int the backup - they won't be affected
- If keyspace is already present in the destination cluster, it will be replaced with the one that's restored
  Not restored tables in this keyspace will be unconfigured
- If table is already present in the destination cluster, it will be changed to the one being restored

Observations from running restore tables tests:
- Restored table in the destination cluster can have additional columns - they will have zero values after restore
- Recently deleted rows from restored table won't be restored (delete information persists in table and is newer than restored data)

*/

type restoreTestHelper backupTestHelper

// newRestoreTestHelper creates backupTestHelper with cql superuser.
func newRestoreTestHelper(t *testing.T, session gocqlx.Session, config Config, location Location, clientConf *scyllaclient.Config) *restoreTestHelper {
	return (*restoreTestHelper)(newBackupTestHelper(t, session, config, location, clientConf))
}

// newDstRestoreTestHelperWithUser creates backupTestHelper with given cql user.
func newRestoreTestHelperWithUser(t *testing.T, session gocqlx.Session, config Config, location Location, clientConf *scyllaclient.Config, user, pass string) *restoreTestHelper {
	return (*restoreTestHelper)(newBackupTestHelperWithUser(t, session, config, location, clientConf, user, pass))
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

	testBucket, _, _ := getBucketKeyspaceUser(t)

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newRestoreTestHelper(t, session, DefaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateSessionAndDropAllKeyspaces(t, h.Client).Close()
	S3InitBucket(t, testBucket)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := os.ReadFile(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			v, err := h.service.GetRestoreTarget(ctx, h.ClusterID, b)
			if err != nil {
				t.Fatal(err)
			}

			if UpdateGoldenFiles() {
				b, _ := json.Marshal(v)
				var buf bytes.Buffer
				json.Indent(&buf, b, "", "  ")
				if err := os.WriteFile(tc.golden, buf.Bytes(), 0666); err != nil {
					t.Fatal(err)
				}
			}

			b, err = os.ReadFile(tc.golden)
			if err != nil {
				t.Fatal(err)
			}
			var golden RestoreTarget
			if err := json.Unmarshal(b, &golden); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(golden, v, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
				t.Fatal(tc.golden, diff)
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

	testBucket, _, _ := getBucketKeyspaceUser(t)

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newRestoreTestHelper(t, session, DefaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateSessionAndDropAllKeyspaces(t, h.Client).Close()
	S3InitBucket(t, testBucket)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := os.ReadFile(tc.input)
			if err != nil {
				t.Fatal(err)
			}

			_, err = h.service.GetRestoreTarget(ctx, h.ClusterID, b)
			if err == nil {
				t.Fatal("GetRestoreTarget() expected error")
			}

			t.Log("GetRestoreTarget(): ", err)
		})
	}
}

func TestRestoreGetUnitsIntegration(t *testing.T) {
	testBucket, testKeyspace, _ := getBucketKeyspaceUser(t)
	const (
		testBackupSize = 1
	)

	var (
		ctx            = context.Background()
		cfg            = DefaultConfig()
		mgrSession     = CreateScyllaManagerDBSession(t)
		loc            = Location{Provider: "s3", Path: testBucket}
		h              = newRestoreTestHelper(t, mgrSession, cfg, loc, nil)
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
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
		SnapshotTag:   h.simpleBackup(loc),
		RestoreTables: true,
	}

	units, err := h.service.GetRestoreUnits(ctx, h.ClusterID, target)
	if err != nil {
		t.Fatal(err)
	}

	expected := []RestoreUnit{
		{
			Keyspace: testKeyspace,
			Tables: []RestoreTable{
				{
					Table:       BigTableName,
					TombstoneGC: "timeout",
				},
			},
		},
	}

	if diff := cmp.Diff(units, expected, cmpopts.IgnoreFields(RestoreUnit{}, "Size"), cmpopts.IgnoreFields(RestoreTable{}, "Size")); diff != "" {
		t.Fatal(diff)
	}
}

func TestRestoreGetUnitsErrorIntegration(t *testing.T) {
	testBucket, testKeyspace, _ := getBucketKeyspaceUser(t)
	const (
		testBackupSize = 1
	)

	var (
		ctx            = context.Background()
		cfg            = DefaultConfig()
		mgrSession     = CreateScyllaManagerDBSession(t)
		loc            = Location{Provider: "s3", Path: testBucket}
		h              = newRestoreTestHelper(t, mgrSession, cfg, loc, nil)
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
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
		SnapshotTag:   h.simpleBackup(loc),
		RestoreTables: true,
	}

	t.Run("non-existent snapshot tag", func(t *testing.T) {
		target := target
		target.SnapshotTag = "sm_fake_snapshot_tagUTC"

		_, err := h.service.GetRestoreUnits(ctx, h.ClusterID, target)
		if err == nil {
			t.Fatal("GetRestoreUnits() expected error")
		}
		t.Log("GetRestoreUnits(): ", err)
	})

	t.Run("no data matching keyspace pattern", func(t *testing.T) {
		target := target
		target.Keyspace = []string{"fake_keyspace"}

		_, err := h.service.GetRestoreUnits(ctx, h.ClusterID, target)
		if err == nil {
			t.Fatal("GetRestoreUnits() expected error")
		}
		t.Log("GetRestoreUnits(): ", err)
	})
}

func TestRestoreTablesSmokeIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 5
		testLoadSize  = 5
		testBatchSize = 1
		testParallel  = 0
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

	smokeRestore(t, target, testKeyspace, testLoadCnt, testLoadSize, testUser, "{'class': 'NetworkTopologyStrategy', 'dc1': 2}")
}

func TestRestoreTablesSmokeNoReplicationIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 2
		testLoadSize  = 2
		testBatchSize = 1
		testParallel  = 0
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

	smokeRestore(t, target, testKeyspace, testLoadCnt, testLoadSize, testUser, "{'class': 'SimpleStrategy', 'replication_factor': 1}")
}

func TestRestoreSchemaSmokeIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 1
		testLoadSize  = 1
		testBatchSize = 2
		testParallel  = 0
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	smokeRestore(t, target, testKeyspace, testLoadCnt, testLoadSize, testUser, "{'class': 'NetworkTopologyStrategy', 'dc1': 2}")
}

func smokeRestore(t *testing.T, target RestoreTarget, keyspace string, loadCnt, loadSize int, user, replication string) {
	var (
		ctx          = context.Background()
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
	)

	// Restore should be performed on user with limited permissions
	if err := createUser(dstSession, user, "pass"); err != nil {
		t.Fatal(err)
	}
	dstH = newRestoreTestHelperWithUser(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	// Recreate schema on destination cluster
	if target.RestoreTables {
		RawWriteData(t, dstSession, keyspace, 0, 0, replication, false)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	target.SnapshotTag = srcH.simpleBackup(target.Location[0])
	target = dstH.regenerateRestoreTarget(target)

	if err := grantPermissionsToUser(dstSession, target, user); err != nil {
		t.Fatal(err)
	}

	Print("When: restore backup on different cluster = (dc1: 3 nodes, dc2: 3 nodes)")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, target); err != nil {
		t.Fatal(err)
	}

	toValidate := []validateTable{
		{Keyspace: keyspace, Table: BigTableName, Column: "id"},
	}
	dstH.validateRestoreSuccess(dstSession, srcSession, target, toValidate)
}

func TestRestoreTablesRestartAgentsIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
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

	restoreWithAgentRestart(t, target, testKeyspace, testLoadCnt, testLoadSize, testUser)
}

func restoreWithAgentRestart(t *testing.T, target RestoreTarget, keyspace string, loadCnt, loadSize int, user string) {
	var (
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
		ctx          = context.Background()
	)

	// Restore should be performed on user with limited permissions
	if err := createUser(dstSession, user, "pass"); err != nil {
		t.Fatal(err)
	}
	dstH = newRestoreTestHelperWithUser(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	// Recreate schema on destination cluster
	if target.RestoreTables {
		WriteDataSecondClusterSchema(t, dstSession, keyspace, 0, 0)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	target.SnapshotTag = srcH.simpleBackup(target.Location[0])
	target = dstH.regenerateRestoreTarget(target)

	if err := grantPermissionsToUser(dstSession, target, user); err != nil {
		t.Fatal(err)
	}

	a := atomic.NewInt64(0)
	dstH.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.Path, "/agent/rclone/sync/copypaths") && a.Inc() == 1 {
			Print("And: agents are restarted")
			restartAgents(dstH.CommonTestHelper)
		}
		return nil, nil
	}))

	Print("When: Restore is running")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, target); err != nil {
		t.Errorf("Expected no error but got %+v", err)
	}

	toValidate := []validateTable{
		{Keyspace: keyspace, Table: BigTableName, Column: "id"},
	}
	dstH.validateRestoreSuccess(dstSession, srcSession, target, toValidate)
}

func TestRestoreTablesResumeIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
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

	restoreWithResume(t, target, testKeyspace, testLoadCnt, testLoadSize, testUser)
}

func TestRestoreTablesResumeContinueFalseIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
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

	restoreWithResume(t, target, testKeyspace, testLoadCnt, testLoadSize, testUser)
}

func restoreWithResume(t *testing.T, target RestoreTarget, keyspace string, loadCnt, loadSize int, user string) {
	var (
		cfg           = DefaultConfig()
		srcClientCfg  = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession    = CreateScyllaManagerDBSession(t)
		dstH          = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil)
		srcH          = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg)
		dstSession    = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession    = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
		ctx1, cancel1 = context.WithCancel(context.Background())
		ctx2, cancel2 = context.WithCancel(context.Background())
	)

	// Restore should be performed on user with limited permissions
	if err := createUser(dstSession, user, "pass"); err != nil {
		t.Fatal(err)
	}
	dstH = newRestoreTestHelperWithUser(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	// Recreate schema on destination cluster
	if target.RestoreTables {
		WriteDataSecondClusterSchema(t, dstSession, keyspace, 0, 0)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	target.SnapshotTag = srcH.simpleBackup(target.Location[0])
	target = dstH.regenerateRestoreTarget(target)

	if err := grantPermissionsToUser(dstSession, target, user); err != nil {
		t.Fatal(err)
	}

	a := atomic.NewInt64(0)
	dstH.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.Path, "/storage_service/sstables/") && a.Inc() == 1 {
			Print("And: context1 is canceled")
			cancel1()
		}
		return nil, nil
	}))

	b := atomic.NewInt64(0)
	dstH.Hrt.SetRespNotifier(func(resp *http.Response, err error) {
		if resp == nil {
			return
		}

		var copiedBody bytes.Buffer
		tee := io.TeeReader(resp.Body, &copiedBody)
		body, _ := io.ReadAll(tee)
		resp.Body = io.NopCloser(&copiedBody)

		// Response to repair status
		if resp.Request.URL.Path == "/storage_service/repair_status" && resp.Request.Method == http.MethodGet && resp.Request.URL.Query()["id"][0] != "-1" {
			status := string(body)
			if status == "\"SUCCESSFUL\"" && b.Inc() == 1 {
				Print("And: context2 is canceled")
				cancel2()
			}
		}
	})

	Print("When: run restore and stop it during load and stream")
	err := dstH.service.Restore(ctx1, dstH.ClusterID, dstH.TaskID, dstH.RunID, target)
	if err == nil {
		t.Fatal("Expected error on run but got nil")
		return
	}
	if !strings.Contains(err.Error(), "context") {
		t.Fatalf("Expected context error but got: %+v", err)
	}

	pr, err := dstH.service.GetRestoreProgress(context.Background(), dstH.ClusterID, dstH.TaskID, dstH.RunID)
	if err != nil {
		t.Fatal(err)
	}
	Printf("And: restore progress: %+#v\n", pr)
	if pr.Downloaded == 0 {
		t.Fatal("Expected partial restore progress")
	}

	Print("When: resume restore and stop in during repair")
	dstH.RunID = uuid.MustRandom()
	err = dstH.service.Restore(ctx2, dstH.ClusterID, dstH.TaskID, dstH.RunID, target)
	if err == nil {
		t.Fatal("Expected error on run but got nil")
		return
	}
	if !strings.Contains(err.Error(), "context") {
		t.Fatalf("Expected context error but got: %+v", err)
	}

	pr, err = dstH.service.GetRestoreProgress(context.Background(), dstH.ClusterID, dstH.TaskID, dstH.RunID)
	if err != nil {
		t.Fatal(err)
	}
	Printf("And: restore progress: %+#v\n", pr)
	if pr.RepairProgress == nil || pr.RepairProgress.Success == 0 {
		t.Fatal("Expected partial repair progress")
	}

	Print("When: resume restore and complete it")
	dstH.RunID = uuid.MustRandom()
	err = dstH.service.Restore(context.Background(), dstH.ClusterID, dstH.TaskID, dstH.RunID, target)
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	Print("Then: data is restored")
	toValidate := []validateTable{
		{Keyspace: keyspace, Table: BigTableName, Column: "id"},
	}
	dstH.validateRestoreSuccess(dstSession, srcSession, target, toValidate)
}

func TestRestoreTablesVersionedIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 2
		testLoadSize  = 1
		testBatchSize = 1
		testParallel  = 3
		corruptCnt    = 3
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

	restoreWithVersions(t, target, testKeyspace, testLoadCnt, testLoadSize, corruptCnt, testUser)
}

func TestRestoreSchemaVersionedIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 2
		testLoadSize  = 1
		testBatchSize = 1
		testParallel  = 3
		corruptCnt    = 3
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	restoreWithVersions(t, target, testKeyspace, testLoadCnt, testLoadSize, corruptCnt, testUser)
}

func restoreWithVersions(t *testing.T, target RestoreTarget, keyspace string, loadCnt, loadSize, corruptCnt int, user string) {
	var (
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
		ctx          = context.Background()
	)

	// Restore should be performed on user with limited permissions
	if err := createUser(dstSession, user, "pass"); err != nil {
		t.Fatal(err)
	}
	dstH = newRestoreTestHelperWithUser(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	Print("Recreate schema on destination cluster")
	if target.RestoreTables {
		WriteDataSecondClusterSchema(t, dstSession, keyspace, 0, 0)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	srcH.simpleBackup(target.Location[0])

	// Corrupting SSTables allows us to force the creation of versioned files
	Print("Choose SSTables to corrupt")
	status, err := srcH.Client.Status(ctx)
	if err != nil {
		t.Fatal("Get status")
	}

	host := status[0]
	var corruptedKeyspace string
	var corruptedTable string
	if target.RestoreTables {
		corruptedKeyspace = keyspace
		corruptedTable = BigTableName
	} else {
		corruptedKeyspace = "system_schema"
		corruptedTable = "keyspaces"
	}

	remoteDir := target.Location[0].RemotePath(RemoteSSTableDir(srcH.ClusterID, host.Datacenter, host.HostID, corruptedKeyspace, corruptedTable))
	opts := &scyllaclient.RcloneListDirOpts{
		Recurse:   true,
		FilesOnly: true,
	}

	var (
		firstCorrupt  []string
		bothCorrupt   []string
		secondCorrupt []string
	)

	err = srcH.Client.RcloneListDirIter(ctx, host.Addr, remoteDir, opts, func(item *scyllaclient.RcloneListDirItem) {
		if _, err = VersionedFileCreationTime(item.Name); err == nil {
			t.Fatalf("Versioned file %s present after first backup", path.Join(remoteDir, item.Path))
		}
		if strings.HasSuffix(item.Name, ".db") {
			switch {
			case len(firstCorrupt) < corruptCnt:
				firstCorrupt = append(firstCorrupt, item.Path)
			case len(bothCorrupt) < corruptCnt:
				bothCorrupt = append(bothCorrupt, item.Path)
			case len(secondCorrupt) < corruptCnt:
				secondCorrupt = append(secondCorrupt, item.Path)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	Printf("First group of corrupted SSTables: %v", firstCorrupt)
	Printf("Common group of corrupted SSTables: %v", bothCorrupt)
	Printf("Second group of corrupted SSTables: %v", secondCorrupt)

	totalFirstCorrupt := append([]string{}, firstCorrupt...)
	totalFirstCorrupt = append(totalFirstCorrupt, bothCorrupt...)

	totalSecondCorrupt := append([]string{}, secondCorrupt...)
	totalSecondCorrupt = append(totalSecondCorrupt, bothCorrupt...)

	// corruptFiles corrupts current newest backup and performs a new one
	corruptFiles := func(i int, toCorrupt []string) string {
		Print("Corrupt backup")
		for _, tc := range toCorrupt {
			file := path.Join(remoteDir, tc)
			body := bytes.NewBufferString(fmt.Sprintf("generation: %d", i))
			if err = srcH.Client.RclonePut(ctx, host.Addr, file, body); err != nil {
				t.Fatalf("Corrupt remote file %s", file)
			}
		}

		Print("Backup with corrupted SSTables in remote location")
		tag := srcH.simpleBackup(target.Location[0])

		Print("Validate creation of versioned files in remote location")
		for _, tc := range toCorrupt {
			corruptedPath := path.Join(remoteDir, tc) + VersionedFileExt(tag)
			if _, err = srcH.Client.RcloneFileInfo(ctx, host.Addr, corruptedPath); err != nil {
				t.Fatalf("Validate file %s: %s", corruptedPath, err)
			}
		}

		return tag
	}

	// This test case consists of 4 corrupted and 1 correct backup.
	// Corruption groups are chosen one by one in order to ensure creation of many versioned files.
	_ = corruptFiles(2, totalFirstCorrupt)
	tag3 := corruptFiles(3, totalSecondCorrupt)
	tag4 := corruptFiles(4, totalFirstCorrupt)
	tag5 := corruptFiles(5, totalSecondCorrupt)

	// This step is done so that we can test the restoration of valid backup with versioned files.
	// After this, only the third backup will be possible to restore.
	// In order to achieve that, versioned files from 3-rd backup (so files with 4-th or 5-th snapshot tag)
	// have to be swapped with their newest, correct versions.
	Print("Swap versioned files so that 3-rd backup can be restored")
	swapWithNewest := func(file, version string) {
		newest := path.Join(remoteDir, file)
		versioned := newest + VersionedFileExt(version)
		tmp := path.Join(path.Dir(newest), "tmp")

		if err = srcH.Client.RcloneMoveFile(ctx, host.Addr, tmp, newest); err != nil {
			t.Fatal(err)
		}
		if err = srcH.Client.RcloneMoveFile(ctx, host.Addr, newest, versioned); err != nil {
			t.Fatal(err)
		}
		if err = srcH.Client.RcloneMoveFile(ctx, host.Addr, versioned, tmp); err != nil {
			t.Fatal(err)
		}
	}
	// 3-rd backup consists of totalFirstCorrupt files introduced by 4-th backup
	for _, tc := range totalFirstCorrupt {
		swapWithNewest(tc, tag4)
	}
	// 3-rd backup consists of secondCorrupt files introduced by 5-th backup
	for _, tc := range secondCorrupt {
		swapWithNewest(tc, tag5)
	}

	Print("Restore 3-rd backup with versioned files")
	target.SnapshotTag = tag3
	target = dstH.regenerateRestoreTarget(target)

	if err = grantPermissionsToUser(dstSession, target, user); err != nil {
		t.Fatal(err)
	}

	if err = dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, target); err != nil {
		t.Fatal(err)
	}

	toValidate := []validateTable{
		{Keyspace: keyspace, Table: BigTableName, Column: "id"},
	}
	dstH.validateRestoreSuccess(dstSession, srcSession, target, toValidate)
}

const (
	mvName      = "testmv"
	siName      = "bydata"
	siTableName = "bydata_index"
)

func TestRestoreTablesViewCQLSchemaIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 4
		testLoadSize  = 5
		testBatchSize = 1
		testParallel  = 0
	)

	target := RestoreTarget{
		Location: []Location{
			{
				DC:       "dc1",
				Provider: S3,
				Path:     testBucket,
			},
		},
		// Check whether view will be restored even when it's not included
		Keyspace:      []string{testKeyspace + "." + BigTableName},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	restoreViewCQLSchema(t, target, testKeyspace, testLoadCnt, testLoadSize, testUser)
}

func restoreViewCQLSchema(t *testing.T, target RestoreTarget, keyspace string, loadCnt, loadSize int, user string) {
	var (
		ctx          = context.Background()
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
	)

	Print("When: Create Restore user")
	if err := createUser(dstSession, user, "pass"); err != nil {
		t.Fatal(err)
	}
	dstH = newRestoreTestHelperWithUser(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	if target.RestoreTables {
		Print("When: Recreate dst schema from CQL")
		WriteDataSecondClusterSchema(t, dstSession, keyspace, 0, 0, BigTableName)
		createBigTableViews(t, dstSession, keyspace, BigTableName, mvName, siName)
	}

	Print("When: Create src table with MV and SI")
	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	createBigTableViews(t, srcSession, keyspace, BigTableName, mvName, siName)
	time.Sleep(5 * time.Second)

	Print("When: Make src backup")
	target.SnapshotTag = srcH.simpleBackup(target.Location[0])
	target = dstH.regenerateRestoreTarget(target)

	Print("When: Grant minimal user permissions for restore")
	if err := grantPermissionsToUser(dstSession, target, user); err != nil {
		t.Fatal(err)
	}

	Print("When: Restore")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, target); err != nil {
		t.Fatal(err)
	}

	toValidate := []validateTable{
		{Keyspace: keyspace, Table: BigTableName, Column: "id"},
		{Keyspace: keyspace, Table: mvName, Column: "id"},
		{Keyspace: keyspace, Table: siTableName, Column: "id"},
	}

	Print("When: Validate restore success")
	dstH.validateRestoreSuccess(dstSession, srcSession, target, toValidate)
}

func TestRestoreFullViewSSTableSchemaIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 4
		testLoadSize  = 5
		testBatchSize = 1
		testParallel  = 0
	)

	locs := []Location{
		{
			DC:       "dc1",
			Provider: S3,
			Path:     testBucket,
		},
	}

	schemaTarget := RestoreTarget{
		Location:      locs,
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	tablesTarget := RestoreTarget{
		Location: locs,
		// Check whether view will be restored even when it's not included
		Keyspace:      []string{testKeyspace + "." + BigTableName},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	restoreViewSSTableSchema(t, schemaTarget, tablesTarget, testKeyspace, testLoadCnt, testLoadSize, testUser)
}

func restoreViewSSTableSchema(t *testing.T, schemaTarget, tablesTarget RestoreTarget, keyspace string, loadCnt, loadSize int, user string) {
	var (
		ctx          = context.Background()
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], &srcClientCfg)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
	)

	Print("When: Create Restore user")
	if err := createUser(dstSession, user, "pass"); err != nil {
		t.Fatal(err)
	}
	dstH = newRestoreTestHelperWithUser(t, mgrSession, cfg, schemaTarget.Location[0], nil, user, "pass")

	Print("When: Create src table with MV and SI")
	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	createBigTableViews(t, srcSession, keyspace, BigTableName, mvName, siName)
	time.Sleep(5 * time.Second)

	Print("When: Make src backup")
	schemaTarget.SnapshotTag = srcH.simpleBackup(schemaTarget.Location[0])
	schemaTarget = dstH.regenerateRestoreTarget(schemaTarget)

	Print("When: Grant minimal user permissions for restore schema")
	if err := grantPermissionsToUser(dstSession, schemaTarget, user); err != nil {
		t.Fatal(err)
	}

	Print("When: Restore schema")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, schemaTarget); err != nil {
		t.Fatal(err)
	}

	toValidate := []validateTable{
		{Keyspace: keyspace, Table: BigTableName, Column: "id"},
		{Keyspace: keyspace, Table: mvName, Column: "id"},
		{Keyspace: keyspace, Table: siTableName, Column: "id"},
	}

	Print("When: Validate restore schema success")
	dstH.validateRestoreSuccess(dstSession, srcSession, schemaTarget, toValidate)

	tablesTarget.SnapshotTag = schemaTarget.SnapshotTag
	dstH.ClusterID = uuid.MustRandom()
	dstH.RunID = uuid.MustRandom()
	tablesTarget = dstH.regenerateRestoreTarget(tablesTarget)

	Print("When: Grant minimal user permissions for restore tables")
	if err := grantPermissionsToUser(dstSession, tablesTarget, user); err != nil {
		t.Fatal(err)
	}

	Print("When: Restore tables")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, tablesTarget); err != nil {
		t.Fatal(err)
	}

	Print("When: Validate restore tables success")
	dstH.validateRestoreSuccess(dstSession, srcSession, tablesTarget, toValidate)
}

func TestRestoreFullIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 2
		testLoadSize  = 1
		testBatchSize = 1
		testParallel  = 3
	)

	locs := []Location{
		{
			DC:       "dc1",
			Provider: S3,
			Path:     testBucket,
		},
	}

	schemaTarget := RestoreTarget{
		Location:      locs,
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	tablesTarget := RestoreTarget{
		Location:      locs,
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	restoreAllTables(t, schemaTarget, tablesTarget, testKeyspace, testLoadCnt, testLoadSize, testUser)
}

func restoreAllTables(t *testing.T, schemaTarget, tablesTarget RestoreTarget, keyspace string, loadCnt, loadSize int, user string) {
	var (
		ctx          = context.Background()
		cfg          = DefaultConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], nil)
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], &srcClientCfg)
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
	)

	// Ensure clean scylla tables
	if err := cleanScyllaTables(srcSession); err != nil {
		t.Fatal(err)
	}
	if err := cleanScyllaTables(dstSession); err != nil {
		t.Fatal(err)
	}

	// Restore should be performed on user with limited permissions
	if err := createUser(dstSession, user, "pass"); err != nil {
		t.Fatal(err)
	}
	dstH = newRestoreTestHelperWithUser(t, mgrSession, cfg, schemaTarget.Location[0], nil, user, "pass")

	srcH.prepareRestoreBackupWithFeatures(srcSession, keyspace, loadCnt, loadSize)
	schemaTarget.SnapshotTag = srcH.simpleBackup(schemaTarget.Location[0])
	schemaTarget = dstH.regenerateRestoreTarget(schemaTarget)

	if err := grantPermissionsToUser(dstSession, schemaTarget, user); err != nil {
		t.Fatal(err)
	}

	Print("Restore schema on different cluster")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, schemaTarget); err != nil {
		t.Fatal(err)
	}

	toValidate := []validateTable{
		{Keyspace: keyspace, Table: BigTableName, Column: "id"},
		{Keyspace: keyspace, Table: mvName, Column: "id"},
		{Keyspace: keyspace, Table: siTableName, Column: "id"},
		{Keyspace: "system_auth", Table: "role_attributes", Column: "role"},
		{Keyspace: "system_auth", Table: "role_members", Column: "role"},
		{Keyspace: "system_auth", Table: "role_permissions", Column: "role"},
		{Keyspace: "system_auth", Table: "roles", Column: "role"},
		{Keyspace: "system_distributed", Table: "service_levels", Column: "service_level"},
		{Keyspace: "system_traces", Table: "events", Column: "session_id"},
		{Keyspace: "system_traces", Table: "node_slow_log", Column: "node_ip"},
		{Keyspace: "system_traces", Table: "node_slow_log_time_idx", Column: "session_id"},
		{Keyspace: "system_traces", Table: "sessions", Column: "session_id"},
		{Keyspace: "system_traces", Table: "sessions_time_idx", Column: "session_id"},
	}

	dstH.validateRestoreSuccess(dstSession, srcSession, schemaTarget, toValidate)

	tablesTarget.SnapshotTag = schemaTarget.SnapshotTag
	dstH.ClusterID = uuid.MustRandom()
	dstH.RunID = uuid.MustRandom()
	tablesTarget = dstH.regenerateRestoreTarget(tablesTarget)

	if err := grantPermissionsToUser(dstSession, tablesTarget, user); err != nil {
		t.Fatal(err)
	}

	Print("Restore tables on different cluster")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, tablesTarget); err != nil {
		t.Fatal(err)
	}

	dstH.validateRestoreSuccess(dstSession, srcSession, tablesTarget, toValidate)
}

// regenerateRestoreTarget applies GetRestoreTarget onto given restore target.
// It's useful for filling keyspace boilerplate.
func (h *restoreTestHelper) regenerateRestoreTarget(target RestoreTarget) RestoreTarget {
	props, err := json.Marshal(target)
	if err != nil {
		h.T.Fatal(err)
	}

	target, err = h.service.GetRestoreTarget(context.Background(), h.ClusterID, props)
	if err != nil {
		h.T.Fatal(err)
	}

	return target
}

func createUser(s gocqlx.Session, user, pass string) error {
	// Drop all non-superusers
	var (
		name  string
		super bool
	)
	iter := s.Query("LIST USERS", nil).Iter()
	for iter.Scan(&name, &super) {
		if !super {
			if err := s.ExecStmt(fmt.Sprintf("DROP USER '%s'", name)); err != nil {
				return errors.Wrap(err, "drop user")
			}
		}
	}
	time.Sleep(time.Second)

	if err := s.ExecStmt(fmt.Sprintf("CREATE USER '%s' WITH PASSWORD '%s'", user, pass)); err != nil {
		return errors.Wrap(err, "create restore test user")
	}
	return nil
}

// createUserWithPermissions creates user that can ALTER every table matched by target's keyspace param.
func grantPermissionsToUser(s gocqlx.Session, target RestoreTarget, user string) error {
	// Restoring schema shouldn't require any permissions
	if target.RestoreSchema {
		return nil
	}

	f, err := ksfilter.NewFilter(target.Keyspace)
	if err != nil {
		return errors.Wrap(err, "create filter")
	}

	var ks, t string
	iter := s.Query("SELECT keyspace_name, table_name FROM system_schema.tables", nil).Iter()
	for iter.Scan(&ks, &t) {
		// Regular tables require ALTER permission.
		if f.Check(ks, t) {
			if err = s.ExecStmt(fmt.Sprintf("GRANT ALTER ON %q.%q TO '%s'", ks, t, user)); err != nil {
				return errors.Wrap(err, "grant alter permission")
			}
		}
		// Views of restored base tables require DROP and CREATE permissions.
		if bt := baseTable(s, ks, t); bt != "" {
			if f.Check(ks, bt) {
				if err = s.ExecStmt(fmt.Sprintf("GRANT DROP ON %q.%q TO '%s'", ks, bt, user)); err != nil {
					return errors.Wrap(err, "grant drop permission")
				}
				if err = s.ExecStmt(fmt.Sprintf("GRANT CREATE ON %q TO '%s'", ks, user)); err != nil {
					return errors.Wrap(err, "grant create permission")
				}
			}
		}
	}

	return iter.Close()
}

type validateTable struct {
	Keyspace string
	Table    string
	Column   string
}

func (h *restoreTestHelper) getRowCount(s gocqlx.Session, vt validateTable) int {
	h.T.Helper()

	var (
		cnt int
		tmp string
	)

	it := s.Query(fmt.Sprintf("SELECT %s FROM %q.%q", vt.Column, vt.Keyspace, vt.Table), nil).Iter()
	for it.Scan(&tmp) {
		cnt++
	}
	if err := it.Close(); err != nil {
		h.T.Fatalf("Couldn't get tables (%s.%s, col: %s) row count: %s", vt.Keyspace, vt.Table, vt.Column, err)
	}

	return cnt
}

func (h *restoreTestHelper) validateRestoreSuccess(dstSession, srcSession gocqlx.Session, target RestoreTarget, tables []validateTable) {
	h.T.Helper()
	Print("Then: validate restore result")

	pr, err := h.service.GetRestoreProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
	if err != nil {
		h.T.Fatalf("Couldn't get progress: %s", err)
	}

	Printf("And: restore progress: %+#v\n", pr)
	if pr.Size != pr.Restored || pr.Size != pr.Downloaded {
		h.T.Fatal("Expected complete restore")
	}
	for _, kpr := range pr.Keyspaces {
		if kpr.Size != kpr.Restored || kpr.Size != kpr.Downloaded {
			h.T.Fatalf("Expected complete keyspace restore (%s)", kpr.Keyspace)
		}
		for _, tpr := range kpr.Tables {
			if tpr.Size != tpr.Restored || tpr.Size != tpr.Downloaded {
				h.T.Fatalf("Expected complete table restore (%s)", tpr.Table)
			}
		}
	}

	if target.RestoreSchema {
		// Required to load schema changes
		h.restartScylla()
	}

	Print("And: validate that restore preserves tombstone_gc mode")
	for _, t := range tables {
		// Don't validate views tombstone_gc
		if baseTable(srcSession, t.Keyspace, t.Table) != "" {
			continue
		}
		mode, err := h.service.GetTableTombstoneGC(context.Background(), h.ClusterID, t.Keyspace, t.Table)
		if err != nil {
			h.T.Fatalf("Couldn't check table's tombstone_gc mode (%s): %s", t, err)
		}
		if mode != "timeout" {
			h.T.Fatalf("Expected 'timeout' tombstone_gc mode, got: %s", mode)
		}
	}

	Print("When: query contents of restored table")
	for _, t := range tables {
		dstCnt := h.getRowCount(dstSession, t)
		srcCnt := 0
		if target.RestoreTables {
			srcCnt = h.getRowCount(srcSession, t)
		}

		h.T.Logf("%s, srcCount = %d, dstCount = %d", t, srcCnt, dstCnt)
		if dstCnt != srcCnt {
			// Destination cluster has additional users used for restore
			if t.Keyspace == "system_auth" {
				if target.RestoreTables && dstCnt < srcCnt {
					h.T.Fatalf("%s: srcCount != dstCount", t)
				}
				continue
			}
			h.T.Fatalf("srcCount != dstCount")
		}
	}
}

// cleanScyllaTables truncates scylla tables populated in prepareRestoreBackupWithFeatures.
func cleanScyllaTables(session gocqlx.Session) error {
	toBeTruncated := []string{
		"system_auth.role_attributes",
		"system_auth.role_members",
		"system_auth.role_permissions",
		"system_distributed.service_levels",
		"system_traces.events",
		"system_traces.node_slow_log",
		"system_traces.node_slow_log_time_idx",
		"system_traces.sessions",
		"system_traces.sessions_time_idx",
	}

	for _, t := range toBeTruncated {
		if err := session.ExecStmt("TRUNCATE TABLE " + t); err != nil {
			return errors.Wrap(err, "truncate table")
		}
	}
	if err := session.ExecStmt("DELETE FROM system_auth.roles WHERE role='role1' IF EXISTS"); err != nil {
		return errors.Wrap(err, "delete role1")
	}
	if err := session.ExecStmt("DELETE FROM system_auth.roles WHERE role='role2' IF EXISTS"); err != nil {
		return errors.Wrap(err, "delete role2")
	}
	return nil
}

// prepareRestoreBackupWithFeatures is a wrapper over prepareRestoreBackup that:
// - adds materialized view and secondary index
// - adds CDC log table
// - populates system_auth, system_traces, system_distributed tables
func (h *restoreTestHelper) prepareRestoreBackupWithFeatures(s gocqlx.Session, keyspace string, loadCnt, loadSize int) {
	statements := []string{
		"CREATE ROLE role1 WITH PASSWORD = 'pas' AND LOGIN = true",
		"CREATE SERVICE LEVEL sl WITH timeout = 500ms AND workload_type = 'interactive'",
		"ATTACH SERVICE_LEVEL sl TO role1",
		"GRANT SELECT ON system_schema.tables TO role1",
		"CREATE ROLE role2",
		"GRANT role1 TO role2",
	}

	t := gocql.NewTraceWriter(s.Session, os.Stdout) // Populate system_traces
	for _, stmt := range statements {
		if err := s.Query(stmt, nil).Trace(t).Exec(); err != nil {
			h.T.Fatalf("Exec stmt: %s, error: %s", stmt, err.Error())
		}
	}

	// Create keyspace and table
	WriteDataSecondClusterSchema(h.T, s, keyspace, 0, 0)

	ExecStmt(h.T, s,
		fmt.Sprintf("ALTER TABLE %s.%s WITH cdc = {'enabled': 'true', 'preimage': 'true'}", keyspace, BigTableName),
	)

	createBigTableViews(h.T, s, keyspace, BigTableName, mvName, siName)

	h.prepareRestoreBackup(s, keyspace, loadCnt, loadSize)
}

func createBigTableViews(t *testing.T, s gocqlx.Session, keyspace, baseTable, mv, si string) {
	t.Helper()

	ExecStmt(t, s,
		fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE data IS NOT NULL PRIMARY KEY (id, data)", keyspace, mv, keyspace, baseTable),
	)
	ExecStmt(t, s,
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s.%s (data)", si, keyspace, baseTable),
	)
}

// prepareRestoreBackup populates second cluster with loadCnt * loadSize MiB of data living in keyspace.big_table table.
// It writes loadCnt batches (each containing loadSize Mib of data), flushing inserted data into SSTables after each.
// It also disables compaction for keyspace.big_table so that SSTables flushed into memory are not merged together.
// This way we can efficiently test restore procedure without the need to produce big backups
// (restore functionality depends more on the amount of restored SSTables rather than on their total size).
func (h *restoreTestHelper) prepareRestoreBackup(session gocqlx.Session, keyspace string, loadCnt, loadSize int) {
	// Create keyspace and table
	WriteDataSecondClusterSchema(h.T, session, keyspace, 0, 0)

	var startingID int
	for i := 0; i < loadCnt; i++ {
		Printf("When: Write load nr %d to second cluster", i)

		startingID = WriteDataSecondClusterSchema(h.T, session, keyspace, startingID, loadSize)
		FlushTable(h.T, h.Client, ManagedSecondClusterHosts(), keyspace, BigTableName)
	}
}

func (h *restoreTestHelper) simpleBackup(location Location) string {
	h.T.Helper()

	// Make sure that next backup will have different snapshot tag
	time.Sleep(time.Second)

	ctx := context.Background()
	keyspaces, err := h.Client.Keyspaces(ctx)
	if err != nil {
		h.T.Fatal(err)
	}

	var units []Unit
	for _, ks := range keyspaces {
		units = append(units, Unit{
			Keyspace: ks,
		})
	}

	backupTarget := Target{
		Units:     units,
		DC:        []string{"dc1", "dc2"},
		Location:  []Location{location},
		Retention: 3,
	}

	Print("When: init backup target")
	if err = h.service.InitTarget(ctx, h.ClusterID, &backupTarget); err != nil {
		h.T.Fatalf("Couldn't init backup target: %s", err)
	}

	Print("When: backup cluster")
	// Task and Run IDs from restoreTestHelper should be reserved for restore tasks
	backupID := uuid.NewTime()
	if err = h.service.Backup(ctx, h.ClusterID, backupID, uuid.NewTime(), backupTarget); err != nil {
		h.T.Fatalf("Couldn't backup cluster: %s", err)
	}

	Print("When: list newly created backup")
	items, err := h.service.List(ctx, h.ClusterID, []Location{location}, ListFilter{
		ClusterID: h.ClusterID,
		TaskID:    backupID,
	})
	if err != nil {
		h.T.Fatalf("Couldn't list backup: %s", err)
	}
	if len(items) != 1 {
		h.T.Fatalf("List() = %v, expected one item", items)
	}
	i := items[0]
	Print(fmt.Sprintf("Then: backup snapshot info: %v", i.SnapshotInfo))

	return i.SnapshotInfo[0].SnapshotTag
}

func (h *restoreTestHelper) restartScylla() {
	h.T.Helper()
	Print("When: restart cluster")

	ctx := context.Background()
	cfg := cqlping.Config{Timeout: 100 * time.Millisecond}
	const cmdRestart = "supervisorctl restart scylla"

	for _, host := range h.GetAllHosts() {
		Print("When: restart Scylla on host: " + host)
		stdout, stderr, err := ExecOnHost(host, cmdRestart)
		if err != nil {
			h.T.Log("stdout", stdout)
			h.T.Log("stderr", stderr)
			h.T.Fatal("Command failed on host", host, err)
		}

		var sessionHosts []string
		b := backoff.WithContext(backoff.WithMaxRetries(
			backoff.NewConstantBackOff(500*time.Millisecond), 10), ctx)
		if err := backoff.Retry(func() error {
			sessionHosts, err = cluster.GetRPCAddresses(ctx, h.Client, []string{host})
			return err
		}, b); err != nil {
			h.T.Fatal(err)
		}

		cfg.Addr = sessionHosts[0]
		cond := func() bool {
			if _, err = cqlping.QueryPing(ctx, cfg, TestDBUsername(), TestDBPassword()); err != nil {
				return false
			}
			status, err := h.Client.Status(ctx)
			if err != nil {
				return false
			}
			return len(status.Live()) == 6
		}

		WaitCond(h.T, cond, time.Second, 60*time.Second)
		Print("Then: Scylla is restarted on host: " + host)
	}

	Print("Then: cluster is restarted")
}

func getBucketKeyspaceUser(t *testing.T) (string, string, string) {
	const (
		prefix = "TestRestore"
		suffix = "Integration"
	)

	name := t.Name()
	if !strings.HasPrefix(name, prefix) {
		t.Fatalf("Test name should start with '%s'", prefix)
	}
	if !strings.HasSuffix(name, suffix) {
		t.Fatalf("Test name should end with '%s'", suffix)
	}
	name = name[len(prefix) : len(name)-len(suffix)]

	re := regexp.MustCompile(`[A-Z][^A-Z]*`)
	matches := re.FindAllString(name, -1)

	var (
		elements = []string{"restoretest"}
		acc      string
	)
	// Merge acronyms into one element
	for _, m := range matches {
		if len(m) == 1 {
			acc += m
			continue
		}
		if acc != "" {
			elements = append(elements, acc)
			acc = ""
		}
		elements = append(elements, m)
	}
	if acc != "" {
		elements = append(elements, acc)
	}

	for i := range elements {
		elements[i] = strings.ToLower(elements[i])
	}

	var (
		bucketName   = strings.Join(elements, "-")
		keyspaceName = strings.Join(elements, "_")
		userName     = keyspaceName + "_user"
	)
	return bucketName, keyspaceName, userName
}

// baseTable returns view's base table or "" if it's not a view.
func baseTable(s gocqlx.Session, keyspace, table string) string {
	q := qb.Select("system_schema.views").
		Columns("base_table_name").
		Where(qb.Eq("keyspace_name")).
		Where(qb.Eq("view_name")).Query(s).BindMap(qb.M{
		"keyspace_name": keyspace,
		"view_name":     table,
	})
	defer q.Release()

	var baseTable string
	if err := q.Scan(&baseTable); err != nil {
		return ""
	}
	return baseTable
}
