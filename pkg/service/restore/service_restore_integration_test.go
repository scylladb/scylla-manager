// Copyright (C) 2022 ScyllaDB

//go:build all || integration
// +build all integration

package restore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testhelper"
	"github.com/scylladb/scylla-manager/v3/pkg/util/jsonutil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"

	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type restoreTestHelper struct {
	*CommonTestHelper

	service   *Service
	backupSvc *backup.Service
	location  backupspec.Location
}

func newRestoreTestHelper(t *testing.T, session gocqlx.Session, config Config, location backupspec.Location, clientConf *scyllaclient.Config, user, pass string) *restoreTestHelper {
	t.Helper()

	S3InitBucket(t, location.Path)
	clusterID := uuid.MustRandom()

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	client := newTestClient(t, hrt, logger.Named("client"), clientConf)
	service, backupSvc := newTestService(t, session, client, config, logger, clusterID, user, pass)
	cHelper := &CommonTestHelper{
		Session:   session,
		Hrt:       hrt,
		Client:    client,
		ClusterID: clusterID,
		TaskID:    uuid.MustRandom(),
		RunID:     uuid.NewTime(),
		T:         t,
	}

	for _, ip := range cHelper.GetAllHosts() {
		if err := client.RcloneResetStats(context.Background(), ip); err != nil {
			t.Error("Couldn't reset stats", ip, err)
		}
	}

	return &restoreTestHelper{
		CommonTestHelper: cHelper,

		service:   service,
		backupSvc: backupSvc,
		location:  location,
	}
}

func newTestClient(t *testing.T, hrt *HackableRoundTripper, logger log.Logger, config *scyllaclient.Config) *scyllaclient.Client {
	t.Helper()

	if config == nil {
		c := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
		config = &c
	}
	config.Transport = hrt

	c, err := scyllaclient.NewClient(*config, logger)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newTestService(t *testing.T, session gocqlx.Session, client *scyllaclient.Client, c Config, logger log.Logger, clusterID uuid.UUID, user, pass string) (*Service, *backup.Service) {
	t.Helper()

	configCacheSvc := NewTestConfigCacheSvc(t, clusterID, client.Config().Hosts)

	repairSvc, err := repair.NewService(
		session,
		repair.DefaultConfig(),
		metrics.NewRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return CreateManagedClusterSession(t, false, client, user, pass), nil
		},
		configCacheSvc,
		log.NewDevelopmentWithLevel(zapcore.ErrorLevel).Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	backupSvc, err := backup.NewService(
		session,
		defaultBackupTestConfig(),
		metrics.NewBackupMetrics(),
		func(_ context.Context, id uuid.UUID) (string, error) {
			return "test_cluster", nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return CreateManagedClusterSession(t, false, client, user, pass), nil
		},
		configCacheSvc,
		log.NewDevelopmentWithLevel(zapcore.ErrorLevel).Named("backup"),
	)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewService(
		repairSvc,
		session,
		c,
		metrics.NewRestoreMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return CreateManagedClusterSession(t, false, client, user, pass), nil
		},
		configCacheSvc,
		logger.Named("restore"),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s, backupSvc
}

func s3Location(bucket string) backupspec.Location {
	return backupspec.Location{
		Provider: backupspec.S3,
		Path:     bucket,
	}
}

func testLocation(bucket, dc string) backupspec.Location {
	return backupspec.Location{
		DC:       dc,
		Provider: backupspec.S3,
		Path:     "restoretest-" + bucket,
	}
}

// newRenameSnapshotSSTablesRespInterceptor renames SSTables in the snapshot directory right
// after the snapshot has been taken. It uses the name mapping provided by the idGen function.
func newRenameSnapshotSSTablesRespInterceptor(client *scyllaclient.Client, s gocqlx.Session, idGen func(id string) string) func(*http.Response, error) (*http.Response, error) {
	return func(r *http.Response, err error) (*http.Response, error) {
		// Look for successful response to snapshot call
		if err != nil || !strings.HasPrefix(r.Request.URL.Path, "/storage_service/snapshots") || r.Request.Method != http.MethodPost {
			return nil, nil
		}
		host, _, err := net.SplitHostPort(r.Request.Host)
		if err != nil {
			return nil, errors.New("snapshot response notifier error: get response host: " + err.Error())
		}
		q := r.Request.URL.Query()
		ks := q.Get("kn")
		rawTabs := q.Get("cf")
		tag := q.Get("tag")
		tabs := strings.Split(rawTabs, ",")
		if len(tabs) == 0 || slices.Equal(tabs, []string{""}) {
			tabs, err = client.Tables(context.Background(), ks)
			if err != nil {
				return nil, errors.New("snapshot response notifier error: get keyspace tables: " + err.Error())
			}
		}

		for _, tab := range tabs {
			version, err := query.GetTableVersion(s, ks, tab)
			if err != nil {
				return nil, errors.New("snapshot response interceptor error: get table version: " + err.Error())
			}
			snapshotDir := path.Join(backupspec.KeyspaceDir(ks), tab+"-"+version, "snapshots", tag)
			// Get snapshot files
			files := make([]string, 0)
			err = client.RcloneListDirIter(context.Background(), host, snapshotDir, nil, func(item *scyllaclient.RcloneListDirItem) {
				// Watch out for the non-sstable files (e.g. backupspec.json)
				if _, err := sstable.ExtractID(item.Name); err != nil {
					return
				}
				files = append(files, item.Name)
			})
			if err != nil {
				return nil, errors.New("snapshot response interceptor error: list snapshot files: " + err.Error())
			}
			// Rename snapshot files
			mapping := sstable.RenameSStables(files, idGen)
			for initial, renamed := range mapping {
				if initial != renamed {
					src := path.Join(snapshotDir, initial)
					dst := path.Join(snapshotDir, renamed)
					if err := client.RcloneMoveFile(context.Background(), host, dst, src); err != nil {
						return nil, errors.New("snapshot response interceptor error: rename SSTable ID: " + err.Error())
					}
				}
			}
		}
		return nil, nil
	}
}

// halfUUIDToIntIDGen is a possible idGen that can be used in newRenameSnapshotSSTablesRespInterceptor.
// It maps around half of encountered UUID SSTables into integer SSTables.
// It only works if the snapshot dir has less than 10000000 SSTables.
func halfUUIDToIntIDGen() func(string) string {
	var mu sync.Mutex
	mapping := make(map[string]string)
	renameUUID := true
	cnt := 10000000
	return func(id string) string {
		mu.Lock()
		defer mu.Unlock()
		// Handle integer SSTable.
		// We want to leave them as they are.
		// We hope that because cnt is set to a huge
		// number, we won't encounter name collisions with
		// the renamed UUID SSTables.
		if _, err := strconv.Atoi(id); err == nil {
			return id
		}

		if newID, ok := mapping[id]; ok {
			return newID
		}
		cnt++
		// Handle UUID SSTable.
		// We want to rename only half of them.
		if renameUUID {
			mapping[id] = fmt.Sprint(cnt)
		} else {
			mapping[id] = id
		}
		renameUUID = !renameUUID
		return mapping[id]
	}
}

func TestRestoreGetTargetUnitsViewsIntegration(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		target string
		units  string
		views  string
	}{
		{
			name:   "tables",
			input:  "testdata/get_target/tables.input.json",
			target: "testdata/get_target/tables.target.json",
			units:  "testdata/get_target/tables.units.json",
			views:  "testdata/get_target/tables.views.json",
		},
		{
			name:   "schema",
			input:  "testdata/get_target/schema.input.json",
			target: "testdata/get_target/schema.target.json",
			units:  "testdata/get_target/schema.units.json",
			views:  "testdata/get_target/schema.views.json",
		},
		{
			name:   "default values",
			input:  "testdata/get_target/default_values.input.json",
			target: "testdata/get_target/default_values.target.json",
			units:  "testdata/get_target/default_values.units.json",
			views:  "testdata/get_target/default_values.views.json",
		},
		{
			name:   "continue false",
			input:  "testdata/get_target/continue_false.input.json",
			target: "testdata/get_target/continue_false.target.json",
			units:  "testdata/get_target/continue_false.units.json",
			views:  "testdata/get_target/continue_false.views.json",
		},
	}

	testBucket, _, _ := getBucketKeyspaceUser(t)
	var (
		ctx            = context.Background()
		cfg            = defaultTestConfig()
		mgrSession     = CreateScyllaManagerDBSession(t)
		loc            = s3Location(testBucket)
		h              = newRestoreTestHelper(t, mgrSession, cfg, loc, nil, "", "")
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	const (
		testKs1        = "ks1"
		testKs2        = "ks2"
		testTable1     = "table1"
		testTable2     = "table2"
		testMV         = "mv1"
		testSI         = "si1"
		testBackupSize = 1
	)

	Print("Tables setup")
	S3InitBucket(h.T, testBucket)

	WriteData(h.T, clusterSession, testKs1, testBackupSize, testTable1, testTable2)
	WriteData(h.T, clusterSession, testKs2, testBackupSize, testTable1, testTable2)

	var ignoreTarget []string
	var ignoreUnits []string
	var ignoredViews []string
	// Those tables have been migrated to system keyspace
	if CheckAnyConstraint(t, h.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		ignoreTarget = []string{
			"!system_auth.*",
			"!system_distributed.service_levels",
		}
		ignoreUnits = append(ignoreUnits,
			"system_auth",
			"service_levels",
		)
	}
	// Not available in Scylla opensource
	if CheckAnyConstraint(t, h.Client, "< 1000") {
		ignoreUnits = append(ignoreUnits, "system_replicated_keys")
	}
	// It's not possible to create views on tablet keyspaces
	rd := scyllaclient.NewRingDescriber(context.Background(), h.Client)
	if !rd.IsTabletKeyspace(testKs1) {
		CreateMaterializedView(h.T, clusterSession, testKs1, testTable1, testMV)
	} else {
		ignoredViews = append(ignoredViews, testMV)
		ignoreTarget = append(ignoreTarget, "!"+testKs1+"."+testMV)
	}
	if !rd.IsTabletKeyspace(testKs2) {
		CreateSecondaryIndex(h.T, clusterSession, testKs2, testTable2, testSI)
	} else {
		ignoredViews = append(ignoredViews, testSI)
		ignoreTarget = append(ignoreTarget, "!"+testKs2+"."+testSI+"_index")
	}

	tag := h.simpleBackup(s3Location(testBucket))
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := h
			h.T = t
			b, err := os.ReadFile(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			b = jsonutil.Set(b, "snapshot_tag", tag)

			var target Target
			if err = json.Unmarshal(b, &target); err != nil {
				t.Fatal(err)
			}
			if target.RestoreSchema {
				h.skipImpossibleSchemaTest()
			}

			target, units, views, err := h.service.GetTargetUnitsViews(ctx, h.ClusterID, b)
			if err != nil {
				t.Fatal(err)
			}
			if target.RestoreSchema {
				h.skipImpossibleSchemaTest()
			}

			if UpdateGoldenFiles() {
				var buf bytes.Buffer

				b, _ := json.Marshal(target)
				_ = json.Indent(&buf, b, "", "  ")
				if err := os.WriteFile(tc.target, buf.Bytes(), 0666); err != nil {
					t.Fatal(err)
				}
				buf.Reset()

				b, _ = json.Marshal(units)
				_ = json.Indent(&buf, b, "", "  ")
				if err := os.WriteFile(tc.units, buf.Bytes(), 0666); err != nil {
					t.Fatal(err)
				}
				buf.Reset()

				b, _ = json.Marshal(views)
				_ = json.Indent(&buf, b, "", "  ")
				if err := os.WriteFile(tc.views, buf.Bytes(), 0666); err != nil {
					t.Fatal(err)
				}
			}

			b, err = os.ReadFile(tc.target)
			if err != nil {
				t.Fatal(err)
			}
			var goldenTarget Target
			if err := json.Unmarshal(b, &goldenTarget); err != nil {
				t.Fatal(err)
			}

			b, err = os.ReadFile(tc.units)
			if err != nil {
				t.Fatal(err)
			}
			var goldenUnits []Unit
			if err := json.Unmarshal(b, &goldenUnits); err != nil {
				t.Fatal(err)
			}

			b, err = os.ReadFile(tc.views)
			if err != nil {
				t.Fatal(err)
			}
			var goldenViews []View
			if err := json.Unmarshal(b, &goldenViews); err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(goldenTarget, target,
				cmpopts.SortSlices(func(a, b string) bool { return a < b }),
				cmpopts.IgnoreUnexported(Target{}),
				cmpopts.IgnoreFields(Target{}, "SnapshotTag"),
				cmpopts.IgnoreSliceElements(func(v string) bool { return slices.Contains(ignoreTarget, v) }),
			); diff != "" {
				t.Fatal(tc.target, diff)
			}

			if diff := cmp.Diff(goldenUnits, units,
				cmpopts.SortSlices(func(a, b Unit) bool { return a.Keyspace < b.Keyspace }),
				cmpopts.SortSlices(func(a, b Table) bool { return a.Table < b.Table }),
				cmpopts.IgnoreFields(Unit{}, "Size"),
				cmpopts.IgnoreFields(Table{}, "Size"),
				cmpopts.IgnoreSliceElements(func(v Unit) bool { return slices.Contains(ignoreUnits, v.Keyspace) }),
				cmpopts.IgnoreSliceElements(func(v Table) bool { return slices.Contains(ignoreUnits, v.Table) }),
			); diff != "" {
				t.Fatal(tc.units, diff)
			}

			if goldenViews == nil {
				goldenViews = make([]View, 0)
			}
			if views == nil {
				views = make([]View, 0)
			}
			if diff := cmp.Diff(goldenViews, views,
				cmpopts.SortSlices(func(a, b View) bool { return a.Keyspace+a.View < b.Keyspace+b.View }),
				cmpopts.IgnoreSliceElements(func(v View) bool {
					return slices.Contains(ignoredViews, v.View)
				}),
				cmpopts.IgnoreFields(View{}, "CreateStmt")); diff != "" {
				t.Fatal(tc.views, diff)
			}
		})
	}
}

func TestRestoreGetTargetUnitsViewsErrorIntegration(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		error    string
		leaveTag bool
	}{
		{
			name:  "missing location",
			input: "testdata/get_target/missing_location.input.json",
			error: "missing location",
		},
		{
			name:  "duplicated locations",
			input: "testdata/get_target/duplicated_locations.input.json",
			error: "specified multiple times",
		},
		{
			name:  "restore both types",
			input: "testdata/get_target/restore_both_types.input.json",
			error: "choose EXACTLY ONE restore type",
		},
		{
			name:  "restore no type",
			input: "testdata/get_target/restore_no_type.input.json",
			error: "choose EXACTLY ONE restore type",
		},
		{
			name:  "schema and keyspace param",
			input: "testdata/get_target/schema_and_keyspace_param.input.json",
			error: "no need to specify '--keyspace' flag",
		},
		{
			name:  "inaccessible bucket",
			input: "testdata/get_target/inaccessible_bucket.input.json",
			error: "specified bucket does not exist",
		},
		{
			name:  "non-positive parallel",
			input: "testdata/get_target/non_positive_parallel.input.json",
			error: "parallel param has to be greater or equal to zero",
		},
		{
			name:  "non-positive batch size",
			input: "testdata/get_target/non_positive_batch_size.input.json",
			error: "batch size param has to be greater or equal to zero",
		},
		{
			name:  "no data matching keyspace pattern",
			input: "testdata/get_target/no_matching_keyspace.input.json",
			error: "no data in backup locations match given keyspace pattern",
		},
		{
			name:     "incorrect snapshot tag",
			input:    "testdata/get_target/incorrect_snapshot_tag.input.json",
			error:    "not a Scylla Manager snapshot tag",
			leaveTag: true,
		},
		{
			name:     "non-existing snapshot tag",
			input:    "testdata/get_target/non_existing_snapshot_tag.input.json",
			error:    "no snapshot with tag",
			leaveTag: true,
		},
	}

	testBucket, _, _ := getBucketKeyspaceUser(t)

	var (
		ctx            = context.Background()
		cfg            = defaultTestConfig()
		mgrSession     = CreateScyllaManagerDBSession(t)
		loc            = s3Location(testBucket)
		h              = newRestoreTestHelper(t, mgrSession, cfg, loc, nil, "", "")
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	const (
		testKs1        = "ks1"
		testKs2        = "ks2"
		testTable1     = "table1"
		testTable2     = "table2"
		testBackupSize = 1
	)

	Print("Tables setup")
	S3InitBucket(h.T, testBucket)

	WriteData(h.T, clusterSession, testKs1, testBackupSize, testTable1, testTable2)
	WriteData(h.T, clusterSession, testKs2, testBackupSize, testTable1, testTable2)

	tag := h.simpleBackup(s3Location(testBucket))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := os.ReadFile(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.leaveTag {
				b = jsonutil.Set(b, "snapshot_tag", tag)
			}

			_, _, _, err = h.service.GetTargetUnitsViews(ctx, h.ClusterID, b)
			if err == nil || !strings.Contains(err.Error(), tc.error) {
				t.Fatalf("GetTargetUnitsViews() got %v, expected %v", err, tc.error)
			}

			t.Log("GetTargetUnitsViews(): ", err)
		})
	}
}

func TestRestoreGetUnitsErrorIntegration(t *testing.T) {
	testBucket, testKeyspace, _ := getBucketKeyspaceUser(t)
	const (
		testBackupSize = 1
	)

	var (
		ctx            = context.Background()
		cfg            = defaultTestConfig()
		mgrSession     = CreateScyllaManagerDBSession(t)
		loc            = backupspec.Location{Provider: "s3", Path: testBucket}
		h              = newRestoreTestHelper(t, mgrSession, cfg, loc, nil, "", "")
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, testBackupSize)

	target := Target{
		Location: []backupspec.Location{
			{
				DC:       "dc1",
				Provider: backupspec.S3,
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

		_, _, _, err := h.service.GetTargetUnitsViews(ctx, h.ClusterID, h.targetToProperties(target))
		if err == nil {
			t.Fatal("GetRestoreUnits() expected error")
		}
		t.Log("GetRestoreUnits(): ", err)
	})

	t.Run("no data matching keyspace pattern", func(t *testing.T) {
		target := target
		target.Keyspace = []string{"fake_keyspace"}

		_, _, _, err := h.service.GetTargetUnitsViews(ctx, h.ClusterID, h.targetToProperties(target))
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

	target := Target{
		Location: []backupspec.Location{
			{
				DC:       "dc1",
				Provider: backupspec.S3,
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

func TestRestoreSchemaSmokeIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 1
		testLoadSize  = 1
		testBatchSize = 2
		testParallel  = 0
	)

	target := Target{
		Location: []backupspec.Location{
			{
				DC:       "dc1",
				Provider: backupspec.S3,
				Path:     testBucket,
			},
		},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	smokeRestore(t, target, testKeyspace, testLoadCnt, testLoadSize, testUser, "{'class': 'NetworkTopologyStrategy', 'dc1': 2}")
}

func smokeRestore(t *testing.T, target Target, keyspace string, loadCnt, loadSize int, user, replication string) {
	var (
		ctx          = context.Background()
		cfg          = defaultTestConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, "", "")
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg, "", "")
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
	)

	if target.RestoreSchema {
		dstH.skipImpossibleSchemaTest()
	}

	// Restore should be performed on user with limited permissions
	dropNonSuperUsers(t, dstSession)
	createUser(t, dstSession, user, "pass")
	dstH = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	// Recreate schema on destination cluster
	if target.RestoreTables {
		RawWriteData(t, dstSession, keyspace, 0, 0, replication, false)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	target.SnapshotTag = srcH.simpleBackup(target.Location[0])

	if target.RestoreTables {
		grantRestoreTablesPermissions(t, dstSession, target.Keyspace, user)
	} else {
		grantRestoreSchemaPermissions(t, dstSession, user)
	}

	Print("When: restore backup on different cluster = (dc1: 3 nodes, dc2: 3 nodes)")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(target)); err != nil {
		t.Fatal(err)
	}

	dstH.validateRestoreSuccess(dstSession, srcSession, target, []table{{ks: keyspace, tab: BigTableName}})
}

func TestRestoreTablesRestartAgentsIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 3
		testLoadSize  = 1
		testBatchSize = 1
		testParallel  = 2
	)

	target := Target{
		Location: []backupspec.Location{
			{
				DC:       "dc1",
				Provider: backupspec.S3,
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

func restoreWithAgentRestart(t *testing.T, target Target, keyspace string, loadCnt, loadSize int, user string) {
	var (
		cfg          = defaultTestConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, "", "")
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg, "", "")
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
		ctx          = context.Background()
	)

	if target.RestoreSchema {
		dstH.skipImpossibleSchemaTest()
		// Test relies on interceptors
		dstH.skipCQLSchemaTestAssumingSSTables()
	}

	// Restore should be performed on user with limited permissions
	dropNonSuperUsers(t, dstSession)
	createUser(t, dstSession, user, "pass")
	dstH = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	// Recreate schema on destination cluster
	if target.RestoreTables {
		WriteDataSecondClusterSchema(t, dstSession, keyspace, 0, 0)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	target.SnapshotTag = srcH.simpleBackup(target.Location[0])

	if target.RestoreTables {
		grantRestoreTablesPermissions(t, dstSession, target.Keyspace, user)
	} else {
		grantRestoreSchemaPermissions(t, dstSession, user)
	}

	a := atomic.NewInt64(0)
	dstH.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.Path, "/agent/rclone/sync/copypaths") && a.Inc() == 1 {
			Print("And: agents are restarted")
			dstH.CommonTestHelper.RestartAgents()
		}
		return nil, nil
	}))

	Print("When: Restore is running")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(target)); err != nil {
		t.Errorf("Expected no error but got %+v", err)
	}

	dstH.validateRestoreSuccess(dstSession, srcSession, target, []table{{ks: keyspace, tab: BigTableName}})
}

func TestRestoreTablesResumeIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 5
		testLoadSize  = 2
		testBatchSize = 1
		testParallel  = 3
	)

	target := Target{
		Location: []backupspec.Location{
			{
				DC:       "dc1",
				Provider: backupspec.S3,
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

	target := Target{
		Location: []backupspec.Location{
			{
				DC:       "dc1",
				Provider: backupspec.S3,
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

func restoreWithResume(t *testing.T, target Target, keyspace string, loadCnt, loadSize int, user string) {
	var (
		cfg           = defaultTestConfig()
		srcClientCfg  = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession    = CreateScyllaManagerDBSession(t)
		dstH          = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, "", "")
		srcH          = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg, "", "")
		dstSession    = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession    = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
		ctx1, cancel1 = context.WithCancel(context.Background())
		ctx2, cancel2 = context.WithCancel(context.Background())
	)

	if target.RestoreSchema {
		dstH.skipImpossibleSchemaTest()
		// Test relies on interceptors
		dstH.skipCQLSchemaTestAssumingSSTables()
	}

	// Restore should be performed on user with limited permissions
	dropNonSuperUsers(t, dstSession)
	createUser(t, dstSession, user, "pass")
	dstH = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	// Recreate schema on destination cluster
	if target.RestoreTables {
		WriteDataSecondClusterSchema(t, dstSession, keyspace, 0, 0)
	}

	// It's not possible to create views on tablet keyspaces
	tabToValidate := []string{BigTableName}
	rd := scyllaclient.NewRingDescriber(context.Background(), srcH.Client)
	if !rd.IsTabletKeyspace(keyspace) {
		mv := "mv_resume"
		CreateMaterializedView(t, srcSession, keyspace, BigTableName, mv)
		if target.RestoreTables {
			CreateMaterializedView(t, dstSession, keyspace, BigTableName, mv)
		}
		tabToValidate = append(tabToValidate, mv)
	}

	// Starting from SM 3.3.1, SM does not allow to back up views,
	// but backed up views should still be tested as older backups might
	// contain them. That's why here we manually force backup target
	// to contain the views.
	backupProps, err := json.Marshal(defaultTestBackupProperties(target.Location[0], ""))
	if err != nil {
		t.Fatal(err)
	}
	backupTarget, err := srcH.backupSvc.GetTarget(context.Background(), srcH.ClusterID, backupProps)
	if err != nil {
		t.Fatal(err)
	}
	backupTarget.Units = []backup.Unit{
		{
			Keyspace:  keyspace,
			Tables:    tabToValidate,
			AllTables: true,
		},
	}
	err = srcH.backupSvc.Backup(context.Background(), srcH.ClusterID, srcH.TaskID, srcH.RunID, backupTarget)
	if err != nil {
		t.Fatal(err)
	}
	backupPr, err := srcH.backupSvc.GetProgress(context.Background(), srcH.ClusterID, srcH.TaskID, srcH.RunID)
	if err != nil {
		t.Fatal(err)
	}
	target.SnapshotTag = backupPr.SnapshotTag

	if target.RestoreTables {
		grantRestoreTablesPermissions(t, dstSession, target.Keyspace, user)
	} else {
		grantRestoreSchemaPermissions(t, dstSession, user)
	}

	a := atomic.NewInt64(0)
	b := atomic.NewInt64(0)
	dstH.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if isLasOrRestoreEndpoint(req.URL.Path) {
			if a.Inc() == 1 {
				Print("And: context1 is canceled")
				cancel1()
			}
		}
		if strings.HasPrefix(req.URL.Path, "/storage_service/repair_async") && b.Inc() == 1 {
			Print("And: context2 is canceled")
			cancel2()
		}
		if strings.HasPrefix(req.URL.Path, "/storage_service/tablets/repair") && b.Inc() == 1 {
			Print("And: context2 is canceled")
			cancel2()
		}
		return nil, nil
	}))

	Print("When: run restore and stop it during load and stream")
	err = dstH.service.Restore(ctx1, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(target))
	if err == nil {
		t.Fatal("Expected error on run but got nil")
		return
	}
	if !strings.Contains(err.Error(), "context") {
		t.Fatalf("Expected context error but got: %+v", err)
	}

	Print("When: resume restore and stop in during repair")
	dstH.RunID = uuid.MustRandom()
	err = dstH.service.Restore(ctx2, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(target))
	if err == nil {
		t.Fatal("Expected error on run but got nil")
		return
	}
	if !strings.Contains(err.Error(), "context") {
		t.Fatalf("Expected context error but got: %+v", err)
	}

	pr, err := dstH.service.GetProgress(context.Background(), dstH.ClusterID, dstH.TaskID, dstH.RunID)
	if err != nil {
		t.Fatal(err)
	}
	Printf("And: restore progress: %+#v\n", pr)
	if pr.RepairProgress == nil || pr.RepairProgress.Success == 0 {
		t.Fatal("Expected partial repair progress")
	}

	Print("When: resume restore and complete it")
	dstH.RunID = uuid.MustRandom()
	err = dstH.service.Restore(context.Background(), dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(target))
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	Print("Then: data is restored")
	dstH.validateRestoreSuccess(dstSession, srcSession, target, []table{{ks: keyspace, tab: BigTableName}})
}

func TestRestoreTablesVersionedIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 5
		testLoadSize  = 1
		testBatchSize = 1
		testParallel  = 0
		corruptCnt    = 3
	)

	target := Target{
		Location: []backupspec.Location{
			{
				DC:       "dc1",
				Provider: backupspec.S3,
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
		testLoadCnt   = 5
		testLoadSize  = 1
		testBatchSize = 1
		testParallel  = 0
		corruptCnt    = 3
	)

	target := Target{
		Location: []backupspec.Location{
			{
				DC:       "dc1",
				Provider: backupspec.S3,
				Path:     testBucket,
			},
		},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	restoreWithVersions(t, target, testKeyspace, testLoadCnt, testLoadSize, corruptCnt, testUser)
}

func restoreWithVersions(t *testing.T, target Target, keyspace string, loadCnt, loadSize, corruptCnt int, user string) {
	var (
		cfg          = defaultTestConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, "", "")
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg, "", "")
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
		ctx          = context.Background()
	)

	if target.RestoreSchema {
		dstH.skipImpossibleSchemaTest()
		// Versioned files don't occur when restoring schema from CQL.
		dstH.skipCQLSchemaTestAssumingSSTables()
	}

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

	// Force creation of integer SSTables in the snapshot dir,
	// as only integer SSTables can be versioned.
	// This also allows us to test scenario with mixed ID type SSTables.
	srcH.Hrt.SetRespInterceptor(newRenameSnapshotSSTablesRespInterceptor(srcH.Client, srcSession, halfUUIDToIntIDGen()))

	// Restore should be performed on user with limited permissions
	//dropNonSuperUsers(t, dstSession)
	//createUser(t, dstSession, user, "pass")
	//dstH = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	if target.RestoreTables {
		Print("Recreate schema on destination cluster")
		WriteDataSecondClusterSchema(t, dstSession, keyspace, 0, 0)
	} else {
		// This test requires SSTables in Scylla data dir to remain unchanged.
		// This is achieved by NullCompactionStrategy in user table, but since system tables
		// cannot be altered, it has to be handled separately.
		if err := srcH.Client.DisableAutoCompaction(ctx, host.Addr, corruptedKeyspace, corruptedTable); err != nil {
			t.Fatal(err)
		}
		defer srcH.Client.EnableAutoCompaction(ctx, host.Addr, corruptedKeyspace, corruptedTable)
	}

	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)
	backupProps := defaultTestBackupProperties(target.Location[0], "")
	backupProps["method"] = backup.MethodAuto // This additionally validates fallback to rclone on versioned files creation
	srcH.simpleBackupWithProperties(target.Location[0], backupProps)

	// Corrupting SSTables allows us to force the creation of versioned files
	Print("Choose SSTables to corrupt")
	remoteDir := target.Location[0].RemotePath(backupspec.RemoteSSTableDir(srcH.ClusterID, host.Datacenter, host.HostID, corruptedKeyspace, corruptedTable))
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
		if _, err = backup.VersionedFileCreationTime(item.Name); err == nil {
			t.Fatalf("Versioned file %s present after first backup", path.Join(remoteDir, item.Path))
		}

		// Corrupt only integer SSTables
		id, err := sstable.ExtractID(item.Name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := strconv.Atoi(id); err != nil {
			return
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
	if len(firstCorrupt) == 0 || len(bothCorrupt) == 0 || len(secondCorrupt) == 0 {
		t.Fatalf("No files to be corrupted, firstCorrupt: %d, bothCorrupt: %d, secondCorrupt: %d",
			len(firstCorrupt), len(bothCorrupt), len(secondCorrupt))
	}

	crc32FileNameFromGivenSSTableFile := func(sstable string) string {
		// Split the filename by dashes
		parts := strings.Split(sstable, "-")

		if len(parts) < 2 {
			return sstable + "-Digest.crc32"
		}
		// Replace the last part with "Digest.crc32"
		parts[len(parts)-1] = "Digest.crc32"
		// Join the parts back together with dashes
		return strings.Join(parts, "-")
	}

	firstCorruptCrc32 := make(map[string]struct{})
	for _, f := range firstCorrupt {
		firstCorruptCrc32[crc32FileNameFromGivenSSTableFile(f)] = struct{}{}
	}
	bothCorruptCrc32 := make(map[string]struct{})
	for _, f := range bothCorrupt {
		bothCorruptCrc32[crc32FileNameFromGivenSSTableFile(f)] = struct{}{}
	}
	secondCorruptCrc32 := make(map[string]struct{})
	for _, f := range secondCorrupt {
		secondCorruptCrc32[crc32FileNameFromGivenSSTableFile(f)] = struct{}{}
	}
	for f := range bothCorruptCrc32 {
		firstCorruptCrc32[f] = struct{}{}
		secondCorruptCrc32[f] = struct{}{}
	}
	keys := func(m map[string]struct{}) []string {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		return keys
	}

	Printf("First group of corrupted SSTables: %v", firstCorrupt)
	Printf("Common group of corrupted SSTables: %v", bothCorrupt)
	Printf("Second group of corrupted SSTables: %v", secondCorrupt)

	totalFirstCorrupt := append([]string{}, firstCorrupt...)
	totalFirstCorrupt = append(totalFirstCorrupt, bothCorrupt...)
	totalFirstCorrupt = append(totalFirstCorrupt, keys(firstCorruptCrc32)...)

	totalSecondCorrupt := append([]string{}, secondCorrupt...)
	totalSecondCorrupt = append(totalSecondCorrupt, bothCorrupt...)
	totalSecondCorrupt = append(totalSecondCorrupt, keys(secondCorruptCrc32)...)

	// corruptFiles corrupts current newest backup and performs a new one
	// it's necessary to change the .crc32 file of given SSTable as well
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
		tag := srcH.simpleBackupWithProperties(target.Location[0], backupProps)

		Print("Validate creation of versioned files in remote location")
		for _, tc := range toCorrupt {
			corruptedPath := path.Join(remoteDir, tc) + backup.VersionedFileExt(tag)
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
		versioned := newest + backup.VersionedFileExt(version)
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
	for _, tc := range keys(secondCorruptCrc32) {
		swapWithNewest(tc, tag5)
	}

	Print("Restore 3-rd backup with versioned files")
	target.SnapshotTag = tag3

	if target.RestoreTables {
		//	grantRestoreTablesPermissions(t, dstSession, target.Keyspace, user)
	} else {
		//	grantRestoreSchemaPermissions(t, dstSession, user)
	}

	if err = dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(target)); err != nil {
		t.Fatal(err)
	}

	dstH.validateRestoreSuccess(dstSession, srcSession, target, []table{{ks: keyspace, tab: BigTableName}})
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

	target := Target{
		Location: []backupspec.Location{
			{
				DC:       "dc1",
				Provider: backupspec.S3,
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

func restoreViewCQLSchema(t *testing.T, target Target, keyspace string, loadCnt, loadSize int, user string) {
	var (
		ctx          = context.Background()
		cfg          = defaultTestConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, "", "")
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], &srcClientCfg, "", "")
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
	)

	if target.RestoreSchema {
		dstH.skipImpossibleSchemaTest()
	}

	Print("When: Create Restore user")
	dropNonSuperUsers(t, dstSession)
	createUser(t, dstSession, user, "pass")
	dstH = newRestoreTestHelper(t, mgrSession, cfg, target.Location[0], nil, user, "pass")

	Print("When: Create src table with MV and SI")
	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)

	rd := scyllaclient.NewRingDescriber(context.Background(), srcH.Client)
	if rd.IsTabletKeyspace(keyspace) {
		t.Skip("Test expects to create views, but it's not possible for tablet keyspaces")
	}

	CreateMaterializedView(t, srcSession, keyspace, BigTableName, mvName)
	CreateSecondaryIndex(t, srcSession, keyspace, BigTableName, siName)

	if target.RestoreTables {
		Print("When: Recreate dst schema from CQL")
		WriteDataSecondClusterSchema(t, dstSession, keyspace, 0, 0, BigTableName)
		CreateMaterializedView(t, dstSession, keyspace, BigTableName, mvName)
		CreateSecondaryIndex(t, dstSession, keyspace, BigTableName, siName)
	}
	time.Sleep(5 * time.Second)

	Print("When: Make src backup")
	target.SnapshotTag = srcH.simpleBackup(target.Location[0])

	if target.RestoreTables {
		grantRestoreTablesPermissions(t, dstSession, target.Keyspace, user)
	} else {
		grantRestoreSchemaPermissions(t, dstSession, user)
	}

	Print("When: Restore")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(target)); err != nil {
		t.Fatal(err)
	}

	Print("When: Validate restore success")
	dstH.validateRestoreSuccess(dstSession, srcSession, target, []table{{ks: keyspace, tab: BigTableName}, {ks: keyspace, tab: mvName}, {ks: keyspace, tab: siTableName}})
}

func TestRestoreFullViewSSTableSchemaIntegration(t *testing.T) {
	testBucket, testKeyspace, testUser := getBucketKeyspaceUser(t)
	const (
		testLoadCnt   = 4
		testLoadSize  = 5
		testBatchSize = 1
		testParallel  = 0
	)

	locs := []backupspec.Location{
		{
			DC:       "dc1",
			Provider: backupspec.S3,
			Path:     testBucket,
		},
	}

	schemaTarget := Target{
		Location:      locs,
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	tablesTarget := Target{
		Location: locs,
		// Check whether view will be restored even when it's not included
		Keyspace:      []string{testKeyspace + "." + BigTableName},
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	restoreViewSSTableSchema(t, schemaTarget, tablesTarget, testKeyspace, testLoadCnt, testLoadSize, testUser)
}

func restoreViewSSTableSchema(t *testing.T, schemaTarget, tablesTarget Target, keyspace string, loadCnt, loadSize int, user string) {
	var (
		ctx          = context.Background()
		cfg          = defaultTestConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], nil, "", "")
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], &srcClientCfg, "", "")
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
	)

	dstH.skipImpossibleSchemaTest()

	Print("When: Create Restore user")
	dropNonSuperUsers(t, dstSession)
	createUser(t, dstSession, user, "pass")
	dstH = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], nil, user, "pass")

	Print("When: Create src table with MV and SI")
	srcH.prepareRestoreBackup(srcSession, keyspace, loadCnt, loadSize)

	rd := scyllaclient.NewRingDescriber(context.Background(), srcH.Client)
	if rd.IsTabletKeyspace(keyspace) {
		t.Skip("Test expects to create views, but it's not possible for tablet keyspaces")
	}

	CreateMaterializedView(t, srcSession, keyspace, BigTableName, mvName)
	CreateSecondaryIndex(t, srcSession, keyspace, BigTableName, siName)
	time.Sleep(5 * time.Second)

	Print("When: Make src backup")
	schemaTarget.SnapshotTag = srcH.simpleBackup(schemaTarget.Location[0])

	Print("When: Restore schema")
	grantRestoreSchemaPermissions(t, dstSession, user)
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(schemaTarget)); err != nil {
		t.Fatal(err)
	}

	Print("When: Validate restore schema success")
	toValidate := []table{{ks: keyspace, tab: BigTableName}, {ks: keyspace, tab: mvName}, {ks: keyspace, tab: siTableName}}
	dstH.validateRestoreSuccess(dstSession, srcSession, schemaTarget, toValidate)

	tablesTarget.SnapshotTag = schemaTarget.SnapshotTag
	dstH.TaskID = uuid.MustRandom()
	dstH.RunID = uuid.MustRandom()

	Print("When: Grant minimal user permissions for restore tables")
	grantRestoreTablesPermissions(t, dstSession, tablesTarget.Keyspace, user)

	Print("When: Restore tables")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(tablesTarget)); err != nil {
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

	locs := []backupspec.Location{
		{
			DC:       "dc1",
			Provider: backupspec.S3,
			Path:     testBucket,
		},
	}

	schemaTarget := Target{
		Location:      locs,
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	tablesTarget := Target{
		Location:      locs,
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	restoreAllTables(t, schemaTarget, tablesTarget, testKeyspace, testLoadCnt, testLoadSize, testUser)
}

func restoreAllTables(t *testing.T, schemaTarget, tablesTarget Target, keyspace string, loadCnt, loadSize int, user string) {
	var (
		ctx          = context.Background()
		cfg          = defaultTestConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], nil, "", "")
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], &srcClientCfg, "", "")
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
	)

	dstH.skipImpossibleSchemaTest()

	// Ensure clean scylla tables
	if err := cleanScyllaTables(t, srcSession, srcH.Client); err != nil {
		t.Fatal(err)
	}
	if err := cleanScyllaTables(t, dstSession, dstH.Client); err != nil {
		t.Fatal(err)
	}

	// Restore should be performed on user with limited permissions
	dropNonSuperUsers(t, dstSession)
	createUser(t, dstSession, user, "pass")
	dstH = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], nil, user, "pass")

	srcH.prepareRestoreBackupWithFeatures(srcSession, keyspace, loadCnt, loadSize)
	schemaTarget.SnapshotTag = srcH.simpleBackup(schemaTarget.Location[0])

	Print("Restore schema on different cluster")
	grantRestoreSchemaPermissions(t, dstSession, user)
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(schemaTarget)); err != nil {
		t.Fatal(err)
	}

	toValidate := []table{
		{ks: "system_traces", tab: "events"},
		{ks: "system_traces", tab: "node_slow_log"},
		{ks: "system_traces", tab: "node_slow_log_time_idx"},
		{ks: "system_traces", tab: "sessions"},
		{ks: "system_traces", tab: "sessions_time_idx"},
	}
	rd := scyllaclient.NewRingDescriber(context.Background(), srcH.Client)
	if !rd.IsTabletKeyspace(keyspace) {
		toValidate = append(toValidate,
			table{ks: keyspace, tab: BigTableName},
			table{ks: keyspace, tab: mvName},
			table{ks: keyspace, tab: siTableName},
		)
	}
	if !CheckAnyConstraint(t, dstH.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		toValidate = append(toValidate,
			table{ks: "system_auth", tab: "role_attributes"},
			table{ks: "system_auth", tab: "role_members"},
			table{ks: "system_auth", tab: "role_permissions"},
			table{ks: "system_auth", tab: "roles"},
			table{ks: "system_distributed", tab: "service_levels"},
		)
	}

	dstH.validateRestoreSuccess(dstSession, srcSession, schemaTarget, toValidate)

	tablesTarget.SnapshotTag = schemaTarget.SnapshotTag
	dstH.TaskID = uuid.MustRandom()
	dstH.RunID = uuid.MustRandom()
	grantRestoreTablesPermissions(t, dstSession, tablesTarget.Keyspace, user)

	Print("Restore tables on different cluster")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(tablesTarget)); err != nil {
		t.Fatal(err)
	}

	dstH.validateRestoreSuccess(dstSession, srcSession, tablesTarget, toValidate)
}

func TestRestoreFullAlternatorIntegration(t *testing.T) {
	testBucket, _, testUser := getBucketKeyspaceUser(t)
	const (
		testTable          = "Tab_le-With1.da_sh2-aNd.d33ot.-"
		testKeyspace       = "alternator_" + testTable
		testBatchSize      = 1
		testParallel       = 3
		testAlternatorPort = 8000
	)

	locs := []backupspec.Location{
		{
			DC:       "dc1",
			Provider: backupspec.S3,
			Path:     testBucket,
		},
	}

	schemaTarget := Target{
		Location:      locs,
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreSchema: true,
	}

	tablesTarget := Target{
		Location:      locs,
		BatchSize:     testBatchSize,
		Parallel:      testParallel,
		RestoreTables: true,
	}

	restoreAlternator(t, schemaTarget, tablesTarget, testKeyspace, testTable, testUser, testAlternatorPort)
}

func restoreAlternator(t *testing.T, schemaTarget, tablesTarget Target, testKeyspace, testTable, user string, alternatorPort int) {
	var (
		ctx          = context.Background()
		cfg          = defaultTestConfig()
		srcClientCfg = scyllaclient.TestConfig(ManagedSecondClusterHosts(), AgentAuthToken())
		mgrSession   = CreateScyllaManagerDBSession(t)
		dstH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], nil, "", "")
		srcH         = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], &srcClientCfg, "", "")
		dstSession   = CreateSessionAndDropAllKeyspaces(t, dstH.Client)
		srcSession   = CreateSessionAndDropAllKeyspaces(t, srcH.Client)
	)

	dstH.skipImpossibleSchemaTest()
	// SM can't restore Alternator table schema via CQL.
	// See https://github.com/scylladb/scylladb/issues/19112
	dstH.skipCQLSchemaTestAssumingSSTables()

	// Restore should be performed on user with limited permissions
	dropNonSuperUsers(t, dstSession)
	createUser(t, dstSession, user, "pass")
	dstH = newRestoreTestHelper(t, mgrSession, cfg, schemaTarget.Location[0], nil, user, "pass")

	accessKeyID, secretAccessKey := CreateAlternatorUser(t, srcSession, "")
	svc := CreateDynamoDBService(t, ManagedSecondClusterHosts()[0], alternatorPort, accessKeyID, secretAccessKey)
	CreateAlternatorTable(t, svc, testTable)
	FillAlternatorTableWithOneRow(t, svc, testTable)

	schemaTarget.SnapshotTag = srcH.simpleBackup(schemaTarget.Location[0])

	Print("Restore schema on different cluster")
	grantRestoreSchemaPermissions(t, dstSession, user)
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(schemaTarget)); err != nil {
		t.Fatal(err)
	}

	toValidate := []table{{ks: testKeyspace, tab: testTable}}
	dstH.validateRestoreSuccess(dstSession, srcSession, schemaTarget, toValidate)

	tablesTarget.SnapshotTag = schemaTarget.SnapshotTag
	dstH.TaskID = uuid.MustRandom()
	dstH.RunID = uuid.MustRandom()
	grantRestoreTablesPermissions(t, dstSession, tablesTarget.Keyspace, user)

	Print("Restore tables on different cluster")
	if err := dstH.service.Restore(ctx, dstH.ClusterID, dstH.TaskID, dstH.RunID, dstH.targetToProperties(tablesTarget)); err != nil {
		t.Fatal(err)
	}

	dstH.validateRestoreSuccess(dstSession, srcSession, tablesTarget, toValidate)
}

func (h *restoreTestHelper) targetToProperties(target Target) json.RawMessage {
	props, err := json.Marshal(target)
	if err != nil {
		h.T.Fatal(err)
	}
	return props
}

func (h *restoreTestHelper) validateRestoreSuccess(dstSession, srcSession gocqlx.Session, target Target, tables []table) {
	h.T.Helper()
	Print("Then: validate restore result")

	if target.RestoreSchema && !h.isRestoreSchemaFromCQLSupported() {
		// Cluster restart is required after restoring schema from SSTables
		h.restartScylla()
	}

	Print("And: validate that restore preserves tombstone_gc mode")
	for _, t := range tables {
		// Don't validate views tombstone_gc
		if baseTable(h.T, srcSession, t.ks, t.tab) != "" {
			continue
		}
		srcMode := tombstoneGCMode(h.T, srcSession, t.ks, t.tab)
		dstMode := tombstoneGCMode(h.T, dstSession, t.ks, t.tab)
		if srcMode != dstMode {
			h.T.Fatalf("Expected %s tombstone_gc mode, got: %s", srcMode, dstMode)
		}
	}

	Print("When: query contents of restored table")
	for _, t := range tables {
		dstCnt := rowCount(h.T, dstSession, t.ks, t.tab)
		srcCnt := 0
		if target.RestoreTables {
			srcCnt = rowCount(h.T, srcSession, t.ks, t.tab)
		}

		h.T.Logf("%s, srcCount = %d, dstCount = %d", t, srcCnt, dstCnt)
		if dstCnt != srcCnt {
			// Destination cluster has additional users used for restore
			if t.ks == "system_auth" {
				if target.RestoreTables && dstCnt < srcCnt {
					h.T.Fatalf("%s: srcCount != dstCount", t)
				}
				continue
			}
			h.T.Fatalf("srcCount != dstCount")
		}
	}

	pr, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
	if err != nil {
		h.T.Fatalf("Couldn't get progress: %s", err)
	}

	Printf("TOTAL %v %v %v", pr.Downloaded, pr.Size, pr.Restored)
	for _, kpr := range pr.Keyspaces {
		for _, tpr := range kpr.Tables {
			Printf("name %s %v %v %v", tpr.Table, tpr.Downloaded, tpr.Size, tpr.Restored)
			if tpr.Size != tpr.Restored {
				h.T.Fatalf("Expected complete table restore (%s)", tpr.Table)
			}
		}
		if kpr.Size != kpr.Restored {
			h.T.Fatalf("Expected complete keyspace restore (%s)", kpr.Keyspace)
		}
	}
	if pr.Size != pr.Restored {
		h.T.Fatal("Expected complete restore")
	}
}

// cleanScyllaTables truncates scylla tables populated in prepareRestoreBackupWithFeatures.
func cleanScyllaTables(t *testing.T, session gocqlx.Session, client *scyllaclient.Client) error {
	ExecStmt(t, session, "DROP ROLE IF EXISTS role2")
	ExecStmt(t, session, "DROP ROLE IF EXISTS role1")
	ExecStmt(t, session, "DROP SERVICE LEVEL IF EXISTS sl")

	toBeTruncated := []string{
		"system_traces.events",
		"system_traces.node_slow_log",
		"system_traces.node_slow_log_time_idx",
		"system_traces.sessions",
		"system_traces.sessions_time_idx",
	}
	for _, tab := range toBeTruncated {
		ExecStmt(t, session, "TRUNCATE TABLE "+tab)
	}
	return nil
}

// prepareRestoreBackupWithFeatures is a wrapper over prepareRestoreBackup that:
// - adds materialized view and secondary index (for vnode keyspace only)
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

	rd := scyllaclient.NewRingDescriber(context.Background(), h.Client)
	if !rd.IsTabletKeyspace(keyspace) {
		// CDC does not work with tablets
		ExecStmt(h.T, s,
			fmt.Sprintf("ALTER TABLE %s.%s WITH cdc = {'enabled': 'true', 'preimage': 'true'}", keyspace, BigTableName),
		)
		// It's not possible to create views on tablet keyspaces
		CreateMaterializedView(h.T, s, keyspace, BigTableName, mvName)
		CreateSecondaryIndex(h.T, s, keyspace, BigTableName, siName)
	}

	h.prepareRestoreBackup(s, keyspace, loadCnt, loadSize)
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

func (h *restoreTestHelper) simpleBackup(location backupspec.Location) string {
	h.T.Helper()

	return h.simpleBackupWithProperties(location, defaultTestBackupProperties(location, ""))
}

func (h *restoreTestHelper) simpleBackupWithProperties(loc backupspec.Location, props map[string]any) string {
	h.T.Helper()

	ctx := context.Background()
	rawProps, err := json.Marshal(props)
	if err != nil {
		h.T.Fatal(err)
	}

	target, err := h.backupSvc.GetTarget(ctx, h.ClusterID, rawProps)
	if err != nil {
		h.T.Fatal(err)
	}

	Print("When: backup cluster")
	// Task and Run IDs from restoreTestHelper should be reserved for restore tasks
	backupID := uuid.NewTime()
	if err = h.backupSvc.Backup(ctx, h.ClusterID, backupID, uuid.NewTime(), target); err != nil {
		h.T.Fatalf("Couldn't backup cluster: %s", err)
	}

	Print("When: list newly created backup")
	items, err := h.backupSvc.List(ctx, h.ClusterID, []backupspec.Location{loc}, backup.ListFilter{
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
			sessionHosts, err = cluster.GetRPCAddresses(ctx, h.Client, []string{host}, false)
			return err
		}, b); err != nil {
			h.T.Fatal(err)
		}

		cfg.Addr = sessionHosts[0]
		if testconfig.IsSSLEnabled() {
			sslOpts := testconfig.CQLSSLOptions()
			cfg.TLSConfig, err = testconfig.TLSConfig(sslOpts)
			if err != nil {
				h.T.Fatalf("tls config: %v", err)
			}
		}
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

// skipImpossibleRestoreSchemaTest skips the test if there
// is no way to restore schema for this cluster configuration.
func (h *restoreTestHelper) skipImpossibleSchemaTest() {
	restoreSchemaFromSSTableSupport := IsRestoreSchemaFromSSTablesSupported(context.Background(), h.Client)
	restoreSchemaFromCQLSupport := h.isRestoreSchemaFromCQLSupported()
	if restoreSchemaFromSSTableSupport != nil && !restoreSchemaFromCQLSupport {
		h.T.Skip("Given cluster configuration does not support schema restoration from neither SSTables or CQL")
	}
}

// skipCQLSchemaTestAssumingSSTables skips the test if it
// assumes that schema will be restored from SSTables,
// but it will actually be restored from CQL.
// This method might be helpful for tests using interceptors.
func (h *restoreTestHelper) skipCQLSchemaTestAssumingSSTables() {
	if h.isRestoreSchemaFromCQLSupported() {
		h.T.Skip("This test assumes that schema is restored from SSTables, " +
			"but it is restored from CQL")
	}
}

func (h *restoreTestHelper) isRestoreSchemaFromCQLSupported() bool {
	return CheckAnyConstraint(h.T, h.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000")
}
