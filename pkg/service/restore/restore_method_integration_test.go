// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package restore_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

const (
	rcloneRestoreAPIPath = "/storage_service/sstables"
	nativeRestoreAPIPath = "/storage_service/restore"
)

// restoreMethodAPIPath maps restore methods to the scylla API paths used by them.
var restoreMethodAPIPath = map[restore.Method]string{
	restore.MethodRclone: rcloneRestoreAPIPath,
	restore.MethodNative: nativeRestoreAPIPath,
	restore.MethodTablet: tabletRestorePath,
}

// nativeRestoreQueryParams are the query params expected in every native restore API call.
var nativeRestoreQueryParams = map[string]string{
	"scope":                "all",
	"primary_replica_only": "true",
}

// restoreMethodSupport describes restore method support of the test cluster.
// It depends on the scylla version and object storage endpoint configuration,
// so test case expectations are defined in terms of it.
type restoreMethodSupport struct {
	// Object storage endpoint is configured in scylla
	endpoint bool
	// Native and tablet aware restore APIs are exposed
	api map[restore.Method]bool
	// Method used by method=auto for vnode tables
	autoVnode restore.Method
	// Method used by method=auto for tablet tables
	autoTablet restore.Method
}

func newRestoreMethodSupport(t *testing.T, h *testHelper, loc backupspec.Location) restoreMethodSupport {
	t.Helper()

	ni, err := h.dstCluster.Client.AnyNodeInfo(t.Context())
	if err != nil {
		t.Fatal(errors.Wrap(err, "get any node info"))
	}
	nativeAPI, err := ni.SupportsNativeRestoreAPI()
	if err != nil {
		t.Fatal(err)
	}
	tabletAPI, err := ni.SupportsTabletRestoreAPI()
	if err != nil {
		t.Fatal(err)
	}

	_, err = ni.ScyllaObjectStorageEndpoint(loc.Provider)
	endpoint := err == nil

	s := restoreMethodSupport{
		endpoint: endpoint,
		api: map[restore.Method]bool{
			restore.MethodRclone: true,
			restore.MethodNative: nativeAPI,
			restore.MethodTablet: tabletAPI,
		},
		autoVnode:  restore.MethodRclone,
		autoTablet: restore.MethodRclone,
	}
	if nativeAPI && endpoint {
		s.autoVnode = restore.MethodNative
		s.autoTablet = restore.MethodNative
	}
	if tabletAPI && endpoint {
		s.autoTablet = restore.MethodTablet
	}
	return s
}

type restoreMethodFixture struct {
	h         *testHelper
	support   restoreMethodSupport
	vnodeTab  table
	tabletTab table
	// rows contains backed up tables contents
	rows map[table]map[int]int
	loc  backupspec.Location
	tag  string
}

type restoreMethodTestCase struct {
	name        string
	description string
	method      restore.MethodList
	// Restored tables, all fixture tables when empty
	tables []table
	// Expected failure of the restore target validation
	expectTargetError bool
	// Expected failure of the restore
	expectRestoreError bool
	// Methods expected to restore each of the restored tables
	expectedMethods map[table]restore.Method
}

func TestRestoreTablesMethodIntegration(t *testing.T) {
	// This test validates that vnode and tablet tables are restored with the scylla API
	// (rclone, native or tablet aware restore) matching the allowed methods:
	// - a single explicit method is used for all tables or the restore fails
	// - a list of methods restores each table with its most preferred method
	// - method=auto uses all methods supported by the cluster and skips the rest
	// - explicit native and tablet methods fail target validation when object storage endpoint is not configured
	// - explicit native and tablet methods fail restore when their API is not exposed, as scylla version is not validated
	// Expectations depend on the restore method support of the test cluster.
	fx := setupRestoreMethodFixture(t)
	s := fx.support

	testCases := []restoreMethodTestCase{
		{
			name:        `method="" uses rclone API`,
			description: "Verifies that omitting the method defaults to method=rclone.",
			method:      "",
			expectedMethods: map[table]restore.Method{
				fx.vnodeTab:  restore.MethodRclone,
				fx.tabletTab: restore.MethodRclone,
			},
		},
		{
			name:        "method=rclone uses rclone API",
			description: "Verifies that explicit rclone method restores all tables via the rclone copypaths API.",
			method:      "rclone",
			expectedMethods: map[table]restore.Method{
				fx.vnodeTab:  restore.MethodRclone,
				fx.tabletTab: restore.MethodRclone,
			},
		},
		{
			name:        "method=auto uses the most preferred supported API",
			description: "Verifies that auto mode restores tablet tables via the tablet aware restore API and the rest via the native restore API when the cluster supports them, and falls back to the rclone copypaths API otherwise.",
			method:      "auto",
			expectedMethods: map[table]restore.Method{
				fx.vnodeTab:  s.autoVnode,
				fx.tabletTab: s.autoTablet,
			},
		},
		{
			name:               "method=native uses native API",
			description:        "Verifies that explicit native method restores all tables via the native restore API.",
			method:             "native",
			expectTargetError:  !s.endpoint,
			expectRestoreError: !s.api[restore.MethodNative],
			expectedMethods: map[table]restore.Method{
				fx.vnodeTab:  restore.MethodNative,
				fx.tabletTab: restore.MethodNative,
			},
		},
		{
			name:               "method=native,rclone uses native API",
			description:        "Verifies that listing native method next to the rclone fallback restores all tables via the native restore API.",
			method:             "native,rclone",
			expectTargetError:  !s.endpoint,
			expectRestoreError: !s.api[restore.MethodNative],
			expectedMethods: map[table]restore.Method{
				fx.vnodeTab:  restore.MethodNative,
				fx.tabletTab: restore.MethodNative,
			},
		},
		{
			name:               "method=tablet uses tablet API",
			description:        "Verifies that explicit tablet method restores tablet tables via the tablet aware restore API.",
			method:             "tablet",
			tables:             []table{fx.tabletTab},
			expectTargetError:  !s.endpoint,
			expectRestoreError: !s.api[restore.MethodTablet],
			expectedMethods: map[table]restore.Method{
				fx.tabletTab: restore.MethodTablet,
			},
		},
		{
			name:               "method=tablet fails on vnode table",
			description:        "Verifies that explicit tablet method without any fallback fails when some of the restored tables can't be restored via the tablet aware restore API.",
			method:             "tablet",
			expectTargetError:  !s.endpoint,
			expectRestoreError: true,
		},
		{
			name:               "method=tablet,native uses tablet API with native fallback",
			description:        "Verifies that tablet tables are restored via the tablet aware restore API and the rest via the native restore API.",
			method:             "tablet,native",
			expectTargetError:  !s.endpoint,
			expectRestoreError: !s.api[restore.MethodTablet] || !s.api[restore.MethodNative],
			expectedMethods: map[table]restore.Method{
				fx.vnodeTab:  restore.MethodNative,
				fx.tabletTab: restore.MethodTablet,
			},
		},
		{
			name:               "method=tablet,rclone uses tablet API with rclone fallback",
			description:        "Verifies that tablet tables are restored via the tablet aware restore API and the rest via the rclone copypaths API.",
			method:             "tablet,rclone",
			expectTargetError:  !s.endpoint,
			expectRestoreError: !s.api[restore.MethodTablet],
			expectedMethods: map[table]restore.Method{
				fx.vnodeTab:  restore.MethodRclone,
				fx.tabletTab: restore.MethodTablet,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Log(tc.description)
			runRestoreMethodCase(t, fx, tc)
		})
	}
}

func setupRestoreMethodFixture(t *testing.T) restoreMethodFixture {
	t.Helper()

	// Using second cluster as src and dst, as tablet aware restore
	// is currently not supported for multi-dc clusters.
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedSecondClusterHosts())
	loc := testLocation("method", "")
	fx := restoreMethodFixture{
		h:         h,
		support:   newRestoreMethodSupport(t, h, loc),
		vnodeTab:  table{ks: randomizedName("method_vnode_"), tab: randomizedName("tab_")},
		tabletTab: table{ks: randomizedName("method_tablet_"), tab: randomizedName("tab_")},
		rows:      make(map[table]map[int]int),
		loc:       loc,
	}
	fx.createSchema(t)

	Print("Fill tables")
	for _, tt := range fx.tables() {
		fillTable(t, h.srcCluster.rootSession, 10, tt.ks, tt.tab)
		fx.rows[tt] = selectTableAsMap[int, int](t, h.srcCluster.rootSession, tt.ks, tt.tab, "id", "data")
	}

	Print("Run backup")
	InitBucket(t, fx.loc.Path)
	fx.tag = h.runBackup(t, map[string]any{
		"location": []backupspec.Location{fx.loc},
		"keyspace": fx.keyspaces(fx.tables()),
	})
	return fx
}

// tables returns all backed up tables.
func (fx restoreMethodFixture) tables() []table {
	return []table{fx.vnodeTab, fx.tabletTab}
}

// keyspaces returns keyspaces of the given tables.
func (fx restoreMethodFixture) keyspaces(tables []table) []string {
	out := make([]string, 0, len(tables))
	for _, tt := range tables {
		out = append(out, tt.ks)
	}
	return out
}

// createSchema creates the backed up keyspaces and tables.
// Note that src and dst describe the same cluster, so the schema is shared.
func (fx restoreMethodFixture) createSchema(t *testing.T) {
	t.Helper()
	Print("Create schema")

	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2} AND tablets = {'enabled': %v}"
	ExecStmt(t, fx.h.srcCluster.rootSession, fmt.Sprintf(ksStmt, fx.vnodeTab.ks, false))
	ExecStmt(t, fx.h.srcCluster.rootSession, fmt.Sprintf(ksStmt, fx.tabletTab.ks, true))
	for _, tt := range fx.tables() {
		ExecStmt(t, fx.h.srcCluster.rootSession, fmt.Sprintf("CREATE TABLE %q.%q (id int PRIMARY KEY, data int)", tt.ks, tt.tab))
	}
}

// prepare recreates the backed up keyspaces and generates a fresh
// restore task, so that test cases don't share any state.
func (fx restoreMethodFixture) prepare(t *testing.T) {
	t.Helper()

	Print("Recreate backed up keyspaces")
	for _, tt := range fx.tables() {
		ExecStmt(t, fx.h.dstCluster.rootSession, fmt.Sprintf("DROP KEYSPACE %q", tt.ks))
	}
	fx.createSchema(t)
	grantRestoreTablesPermissions(t, fx.h.dstCluster.Client, fx.h.dstCluster.rootSession, fx.keyspaces(fx.tables()), fx.h.dstUser)
	// Scylla tracks sstables downloaded during tablet aware restore per
	// snapshot and table, and skips them on the following restore attempts.
	// Truncate this data, so that the recreated tables are restored from scratch.
	if fx.support.api[restore.MethodTablet] {
		ExecStmt(t, fx.h.dstCluster.rootSession, "TRUNCATE TABLE system_distributed.snapshot_sstables")
	}
	fx.h.dstCluster.TaskID = uuid.NewTime()
	fx.h.dstCluster.RunID = uuid.NewTime()
}

func runRestoreMethodCase(t *testing.T, fx restoreMethodFixture, tc restoreMethodTestCase) {
	t.Helper()

	fx.prepare(t)

	tables := tc.tables
	if len(tables) == 0 {
		tables = fx.tables()
	}

	// Record the restore APIs used by the restore and validate native restore query params
	var (
		mu       sync.Mutex
		usedAPIs = make(map[restore.Method]struct{})
	)
	fx.h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		for m, p := range restoreMethodAPIPath {
			if !strings.HasPrefix(req.URL.Path, p) {
				continue
			}
			mu.Lock()
			usedAPIs[m] = struct{}{}
			mu.Unlock()
			if m != restore.MethodNative {
				continue
			}
			for k, v := range nativeRestoreQueryParams {
				if got := req.URL.Query().Get(k); got != v {
					t.Errorf("Expected native restore query param %q to be %q, got %q", k, v, got)
				}
			}
		}
		return nil, nil
	}))
	defer fx.h.dstCluster.Hrt.SetInterceptor(nil)

	props := map[string]any{
		"location":       []backupspec.Location{fx.loc},
		"snapshot_tag":   fx.tag,
		"restore_tables": true,
		"keyspace":       fx.keyspaces(tables),
	}
	if tc.method != "" {
		props["method"] = tc.method
	}
	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "marshal properties"))
	}

	if tc.expectTargetError {
		Print("Validate restore target")
		if _, _, _, err := fx.h.dstRestoreSvc.GetTargetUnitsViews(t.Context(), fx.h.dstCluster.ClusterID, rawProps); err == nil {
			t.Fatal("Expected GetTargetUnitsViews to fail")
		}
		return
	}

	Print("Run restore")
	err = fx.h.dstRestoreSvc.Restore(t.Context(), fx.h.dstCluster.ClusterID, fx.h.dstCluster.TaskID, fx.h.dstCluster.RunID, rawProps)
	if tc.expectRestoreError {
		if err == nil {
			t.Fatal("Expected restore to fail")
		}
		return
	}
	if err != nil {
		t.Fatal(errors.Wrap(err, "run restore"))
	}

	Print("Validate used restore APIs")
	expectedAPIs := make(map[restore.Method]struct{})
	for _, m := range tc.expectedMethods {
		expectedAPIs[m] = struct{}{}
	}
	mu.Lock()
	defer mu.Unlock()
	if !maps.Equal(usedAPIs, expectedAPIs) {
		t.Errorf("Expected %v restore APIs to be used, got %v",
			slices.Sorted(maps.Keys(expectedAPIs)), slices.Sorted(maps.Keys(usedAPIs)))
	}

	Print("Validate restore progress")
	pr := fx.h.getRestoreProgress(t)
	validateCompleteProgress(t, pr, tables)
	validateProgressMethod(t, pr, func(tt table) []restore.Method {
		return []restore.Method{tc.expectedMethods[tt]}
	})

	Print("Validate restored data")
	for _, tt := range tables {
		got := selectTableAsMap[int, int](t, fx.h.dstCluster.rootSession, tt.ks, tt.tab, "id", "data")
		if !maps.Equal(fx.rows[tt], got) {
			t.Errorf("Table %s.%s content mismatch after restore (expected %d rows, got %d)",
				tt.ks, tt.tab, len(fx.rows[tt]), len(got))
		}
	}
}

func TestRestoreFullChangingMethodIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())

	ni, err := h.srcCluster.Client.AnyNodeInfo(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := ni.SupportsNativeBackupAPI(); err != nil {
		t.Fatal(err)
	} else if !ok || IsIPV6Network() {
		t.Skip("Test assumes native backup usage")
	}

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ks := randomizedName("changing_method_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab := randomizedName("tab_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Location and permissions setup")
	loc := testLocation("changing-method", "")
	// Since native restore doesn't support localstorage,
	// we need to use s3 for testing changing method.
	loc.Provider = backupspec.S3
	S3InitBucket(t, loc.Path)
	ksFilter := []string{ks}
	backupProps := defaultTestBackupProperties(loc, ks)
	// Configure retention policy so that the last backup happens after purge
	backupProps["retention"] = 3
	backupProps["retention_days"] = 0
	backupProps["retention_map"] = backup.RetentionMap{
		h.srcCluster.TaskID: backup.RetentionPolicy{
			RetentionDays: 0,
			Retention:     3,
		},
	}
	grantRestoreSchemaPermissions(t, h.dstCluster.Client, h.dstCluster.rootSession, h.dstUser)

	type testIter struct {
		backupMethod backup.Method
		rowCnt       int
	}
	testCases := []testIter{
		{backupMethod: backup.MethodRclone, rowCnt: 50},
		{backupMethod: backup.MethodNative, rowCnt: 100},
		{backupMethod: backup.MethodRclone, rowCnt: 150},
		{backupMethod: backup.MethodNative, rowCnt: 200},
		{backupMethod: backup.MethodRclone, rowCnt: 250},
	}
	for i, tc := range testCases {
		t.Log("Fill: ", i)
		fillTable(t, h.srcCluster.rootSession, tc.rowCnt, ks, tab)

		t.Log("Backup: ", i)
		backupProps["method"] = tc.backupMethod
		tag := h.runBackup(t, backupProps)

		t.Log("Restore schema: ", i)
		ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf("DROP KEYSPACE IF EXISTS %q", ks))
		restoreProps := defaultTestProperties(loc, tag, false)
		h.runRestore(t, restoreProps)

		t.Log("Restore tables: ", i)
		grantRestoreTablesPermissions(t, h.dstCluster.Client, h.dstCluster.rootSession, ksFilter, h.dstUser)
		restoreProps = defaultTestProperties(loc, tag, true)
		h.runRestore(t, restoreProps)

		t.Log("Validate: ", i)
		h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
	}
}
