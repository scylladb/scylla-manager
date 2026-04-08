// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package restore_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/pkg/util/version"
)

const (
	rcloneRestoreAPIPath = "/storage_service/sstables"
	nativeRestoreAPIPath = "/storage_service/restore"
)

type restoreMethodFixture struct {
	h   *testHelper
	ks  string
	tab string
	loc backupspec.Location
	tag string
}

type restoreMethodTestCase struct {
	name               string
	description        string
	method             restore.Method
	ensuredPath        string
	blockedPath        string
	ensuredQueryParams map[string]string
	expectTargetError  bool
}

func TestRestoreTablesMethodIntegration(t *testing.T) {
	// This test validates that the correct API is used
	// for restoring batches (rclone or scylla).
	fx := setupRestoreMethodFixture(t)

	ni, err := fx.h.dstCluster.Client.AnyNodeInfo(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// Native restore was released in 2025.2 as experimental and released in 2026.1 as production ready.
	// When using method=auto, SM uses native method only when native restore is production ready.
	// When using method=native, SM uses native method even when it's experimental.
	nativeAPIExposed, err := version.CheckConstraint(ni.ScyllaVersion, ">= 2025.2")
	if err != nil {
		t.Fatal(err)
	}
	nativeAPISupported, err := ni.SupportsNativeRestoreAPI()
	if err != nil {
		t.Fatal(err)
	}
	// IPV6 object storage endpoints are not supported by scylla
	configuredObjectStorageEndpoint := !IsIPV6Network()
	nativeQueryParams := map[string]string{
		"scope":                "all",
		"primary_replica_only": "true",
	}
	// Test cases vary based on used scylla version and IP family.
	// Test cases for default and method=rclone.
	testCases := []restoreMethodTestCase{
		{
			name:              "method=rclone uses rclone API",
			description:       "Verifies that explicit rclone method restores data via the rclone copypaths API and does not touch the native restore endpoint.",
			method:            restore.MethodRclone,
			ensuredPath:       rcloneRestoreAPIPath,
			blockedPath:       nativeRestoreAPIPath,
			expectTargetError: false,
		},
		{
			name:              `method="" uses rclone API`,
			description:       "Verifies that omitting the method defaults to method=rclone.",
			method:            "",
			ensuredPath:       rcloneRestoreAPIPath,
			blockedPath:       nativeRestoreAPIPath,
			expectTargetError: false,
		},
	}
	// Test cases for method=auto
	if nativeAPISupported && configuredObjectStorageEndpoint {
		testCases = append(testCases, restoreMethodTestCase{
			name:               "method=auto uses native API when supported",
			description:        "Verifies that auto mode upgrades to native restore when the cluster supports it and that the expected native query parameters are sent.",
			method:             restore.MethodAuto,
			ensuredPath:        nativeRestoreAPIPath,
			blockedPath:        rcloneRestoreAPIPath,
			ensuredQueryParams: nativeQueryParams,
			expectTargetError:  false,
		})
	} else {
		testCases = append(testCases, restoreMethodTestCase{
			name:              "method=auto falls back to rclone API",
			description:       "Verifies that auto mode falls back to rclone when either the cluster doesn't support native restore or object storage endpoint is not configured.",
			method:            restore.MethodAuto,
			ensuredPath:       rcloneRestoreAPIPath,
			blockedPath:       nativeRestoreAPIPath,
			expectTargetError: false,
		})
	}
	// Test cases for method=native
	if nativeAPIExposed && configuredObjectStorageEndpoint {
		testCases = append(testCases, restoreMethodTestCase{
			name:               "method=native uses native API when exposed",
			description:        "Verifies that explicit native method uses the native restore endpoint when the API is exposed and object storage endpoint is configured.",
			method:             restore.MethodNative,
			ensuredPath:        nativeRestoreAPIPath,
			blockedPath:        rcloneRestoreAPIPath,
			ensuredQueryParams: nativeQueryParams,
			expectTargetError:  false,
		})
	} else {
		testCases = append(testCases, restoreMethodTestCase{
			name:              "method=native target validation fails when unavailable",
			description:       "Verifies that explicit native method is rejected during target validation when the cluster doesn't expose native restore API or object storage endpoint is not configured.",
			method:            restore.MethodNative,
			expectTargetError: true,
		})
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

	h := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ks := randomizedName("method_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab := randomizedName("tab_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab)

	Print("Backup setup")
	loc := testLocation("method", "")
	InitBucket(t, loc.Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ks))
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)

	return restoreMethodFixture{
		h:   h,
		ks:  ks,
		tab: tab,
		loc: loc,
		tag: tag,
	}
}

func runRestoreMethodCase(t *testing.T, fx restoreMethodFixture, tc restoreMethodTestCase) {
	t.Helper()

	if err := fx.h.dstCluster.rootSession.ExecStmt(fmt.Sprintf("TRUNCATE TABLE %q.%q", fx.ks, fx.tab)); err != nil {
		t.Fatal(err)
	}

	var (
		ensuredPathUsed atomic.Bool
		blockedPathUsed atomic.Bool
	)
	fx.h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if tc.ensuredPath != "" && strings.HasPrefix(req.URL.Path, tc.ensuredPath) {
			ensuredPathUsed.Store(true)
			for k, v := range tc.ensuredQueryParams {
				if got := req.URL.Query().Get(k); got != v {
					t.Errorf("Expected query param %q to be %q, got %q", k, v, got)
				}
			}
		}
		if tc.blockedPath != "" && strings.HasPrefix(req.URL.Path, tc.blockedPath) {
			blockedPathUsed.Store(true)
		}
		return nil, nil
	}))

	props := defaultTestProperties(fx.loc, fx.tag, true)
	props["keyspace"] = []string{fx.ks}
	if tc.method == "" {
		delete(props, "method")
	} else {
		props["method"] = tc.method
	}
	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(err)
	}

	if tc.expectTargetError {
		if _, _, _, err := fx.h.dstRestoreSvc.GetTargetUnitsViews(t.Context(), fx.h.dstCluster.ClusterID, rawProps); err == nil {
			t.Fatal("Expected GetTargetUnitsViews to fail")
		}
		return
	}

	fx.h.dstCluster.RunID = uuid.NewTime()
	if err := fx.h.dstRestoreSvc.Restore(t.Context(), fx.h.dstCluster.ClusterID, fx.h.dstCluster.TaskID, fx.h.dstCluster.RunID, rawProps); err != nil {
		t.Fatal(err)
	}

	if tc.ensuredPath != "" && !ensuredPathUsed.Load() {
		t.Fatalf("Expected SM to use %q API", tc.ensuredPath)
	}
	if tc.blockedPath != "" && blockedPathUsed.Load() {
		t.Fatalf("Expected SM not to use %q API", tc.blockedPath)
	}

	fx.h.validateIdenticalTables(t, []table{{ks: fx.ks, tab: fx.tab}})
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
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)

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
		restoreProps = defaultTestProperties(loc, tag, true)
		h.runRestore(t, restoreProps)

		t.Log("Validate: ", i)
		h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
	}
}
