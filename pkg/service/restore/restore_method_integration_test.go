// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package restore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
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
	h        *testHelper
	ctx      context.Context
	ks       string
	tab      string
	loc      backupspec.Location
	tag      string
	ksFilter []string
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

type restoreMethodEndpointUsage struct {
	ensured atomic.Bool
	blocked atomic.Bool
}

func TestRestoreTablesMethodIntegration(t *testing.T) {
	// This test validates that the correct API is used
	// for restoring batches (Rclone or Scylla).
	fx := setupRestoreMethodFixture(t)

	ni, err := fx.h.dstCluster.Client.AnyNodeInfo(fx.ctx)
	if err != nil {
		t.Fatal(err)
	}
	nativeAPIExposed, err := version.CheckConstraint(ni.ScyllaVersion, ">= 2025.2")
	if err != nil {
		t.Fatal(err)
	}
	nativeAPISupported, err := ni.SupportsNativeRestoreAPI()
	if err != nil {
		t.Fatal(err)
	}
	canUseNative := !IsIPV6Network()
	nativeQueryParams := map[string]string{
		"scope":                "all",
		"primary_replica_only": "true",
	}

	testCases := []restoreMethodTestCase{
		{
			name:              "method=rclone uses rclone API",
			description:       "Verifies that explicit rclone method restores data via the Rclone copypaths API and does not touch the native restore endpoint.",
			method:            restore.MethodRclone,
			ensuredPath:       rcloneRestoreAPIPath,
			blockedPath:       nativeRestoreAPIPath,
			expectTargetError: false,
		},
		{
			name:              "method=default uses rclone API",
			description:       "Verifies that omitting the method keeps the default restore behavior on the Rclone API.",
			method:            "",
			ensuredPath:       rcloneRestoreAPIPath,
			blockedPath:       nativeRestoreAPIPath,
			expectTargetError: false,
		},
	}

	if nativeAPISupported && canUseNative {
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
			description:       "Verifies that auto mode falls back to Rclone when native restore cannot be used on this environment.",
			method:            restore.MethodAuto,
			ensuredPath:       rcloneRestoreAPIPath,
			blockedPath:       nativeRestoreAPIPath,
			expectTargetError: false,
		})
	}

	if nativeAPIExposed && canUseNative {
		testCases = append(testCases, restoreMethodTestCase{
			name:               "method=native uses native API when exposed",
			description:        "Verifies that explicit native method uses the native restore endpoint when the API is exposed and available.",
			method:             restore.MethodNative,
			ensuredPath:        nativeRestoreAPIPath,
			blockedPath:        rcloneRestoreAPIPath,
			ensuredQueryParams: nativeQueryParams,
			expectTargetError:  false,
		})
	} else {
		testCases = append(testCases, restoreMethodTestCase{
			name:              "method=native target validation fails when unavailable",
			description:       "Verifies that explicit native method is rejected during target validation when the environment does not expose a usable native restore API.",
			method:            restore.MethodNative,
			expectTargetError: true,
		})
	}

	for _, tc := range testCases {
		tc := tc
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
	S3InitBucket(t, loc.Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ks))
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)

	return restoreMethodFixture{
		h:        h,
		ctx:      context.Background(),
		ks:       ks,
		tab:      tab,
		loc:      loc,
		tag:      tag,
		ksFilter: ksFilter,
	}
}

func runRestoreMethodCase(t *testing.T, fx restoreMethodFixture, tc restoreMethodTestCase) {
	t.Helper()

	if err := fx.h.dstCluster.rootSession.ExecStmt(fmt.Sprintf("TRUNCATE TABLE %q.%q", fx.ks, fx.tab)); err != nil {
		t.Fatal(err)
	}

	usage := &restoreMethodEndpointUsage{}
	fx.h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if tc.ensuredPath != "" && strings.HasPrefix(req.URL.Path, tc.ensuredPath) {
			usage.ensured.Store(true)
			for k, v := range tc.ensuredQueryParams {
				if got := req.URL.Query().Get(k); got != v {
					t.Errorf("Expected query param %q to be %q, got %q", k, v, got)
				}
			}
		}
		if tc.blockedPath != "" && strings.HasPrefix(req.URL.Path, tc.blockedPath) {
			usage.blocked.Store(true)
		}
		return nil, nil
	}))

	rawProps, err := makeRestoreMethodRawProps(fx, tc.method)
	if err != nil {
		t.Fatal(err)
	}

	if tc.expectTargetError {
		if _, _, _, err := fx.h.dstRestoreSvc.GetTargetUnitsViews(fx.ctx, fx.h.dstCluster.ClusterID, rawProps); err == nil {
			t.Fatal("Expected GetTargetUnitsViews to fail")
		}
		return
	}

	fx.h.dstCluster.RunID = uuid.NewTime()
	if err := fx.h.dstRestoreSvc.Restore(fx.ctx, fx.h.dstCluster.ClusterID, fx.h.dstCluster.TaskID, fx.h.dstCluster.RunID, rawProps); err != nil {
		t.Fatal(err)
	}

	assertRestoreMethodEndpointUsage(t, tc, usage)
	fx.h.validateIdenticalTables(t, []table{{ks: fx.ks, tab: fx.tab}})
}

func makeRestoreMethodRawProps(fx restoreMethodFixture, method restore.Method) ([]byte, error) {
	props := defaultTestProperties(fx.loc, fx.tag, true)
	props["keyspace"] = fx.ksFilter
	if method == "" {
		delete(props, "method")
	} else {
		props["method"] = method
	}
	return json.Marshal(props)
}

func assertRestoreMethodEndpointUsage(t *testing.T, tc restoreMethodTestCase, usage *restoreMethodEndpointUsage) {
	t.Helper()

	if tc.ensuredPath != "" && !usage.ensured.Load() {
		t.Fatalf("Expected SM to use %q API", tc.ensuredPath)
	}
	if tc.blockedPath != "" && usage.blocked.Load() {
		t.Fatalf("Expected SM not to use %q API", tc.blockedPath)
	}
}
