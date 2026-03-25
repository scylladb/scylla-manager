// Copyright (C) 2022 ScyllaDB

//go:build all || integration

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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/jsonutil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

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
			tabs, err = client.Tables(r.Request.Context(), ks)
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
			err = client.RcloneListDirIter(r.Request.Context(), host, snapshotDir, nil, func(item *scyllaclient.RcloneListDirItem) {
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
					if err := client.RcloneMoveFile(r.Request.Context(), host, dst, src); err != nil {
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

// isRestoreSchemaFromCQLSupported returns true when the cluster supports
// restore schema from CQL (DESCRIBE SCHEMA WITH INTERNALS).
func isRestoreSchemaFromCQLSupported(t *testing.T, client *scyllaclient.Client) bool {
	return CheckAnyConstraint(t, client, ">= 6.0, < 2000", ">= 2024.2, > 1000")
}

// skipImpossibleSchemaTest skips the test if there
// is no way to restore schema for this cluster configuration.
func skipImpossibleSchemaTest(t *testing.T, client *scyllaclient.Client) {
	t.Helper()
	restoreSchemaFromSSTableSupport := IsRestoreSchemaFromSSTablesSupported(t.Context(), client)
	restoreSchemaFromCQLSupport := isRestoreSchemaFromCQLSupported(t, client)
	if restoreSchemaFromSSTableSupport != nil && !restoreSchemaFromCQLSupport {
		t.Skip("Given cluster configuration does not support schema restoration from neither SSTables or CQL")
	}
}

// skipCQLSchemaTestAssumingSSTables skips the test if it
// assumes that schema will be restored from SSTables,
// but it will actually be restored from CQL.
// This method might be helpful for tests using interceptors.
func skipCQLSchemaTestAssumingSSTables(t *testing.T, client *scyllaclient.Client) {
	t.Helper()
	if isRestoreSchemaFromCQLSupported(t, client) {
		t.Skip("This test assumes that schema is restored from SSTables, " +
			"but it is restored from CQL")
	}
}

func TestRestoreGetTargetUnitsViewsIntegration(t *testing.T) {
	// Tests GetTargetUnitsViews with valid inputs, validating that the
	// returned target, units, and views match the expected golden files
	// for various restore configurations (tables, schema, default values,
	// continue false).
	testCases := []struct {
		name        string
		description string
		input       string
		target      string
		units       string
		views       string
	}{
		{
			name:        "tables",
			description: "Restore tables with explicit keyspace filter",
			input:       "testdata/get_target/tables.input.json",
			target:      "testdata/get_target/tables.target.json",
			units:       "testdata/get_target/tables.units.json",
			views:       "testdata/get_target/tables.views.json",
		},
		{
			name:        "schema",
			description: "Restore schema with all keyspaces",
			input:       "testdata/get_target/schema.input.json",
			target:      "testdata/get_target/schema.target.json",
			units:       "testdata/get_target/schema.units.json",
			views:       "testdata/get_target/schema.views.json",
		},
		{
			name:        "default values",
			description: "Restore with default batch size and parallel values",
			input:       "testdata/get_target/default_values.input.json",
			target:      "testdata/get_target/default_values.target.json",
			units:       "testdata/get_target/default_values.units.json",
			views:       "testdata/get_target/default_values.views.json",
		},
		{
			name:        "continue false",
			description: "Restore with continue=false",
			input:       "testdata/get_target/continue_false.input.json",
			target:      "testdata/get_target/continue_false.target.json",
			units:       "testdata/get_target/continue_false.units.json",
			views:       "testdata/get_target/continue_false.views.json",
		},
	}

	h := newTestHelper(t, ManagedClusterHosts(), ManagedClusterHosts())

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
	loc := testLocation("get-target-units-views", "")
	S3InitBucket(t, loc.Path)

	WriteData(t, h.srcCluster.rootSession, testKs1, testBackupSize, testTable1, testTable2)
	WriteData(t, h.srcCluster.rootSession, testKs2, testBackupSize, testTable1, testTable2)

	var ignoreTarget []string
	var ignoreUnits []string
	var ignoredViews []string
	// Those tables have been migrated to system keyspace
	if CheckAnyConstraint(t, h.srcCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
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
	if CheckAnyConstraint(t, h.srcCluster.Client, "< 1000") {
		ignoreUnits = append(ignoreUnits, "system_replicated_keys")
	}
	// It's not possible to create views on tablet keyspaces
	rd := scyllaclient.NewRingDescriber(t.Context(), h.srcCluster.Client)
	if !rd.IsTabletKeyspace(testKs1) {
		CreateMaterializedView(t, h.srcCluster.rootSession, testKs1, testTable1, testMV)
	} else {
		ignoredViews = append(ignoredViews, testMV)
		ignoreTarget = append(ignoreTarget, "!"+testKs1+"."+testMV)
	}
	if !rd.IsTabletKeyspace(testKs2) {
		CreateSecondaryIndex(t, h.srcCluster.rootSession, testKs2, testTable2, testSI)
	} else {
		ignoredViews = append(ignoredViews, testSI)
		ignoreTarget = append(ignoreTarget, "!"+testKs2+"."+testSI+"_index")
	}

	tag := h.runBackup(t, defaultTestBackupProperties(loc, ""))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Log(tc.description)

			b, err := os.ReadFile(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			b = jsonutil.Set(b, "snapshot_tag", tag)
			b = jsonutil.Set(b, "location", []backupspec.Location{loc})

			var target Target
			if err = json.Unmarshal(b, &target); err != nil {
				t.Fatal(err)
			}
			if target.RestoreSchema {
				skipImpossibleSchemaTest(t, h.dstCluster.Client)
			}

			ctx := t.Context()
			target, units, views, err := h.dstRestoreSvc.GetTargetUnitsViews(ctx, h.dstCluster.ClusterID, b)
			if err != nil {
				t.Fatal(err)
			}
			if target.RestoreSchema {
				skipImpossibleSchemaTest(t, h.dstCluster.Client)
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
				cmpopts.IgnoreFields(Table{}, "TombstoneGC"),
				cmpopts.IgnoreSliceElements(func(v Unit) bool { return slices.Contains(ignoreUnits, v.Keyspace) }),
				cmpopts.IgnoreSliceElements(func(v Table) bool { return slices.Contains(ignoreUnits, v.Table) }),
			); diff != "" {
				t.Fatal(tc.units, diff)
			}
			for _, u := range units {
				for _, tab := range u.Tables {
					if tab.TombstoneGC == "" {
						t.Fatalf("Expected tombstone gc mode of %s.%s to be set", u.Keyspace, tab.Table)
					}
				}
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
	// Tests GetTargetUnitsViews with invalid inputs, validating that
	// appropriate errors are returned for various invalid configurations
	// (missing location, duplicated locations, invalid types, inaccessible
	// bucket, non-positive parallel/batch size, etc.).
	testCases := []struct {
		name          string
		description   string
		input         string
		error         string
		leaveTag      bool
		leaveLocation bool
	}{
		{
			name:          "missing location",
			description:   "Error when no location is specified",
			input:         "testdata/get_target/missing_location.input.json",
			error:         "missing location",
			leaveLocation: true,
		},
		{
			name:          "duplicated locations",
			description:   "Error when the same location is specified multiple times",
			input:         "testdata/get_target/duplicated_locations.input.json",
			error:         "specified multiple times",
			leaveLocation: true,
		},
		{
			name:        "restore both types",
			description: "Error when both restore_tables and restore_schema are true",
			input:       "testdata/get_target/restore_both_types.input.json",
			error:       "choose EXACTLY ONE restore type",
		},
		{
			name:        "restore no type",
			description: "Error when neither restore_tables nor restore_schema is set",
			input:       "testdata/get_target/restore_no_type.input.json",
			error:       "choose EXACTLY ONE restore type",
		},
		{
			name:        "schema and keyspace param",
			description: "Error when schema restore is combined with keyspace filter",
			input:       "testdata/get_target/schema_and_keyspace_param.input.json",
			error:       "no need to specify '--keyspace' flag",
		},
		{
			name:          "inaccessible bucket",
			description:   "Error when the specified S3 bucket does not exist",
			input:         "testdata/get_target/inaccessible_bucket.input.json",
			error:         "specified bucket does not exist",
			leaveLocation: true,
		},
		{
			name:        "non-positive parallel",
			description: "Error when parallel param is negative",
			input:       "testdata/get_target/non_positive_parallel.input.json",
			error:       "parallel param has to be greater or equal to zero",
		},
		{
			name:        "non-positive batch size",
			description: "Error when batch size param is negative",
			input:       "testdata/get_target/non_positive_batch_size.input.json",
			error:       "batch size param has to be greater or equal to zero",
		},
		{
			name:        "no data matching keyspace pattern",
			description: "Error when keyspace pattern does not match any data in backup",
			input:       "testdata/get_target/no_matching_keyspace.input.json",
			error:       "no data in backup locations match given keyspace pattern",
		},
		{
			name:        "incorrect snapshot tag",
			description: "Error when snapshot tag format is invalid",
			input:       "testdata/get_target/incorrect_snapshot_tag.input.json",
			error:       "not a Scylla Manager snapshot tag",
			leaveTag:    true,
		},
		{
			name:        "non-existing snapshot tag",
			description: "Error when snapshot tag does not exist in backup",
			input:       "testdata/get_target/non_existing_snapshot_tag.input.json",
			error:       "no snapshot with tag",
			leaveTag:    true,
		},
	}

	h := newTestHelper(t, ManagedClusterHosts(), ManagedClusterHosts())

	const (
		testKs1        = "ks1"
		testKs2        = "ks2"
		testTable1     = "table1"
		testTable2     = "table2"
		testBackupSize = 1
	)

	Print("Tables setup")
	loc := testLocation("get-target-error", "")
	S3InitBucket(t, loc.Path)

	WriteData(t, h.srcCluster.rootSession, testKs1, testBackupSize, testTable1, testTable2)
	WriteData(t, h.srcCluster.rootSession, testKs2, testBackupSize, testTable1, testTable2)

	tag := h.runBackup(t, defaultTestBackupProperties(loc, ""))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Log(tc.description)

			b, err := os.ReadFile(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.leaveTag {
				b = jsonutil.Set(b, "snapshot_tag", tag)
			}
			if !tc.leaveLocation {
				b = jsonutil.Set(b, "location", []backupspec.Location{loc})
			}

			ctx := t.Context()
			_, _, _, err = h.dstRestoreSvc.GetTargetUnitsViews(ctx, h.dstCluster.ClusterID, b)
			if err == nil || !strings.Contains(err.Error(), tc.error) {
				t.Fatalf("GetTargetUnitsViews() got %v, expected %v", err, tc.error)
			}

			t.Log("GetTargetUnitsViews(): ", err)
		})
	}
}

func TestRestoreGetUnitsErrorIntegration(t *testing.T) {
	// Tests GetTargetUnitsViews error paths when called with a valid backup
	// but invalid snapshot tag or non-matching keyspace pattern in the target.
	testCases := []struct {
		name        string
		description string
		modTarget   func(target Target) Target
	}{
		{
			name:        "non-existent snapshot tag",
			description: "Error when snapshot tag does not exist in backup",
			modTarget: func(target Target) Target {
				target.SnapshotTag = "sm_fake_snapshot_tagUTC"
				return target
			},
		},
		{
			name:        "no data matching keyspace pattern",
			description: "Error when keyspace pattern does not match any data in backup",
			modTarget: func(target Target) Target {
				target.Keyspace = []string{"fake_keyspace"}
				return target
			},
		},
	}

	h := newTestHelper(t, ManagedClusterHosts(), ManagedClusterHosts())

	ks := randomizedName("units_err_")
	loc := testLocation("get-units-error", "")
	S3InitBucket(t, loc.Path)

	Print("Table setup")
	WriteDataSecondClusterSchema(t, h.srcCluster.rootSession, ks, 0, 0)
	createTable(t, h.srcCluster.rootSession, ks, "tab1")
	fillTable(t, h.srcCluster.rootSession, 10, ks, "tab1")

	Print("Run backup")
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ks))

	target := Target{
		Location:      []backupspec.Location{loc},
		Keyspace:      []string{ks},
		SnapshotTag:   tag,
		RestoreTables: true,
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Log(tc.description)

			modifiedTarget := tc.modTarget(target)
			props, err := json.Marshal(modifiedTarget)
			if err != nil {
				t.Fatal(err)
			}

			ctx := t.Context()
			_, _, _, err = h.dstRestoreSvc.GetTargetUnitsViews(ctx, h.dstCluster.ClusterID, props)
			if err == nil {
				t.Fatal("GetTargetUnitsViews() expected error")
			}
			t.Log("GetTargetUnitsViews(): ", err)
		})
	}
}

func TestRestoreSmokeIntegration(t *testing.T) {
	// Tests basic backup and restore operations for both tables and schema
	// modes. Validates that data is correctly restored from one cluster to
	// another with proper permissions and tombstone_gc mode preservation.
	testCases := []struct {
		name          string
		description   string
		loadCnt       int
		loadSize      int
		batchSize     int
		parallel      int
		restoreTables bool
	}{
		{
			name:          "restore tables",
			description:   "Smoke test for restoring tables with multiple loads",
			loadCnt:       5,
			loadSize:      5,
			batchSize:     1,
			parallel:      0,
			restoreTables: true,
		},
		{
			name:          "restore schema",
			description:   "Smoke test for restoring schema",
			loadCnt:       1,
			loadSize:      1,
			batchSize:     2,
			parallel:      0,
			restoreTables: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Log(tc.description)

			h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

			loc := testLocation("smoke-"+strings.ReplaceAll(tc.name, " ", "-"), "")
			S3InitBucket(t, loc.Path)
			ks := randomizedName("smoke_")

			if !tc.restoreTables {
				skipImpossibleSchemaTest(t, h.dstCluster.Client)
			}

			Print("Prepare src data")
			prepareRestoreBackup(t, h, ks, tc.loadCnt, tc.loadSize)

			Print("Run backup")
			tag := h.runBackup(t, defaultTestBackupProperties(loc, ""))

			if tc.restoreTables {
				Print("Recreate schema on destination cluster")
				WriteDataSecondClusterSchema(t, h.dstCluster.rootSession, ks, 0, 0)
				grantRestoreTablesPermissions(t, h.dstCluster.rootSession, []string{ks}, h.dstUser)
			} else {
				grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
			}

			Print("Run restore")
			props := defaultTestProperties(loc, tag, tc.restoreTables)
			if tc.restoreTables {
				props["keyspace"] = []string{ks}
			}
			props["batch_size"] = tc.batchSize
			props["parallel"] = tc.parallel
			h.runRestore(t, props)

			Print("Validate restore")
			if !tc.restoreTables && !isRestoreSchemaFromCQLSupported(t, h.dstCluster.Client) {
				h.dstCluster.RestartScylla()
			}

			validateRestoreData(t, h, tc.restoreTables, []table{{ks: ks, tab: BigTableName}})
		})
	}
}

func TestRestoreTablesRestartAgentsIntegration(t *testing.T) {
	// Tests that restore can recover from agent restarts during data transfer.
	// Validates that even when agents are restarted mid-restore, the restore
	// completes successfully with correct data.
	t.Skip("Skip until we decide that we need to implement such mechanism " +
		"or that it's not needed and the test should be removed (#4589)")

	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	ks := randomizedName("restart_agents_")
	loc := testLocation("restart-agents", "")
	S3InitBucket(t, loc.Path)

	const (
		loadCnt  = 3
		loadSize = 1
	)

	Print("Prepare src data")
	prepareRestoreBackup(t, h, ks, loadCnt, loadSize)

	Print("Recreate schema on destination cluster")
	WriteDataSecondClusterSchema(t, h.dstCluster.rootSession, ks, 0, 0)

	Print("Run backup")
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ks))

	Print("Grant permissions")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, []string{ks}, h.dstUser)

	Print("Setup interceptor to restart agents mid-restore")
	var called atomic.Int64
	h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.Path, "/agent/rclone/sync/copypaths") && called.Add(1) == 1 {
			Print("And: agents are restarted")
			h.dstCluster.RestartAgents()
		}
		return nil, nil
	}))

	Print("Run restore")
	props := defaultTestProperties(loc, tag, true)
	props["keyspace"] = []string{ks}
	props["batch_size"] = 1
	props["parallel"] = 2
	h.runRestore(t, props)

	Print("Validate restore")
	validateRestoreData(t, h, true, []table{{ks: ks, tab: BigTableName}})
}

func TestRestoreResumeIntegration(t *testing.T) {
	// Tests that restore can be paused and resumed at different stages
	// (data loading, streaming, repair) and still complete successfully.
	// Also validates that materialized views backed up from older SM versions
	// are handled correctly during resume. Tests both continue=true and
	// continue=false modes.
	testCases := []struct {
		name        string
		description string
		continueVal bool
	}{
		{
			name:        "continue true",
			description: "Resume restore with continue=true preserves progress across runs",
			continueVal: true,
		},
		{
			name:        "continue false",
			description: "Resume restore with continue=false restarts from scratch but still completes",
			continueVal: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Log(tc.description)

			h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

			ks := randomizedName("resume_")
			loc := testLocation("resume-"+strings.ReplaceAll(tc.name, " ", "-"), "")
			S3InitBucket(t, loc.Path)

			const (
				loadCnt  = 5
				loadSize = 2
			)

			Print("Prepare src data")
			prepareRestoreBackup(t, h, ks, loadCnt, loadSize)

			Print("Recreate schema on destination cluster")
			WriteDataSecondClusterSchema(t, h.dstCluster.rootSession, ks, 0, 0)

			// It's not possible to create views on tablet keyspaces
			tabToValidate := []string{BigTableName}
			rd := scyllaclient.NewRingDescriber(t.Context(), h.srcCluster.Client)
			if !rd.IsTabletKeyspace(ks) {
				mv := "mv_resume"
				CreateMaterializedView(t, h.srcCluster.rootSession, ks, BigTableName, mv)
				CreateMaterializedView(t, h.dstCluster.rootSession, ks, BigTableName, mv)
				tabToValidate = append(tabToValidate, mv)
			}

			// Starting from SM 3.3.1, SM does not allow to back up views,
			// but backed up views should still be tested as older backups might
			// contain them. That's why here we manually force backup target
			// to contain the views.
			backupProps, err := json.Marshal(defaultTestBackupProperties(loc, ""))
			if err != nil {
				t.Fatal(err)
			}
			backupTarget, err := h.srcBackupSvc.GetTarget(t.Context(), h.srcCluster.ClusterID, backupProps)
			if err != nil {
				t.Fatal(err)
			}
			backupTarget.Units = []backup.Unit{
				{
					Keyspace:  ks,
					Tables:    tabToValidate,
					AllTables: true,
				},
			}
			h.srcCluster.RunID = uuid.NewTime()
			err = h.srcBackupSvc.Backup(t.Context(), h.srcCluster.ClusterID, h.srcCluster.TaskID, h.srcCluster.RunID, backupTarget)
			if err != nil {
				t.Fatal(err)
			}
			backupPr, err := h.srcBackupSvc.GetProgress(t.Context(), h.srcCluster.ClusterID, h.srcCluster.TaskID, h.srcCluster.RunID)
			if err != nil {
				t.Fatal(err)
			}
			tag := backupPr.SnapshotTag

			Print("Grant permissions")
			grantRestoreTablesPermissions(t, h.dstCluster.rootSession, []string{ks}, h.dstUser)

			props := defaultTestProperties(loc, tag, true)
			props["keyspace"] = []string{ks}
			props["batch_size"] = 1
			props["parallel"] = 3
			props["continue"] = tc.continueVal

			ctx1, cancel1 := context.WithCancel(t.Context())
			ctx2, cancel2 := context.WithCancel(t.Context())

			var lasCounter atomic.Int64
			var repairCounter atomic.Int64
			h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if isLasOrRestoreEndpoint(req.URL.Path) && lasCounter.Add(1) == 1 {
					Print("And: context1 is canceled")
					cancel1()
				}
				if strings.HasPrefix(req.URL.Path, "/storage_service/repair_async") && repairCounter.Add(1) == 1 {
					Print("And: context2 is canceled")
					cancel2()
				}
				if strings.HasPrefix(req.URL.Path, "/storage_service/tablets/repair") && repairCounter.Add(1) == 1 {
					Print("And: context2 is canceled")
					cancel2()
				}
				return nil, nil
			}))

			Print("When: run restore and stop it during load and stream")
			rawProps, err := json.Marshal(props)
			if err != nil {
				t.Fatal(err)
			}
			err = h.dstRestoreSvc.Restore(ctx1, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
			if err == nil {
				t.Fatal("Expected error on run but got nil")
			}
			if !strings.Contains(err.Error(), "context") {
				t.Fatalf("Expected context error but got: %+v", err)
			}

			Print("When: resume restore and stop it during repair")
			h.dstCluster.RunID = uuid.NewTime()
			err = h.dstRestoreSvc.Restore(ctx2, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
			if err == nil {
				t.Fatal("Expected error on run but got nil")
			}
			if !strings.Contains(err.Error(), "context") {
				t.Fatalf("Expected context error but got: %+v", err)
			}

			pr := h.getRestoreProgress(t)
			Printf("And: restore progress: %+#v\n", pr)
			if pr.RepairProgress == nil || pr.RepairProgress.Success == 0 {
				t.Fatal("Expected partial repair progress")
			}

			Print("When: resume restore and complete it")
			h.dstCluster.RunID = uuid.NewTime()
			err = h.dstRestoreSvc.Restore(t.Context(), h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
			if err != nil {
				t.Fatal("Unexpected error", err)
			}

			Print("Then: data is restored")
			validateRestoreData(t, h, true, []table{{ks: ks, tab: BigTableName}})
		})
	}
}

func TestRestoreVersionedIntegration(t *testing.T) {
	// Tests restore of backups containing versioned SSTables. Validates that
	// when SSTables are corrupted between backups creating versioned files,
	// the correct version of each SSTable is restored. Tests both tables
	// and schema restore with mixed UUID and integer SSTable IDs.
	testCases := []struct {
		name          string
		description   string
		restoreTables bool
	}{
		{
			name:          "restore tables",
			description:   "Restore tables from backup with versioned SSTables",
			restoreTables: true,
		},
		{
			name:          "restore schema",
			description:   "Restore schema from backup with versioned SSTables",
			restoreTables: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Log(tc.description)

			h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

			ks := randomizedName("versioned_")
			loc := testLocation("versioned-"+strings.ReplaceAll(tc.name, " ", "-"), "")
			S3InitBucket(t, loc.Path)

			const (
				loadCnt    = 5
				loadSize   = 1
				corruptCnt = 3
			)

			if !tc.restoreTables {
				skipImpossibleSchemaTest(t, h.dstCluster.Client)
				// Versioned files don't occur when restoring schema from CQL.
				skipCQLSchemaTestAssumingSSTables(t, h.dstCluster.Client)
			}

			ctx := t.Context()
			status, err := h.srcCluster.Client.Status(ctx)
			if err != nil {
				t.Fatal("Get status")
			}
			host := status[0]

			var corruptedKeyspace string
			var corruptedTable string
			if tc.restoreTables {
				corruptedKeyspace = ks
				corruptedTable = BigTableName
			} else {
				corruptedKeyspace = "system_schema"
				corruptedTable = "keyspaces"
			}

			// Force creation of integer SSTables in the snapshot dir,
			// as only integer SSTables can be versioned.
			// This also allows us to test scenario with mixed ID type SSTables.
			h.srcCluster.Hrt.SetRespInterceptor(newRenameSnapshotSSTablesRespInterceptor(h.srcCluster.Client, h.srcCluster.rootSession, halfUUIDToIntIDGen()))

			if tc.restoreTables {
				Print("Recreate schema on destination cluster")
				WriteDataSecondClusterSchema(t, h.dstCluster.rootSession, ks, 0, 0)
			} else {
				// This test requires SSTables in Scylla data dir to remain unchanged.
				// This is achieved by NullCompactionStrategy in user table, but since system tables
				// cannot be altered, it has to be handled separately.
				if err := h.srcCluster.Client.DisableAutoCompaction(ctx, host.Addr, corruptedKeyspace, corruptedTable); err != nil {
					t.Fatal(err)
				}
				defer h.srcCluster.Client.EnableAutoCompaction(ctx, host.Addr, corruptedKeyspace, corruptedTable)
			}

			Print("Prepare src data")
			prepareRestoreBackup(t, h, ks, loadCnt, loadSize)
			backupProps := defaultTestBackupProperties(loc, "")
			backupProps["method"] = backup.MethodAuto // This additionally validates fallback to rclone on versioned files creation
			simpleBackupWithProps(t, h, loc, backupProps)

			// Corrupting SSTables allows us to force the creation of versioned files
			Print("Choose SSTables to corrupt")
			remoteDir := loc.RemotePath(backupspec.RemoteSSTableDir(h.srcCluster.ClusterID, host.Datacenter, host.HostID, corruptedKeyspace, corruptedTable))
			opts := &scyllaclient.RcloneListDirOpts{
				Recurse:   true,
				FilesOnly: true,
			}

			var (
				firstCorrupt  []string
				bothCorrupt   []string
				secondCorrupt []string
			)

			err = h.srcCluster.Client.RcloneListDirIter(ctx, host.Addr, remoteDir, opts, func(item *scyllaclient.RcloneListDirItem) {
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
				parts := strings.Split(sstable, "-")
				if len(parts) < 2 {
					return sstable + "-Digest.crc32"
				}
				parts[len(parts)-1] = "Digest.crc32"
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
					if err = h.srcCluster.Client.RclonePut(ctx, host.Addr, file, body); err != nil {
						t.Fatalf("Corrupt remote file %s", file)
					}
				}

				Print("Backup with corrupted SSTables in remote location")
				tag := simpleBackupWithProps(t, h, loc, backupProps)

				Print("Validate creation of versioned files in remote location")
				for _, tc := range toCorrupt {
					corruptedPath := path.Join(remoteDir, tc) + backup.VersionedFileExt(tag)
					if _, err = h.srcCluster.Client.RcloneFileInfo(ctx, host.Addr, corruptedPath); err != nil {
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

				if err = h.srcCluster.Client.RcloneMoveFile(ctx, host.Addr, tmp, newest); err != nil {
					t.Fatal(err)
				}
				if err = h.srcCluster.Client.RcloneMoveFile(ctx, host.Addr, newest, versioned); err != nil {
					t.Fatal(err)
				}
				if err = h.srcCluster.Client.RcloneMoveFile(ctx, host.Addr, versioned, tmp); err != nil {
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

			Print("Grant permissions")
			if tc.restoreTables {
				grantRestoreTablesPermissions(t, h.dstCluster.rootSession, []string{ks}, h.dstUser)
			} else {
				grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
			}

			Print("Restore 3-rd backup with versioned files")
			restoreProps := defaultTestProperties(loc, tag3, tc.restoreTables)
			if tc.restoreTables {
				restoreProps["keyspace"] = []string{ks}
			}
			restoreProps["batch_size"] = 1
			restoreProps["parallel"] = 0
			restoreProps["method"] = MethodAuto

			rawRestoreProps, err := json.Marshal(restoreProps)
			if err != nil {
				t.Fatal(err)
			}

			h.dstCluster.RunID = uuid.NewTime()
			if err = h.dstRestoreSvc.Restore(ctx, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawRestoreProps); err != nil {
				t.Fatal(err)
			}

			Print("Validate restore")
			if !tc.restoreTables && !isRestoreSchemaFromCQLSupported(t, h.dstCluster.Client) {
				h.dstCluster.RestartScylla()
			}
			validateRestoreData(t, h, tc.restoreTables, []table{{ks: ks, tab: BigTableName}})
		})
	}
}

const (
	mvName      = "testmv"
	siName      = "bydata"
	siTableName = "bydata_index"
)

func TestRestoreTablesViewCQLSchemaIntegration(t *testing.T) {
	// Tests that materialized views and secondary indexes are correctly
	// restored when restoring tables with CQL-based schema. Validates that
	// views are restored even when they are not explicitly included in
	// the keyspace filter.
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	ks := randomizedName("view_cql_")
	loc := testLocation("view-cql-schema", "")
	S3InitBucket(t, loc.Path)

	const (
		loadCnt  = 4
		loadSize = 5
	)

	Print("Create src table with MV and SI")
	prepareRestoreBackup(t, h, ks, loadCnt, loadSize)

	rd := scyllaclient.NewRingDescriber(t.Context(), h.srcCluster.Client)
	if rd.IsTabletKeyspace(ks) {
		t.Skip("Test expects to create views, but it's not possible for tablet keyspaces")
	}

	CreateMaterializedView(t, h.srcCluster.rootSession, ks, BigTableName, mvName)
	CreateSecondaryIndex(t, h.srcCluster.rootSession, ks, BigTableName, siName)

	Print("Recreate dst schema from CQL")
	WriteDataSecondClusterSchema(t, h.dstCluster.rootSession, ks, 0, 0, BigTableName)
	CreateMaterializedView(t, h.dstCluster.rootSession, ks, BigTableName, mvName)
	CreateSecondaryIndex(t, h.dstCluster.rootSession, ks, BigTableName, siName)
	time.Sleep(5 * time.Second)

	Print("Run backup")
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ""))

	Print("Grant permissions")
	// Check whether view will be restored even when it's not included
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, []string{ks + "." + BigTableName}, h.dstUser)

	Print("Run restore")
	props := defaultTestProperties(loc, tag, true)
	props["keyspace"] = []string{ks + "." + BigTableName}
	props["batch_size"] = 1
	props["parallel"] = 0
	h.runRestore(t, props)

	Print("Validate restore")
	validateRestoreData(t, h, true, []table{
		{ks: ks, tab: BigTableName},
		{ks: ks, tab: mvName},
		{ks: ks, tab: siTableName},
	})
}

func TestRestoreFullViewSSTableSchemaIntegration(t *testing.T) {
	// Tests full schema + tables restore with SSTable-based schema restoration
	// that includes materialized views and secondary indexes. First restores
	// schema, then restores tables, validating views are correctly created
	// and populated at each stage.
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	skipImpossibleSchemaTest(t, h.dstCluster.Client)

	ks := randomizedName("view_sstable_")
	loc := testLocation("view-sstable-schema", "")
	S3InitBucket(t, loc.Path)

	const (
		loadCnt  = 4
		loadSize = 5
	)

	Print("Create src table with MV and SI")
	prepareRestoreBackup(t, h, ks, loadCnt, loadSize)

	rd := scyllaclient.NewRingDescriber(t.Context(), h.srcCluster.Client)
	if rd.IsTabletKeyspace(ks) {
		t.Skip("Test expects to create views, but it's not possible for tablet keyspaces")
	}

	CreateMaterializedView(t, h.srcCluster.rootSession, ks, BigTableName, mvName)
	CreateSecondaryIndex(t, h.srcCluster.rootSession, ks, BigTableName, siName)
	time.Sleep(5 * time.Second)

	Print("Run backup")
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ""))

	toValidate := []table{
		{ks: ks, tab: BigTableName},
		{ks: ks, tab: mvName},
		{ks: ks, tab: siTableName},
	}

	Print("Restore schema")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	schemaProps := defaultTestProperties(loc, tag, false)
	h.runRestore(t, schemaProps)

	Print("Validate schema restore")
	if !isRestoreSchemaFromCQLSupported(t, h.dstCluster.Client) {
		h.dstCluster.RestartScylla()
	}
	validateRestoreData(t, h, false, toValidate)

	Print("Grant minimal user permissions for restore tables")
	// Reset user permissions after schema restore
	dropNonSuperUsers(t, h.dstCluster.rootSession)
	createUser(t, h.dstCluster.rootSession, h.dstUser, h.dstPass)
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, []string{ks + "." + BigTableName}, h.dstUser)

	Print("Restore tables")
	h.dstCluster.TaskID = uuid.NewTime()
	h.dstCluster.RunID = uuid.NewTime()
	tablesProps := defaultTestProperties(loc, tag, true)
	tablesProps["keyspace"] = []string{ks + "." + BigTableName}
	tablesProps["batch_size"] = 1
	tablesProps["parallel"] = 0
	h.runRestore(t, tablesProps)

	Print("Validate tables restore")
	validateRestoreData(t, h, true, toValidate)
}

func TestRestoreFullIntegration(t *testing.T) {
	// Tests full cluster restore (schema + tables) including system tables
	// (system_auth, system_traces, system_distributed), user keyspaces,
	// materialized views, and secondary indexes. Validates that all data
	// is correctly restored from one cluster to another with proper user
	// permissions.
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	skipImpossibleSchemaTest(t, h.dstCluster.Client)

	ks := randomizedName("full_")
	loc := testLocation("full-restore", "")
	S3InitBucket(t, loc.Path)

	const (
		loadCnt  = 2
		loadSize = 1
	)

	// Ensure clean scylla tables
	if err := cleanScyllaTables(t, h.srcCluster.rootSession, h.srcCluster.Client); err != nil {
		t.Fatal(err)
	}
	if err := cleanScyllaTables(t, h.dstCluster.rootSession, h.dstCluster.Client); err != nil {
		t.Fatal(err)
	}

	Print("Prepare src data with features")
	prepareRestoreBackupWithFeatures(t, h, ks, loadCnt, loadSize)

	Print("Run backup")
	tag := h.runBackup(t, defaultTestBackupProperties(loc, ""))

	Print("Restore schema on different cluster")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	schemaProps := defaultTestProperties(loc, tag, false)
	schemaProps["batch_size"] = 1
	schemaProps["parallel"] = 3
	h.runRestore(t, schemaProps)

	toValidate := []table{
		{ks: "system_traces", tab: "events"},
		{ks: "system_traces", tab: "node_slow_log"},
		{ks: "system_traces", tab: "node_slow_log_time_idx"},
		{ks: "system_traces", tab: "sessions"},
		{ks: "system_traces", tab: "sessions_time_idx"},
	}
	rd := scyllaclient.NewRingDescriber(t.Context(), h.srcCluster.Client)
	if !rd.IsTabletKeyspace(ks) {
		toValidate = append(toValidate,
			table{ks: ks, tab: BigTableName},
			table{ks: ks, tab: mvName},
			table{ks: ks, tab: siTableName},
		)
	}
	if !CheckAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		toValidate = append(toValidate,
			table{ks: "system_auth", tab: "role_attributes"},
			table{ks: "system_auth", tab: "role_members"},
			table{ks: "system_auth", tab: "role_permissions"},
			table{ks: "system_auth", tab: "roles"},
			table{ks: "system_distributed", tab: "service_levels"},
		)
	}

	if !isRestoreSchemaFromCQLSupported(t, h.dstCluster.Client) {
		h.dstCluster.RestartScylla()
	}
	validateRestoreData(t, h, false, toValidate)

	Print("Restore tables on different cluster")
	// Reset user permissions after schema restore
	dropNonSuperUsers(t, h.dstCluster.rootSession)
	createUser(t, h.dstCluster.rootSession, h.dstUser, h.dstPass)
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, nil, h.dstUser)
	h.dstCluster.TaskID = uuid.NewTime()
	h.dstCluster.RunID = uuid.NewTime()
	tablesProps := defaultTestProperties(loc, tag, true)
	tablesProps["batch_size"] = 1
	tablesProps["parallel"] = 3
	h.runRestore(t, tablesProps)

	validateRestoreData(t, h, true, toValidate)
}

// prepareRestoreBackup populates src cluster with loadCnt * loadSize MiB of data
// living in keyspace.big_table table. It writes loadCnt batches (each containing
// loadSize Mib of data), flushing inserted data into SSTables after each.
// It also disables compaction for keyspace.big_table so that SSTables flushed
// into memory are not merged together.
func prepareRestoreBackup(t *testing.T, h *testHelper, keyspace string, loadCnt, loadSize int) {
	t.Helper()

	// Create keyspace and table
	WriteDataSecondClusterSchema(t, h.srcCluster.rootSession, keyspace, 0, 0)

	var startingID int
	for i := 0; i < loadCnt; i++ {
		Printf("When: Write load nr %d to src cluster", i)
		startingID = WriteDataSecondClusterSchema(t, h.srcCluster.rootSession, keyspace, startingID, loadSize)
		FlushTable(t, h.srcCluster.Client, h.srcCluster.Client.Config().Hosts, keyspace, BigTableName)
	}
}

// prepareRestoreBackupWithFeatures is a wrapper over prepareRestoreBackup that:
// - adds materialized view and secondary index (for vnode keyspace only)
// - adds CDC log table
// - populates system_auth, system_traces, system_distributed tables
func prepareRestoreBackupWithFeatures(t *testing.T, h *testHelper, keyspace string, loadCnt, loadSize int) {
	t.Helper()

	statements := []string{
		"CREATE ROLE role1 WITH PASSWORD = 'pas' AND LOGIN = true",
		"CREATE SERVICE LEVEL sl WITH timeout = 500ms AND workload_type = 'interactive'",
		"ATTACH SERVICE_LEVEL sl TO role1",
		"GRANT SELECT ON system_schema.tables TO role1",
		"CREATE ROLE role2",
		"GRANT role1 TO role2",
	}

	tr := gocql.NewTraceWriter(h.srcCluster.rootSession.Session, os.Stdout) // Populate system_traces
	for _, stmt := range statements {
		if err := h.srcCluster.rootSession.Query(stmt, nil).Trace(tr).Exec(); err != nil {
			t.Fatalf("Exec stmt: %s, error: %s", stmt, err.Error())
		}
	}

	// Create keyspace and table
	WriteDataSecondClusterSchema(t, h.srcCluster.rootSession, keyspace, 0, 0)

	rd := scyllaclient.NewRingDescriber(t.Context(), h.srcCluster.Client)
	if !rd.IsTabletKeyspace(keyspace) {
		// CDC does not work with tablets
		ExecStmt(t, h.srcCluster.rootSession,
			fmt.Sprintf("ALTER TABLE %s.%s WITH cdc = {'enabled': 'true', 'preimage': 'true'}", keyspace, BigTableName),
		)
		// It's not possible to create views on tablet keyspaces
		CreateMaterializedView(t, h.srcCluster.rootSession, keyspace, BigTableName, mvName)
		CreateSecondaryIndex(t, h.srcCluster.rootSession, keyspace, BigTableName, siName)
	}

	prepareRestoreBackup(t, h, keyspace, loadCnt, loadSize)
}

// simpleBackupWithProps runs a backup with the given properties and returns the snapshot tag.
func simpleBackupWithProps(t *testing.T, h *testHelper, loc backupspec.Location, props map[string]any) string {
	t.Helper()

	ctx := t.Context()
	h.srcCluster.RunID = uuid.NewTime()

	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(err)
	}

	target, err := h.srcBackupSvc.GetTarget(ctx, h.srcCluster.ClusterID, rawProps)
	if err != nil {
		t.Fatal(err)
	}

	Print("When: backup cluster")
	if err = h.srcBackupSvc.Backup(ctx, h.srcCluster.ClusterID, h.srcCluster.TaskID, h.srcCluster.RunID, target); err != nil {
		t.Fatalf("Couldn't backup cluster: %s", err)
	}

	Print("When: list newly created backup")
	items, err := h.srcBackupSvc.List(ctx, h.srcCluster.ClusterID, []backupspec.Location{loc}, backup.ListFilter{
		ClusterID: h.srcCluster.ClusterID,
		TaskID:    h.srcCluster.TaskID,
	})
	if err != nil {
		t.Fatalf("Couldn't list backup: %s", err)
	}
	if len(items) != 1 {
		t.Fatalf("List() = %v, expected one item", items)
	}
	i := items[0]
	Print(fmt.Sprintf("Then: backup snapshot info: %v", i.SnapshotInfo))

	return i.SnapshotInfo[0].SnapshotTag
}

// cleanScyllaTables truncates scylla tables populated in prepareRestoreBackupWithFeatures.
func cleanScyllaTables(t *testing.T, session gocqlx.Session, client *scyllaclient.Client) error {
	t.Helper()

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

// validateRestoreData validates that restored data matches source data by comparing
// row counts and tombstone_gc modes.
func validateRestoreData(t *testing.T, h *testHelper, restoreTables bool, tables []table) {
	t.Helper()

	Print("Validate tombstone_gc mode")
	for _, tab := range tables {
		// Don't validate views tombstone_gc
		if baseTable(t, h.srcCluster.rootSession, tab.ks, tab.tab) != "" {
			continue
		}
		srcMode := tombstoneGCMode(t, h.srcCluster.rootSession, tab.ks, tab.tab)
		dstMode := tombstoneGCMode(t, h.dstCluster.rootSession, tab.ks, tab.tab)
		if srcMode != dstMode {
			t.Fatalf("Expected %s tombstone_gc mode, got: %s", srcMode, dstMode)
		}
	}

	Print("Validate row counts")
	for _, tab := range tables {
		dstCnt := rowCount(t, h.dstCluster.rootSession, tab.ks, tab.tab)
		srcCnt := 0
		if restoreTables {
			srcCnt = rowCount(t, h.srcCluster.rootSession, tab.ks, tab.tab)
		}

		t.Logf("%s.%s, srcCount = %d, dstCount = %d", tab.ks, tab.tab, srcCnt, dstCnt)
		if dstCnt != srcCnt {
			// Destination cluster has additional users used for restore
			if tab.ks == "system_auth" {
				if restoreTables && dstCnt < srcCnt {
					t.Fatalf("%s.%s: srcCount != dstCount", tab.ks, tab.tab)
				}
				continue
			}
			t.Fatalf("srcCount != dstCount")
		}
	}

	Print("Validate restore progress")
	pr := h.getRestoreProgress(t)
	Printf("TOTAL %v %v %v", pr.Downloaded, pr.Size, pr.Restored)
	for _, kpr := range pr.Keyspaces {
		for _, tpr := range kpr.Tables {
			Printf("name %s %v %v %v", tpr.Table, tpr.Downloaded, tpr.Size, tpr.Restored)
			if tpr.Size != tpr.Restored {
				t.Fatalf("Expected complete table restore (%s)", tpr.Table)
			}
		}
		if kpr.Size != kpr.Restored {
			t.Fatalf("Expected complete keyspace restore (%s)", kpr.Keyspace)
		}
	}
	if pr.Size != pr.Restored {
		t.Fatal("Expected complete restore")
	}
}
