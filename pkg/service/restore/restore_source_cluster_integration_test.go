// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package restore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TestRestoreSourceClusterIntegration tests the 'source_cluster_id' restore property.
// Backups of multiple clusters can be stored in the same location and share the snapshot tag.
// Without the property, restore reads backup files of all those clusters.
// With the property, restore reads only backup files (manifests and schema file) of the given cluster,
// and fails as for an unknown snapshot tag when the cluster has no backup with the tag.
func TestRestoreSourceClusterIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	ks := randomizedName("source_cluster_ks_")
	tab := randomizedName("tab_")
	loc := testLocation("source-cluster", "")
	tag := setupSourceClusterBackup(t, h, loc, ks, tab)
	setupForeignClusterBackupFiles(t, h, loc, tag, uuid.NewTime())

	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, []string{ks}, h.dstUser)

	testCases := []struct {
		name            string
		description     string
		restoreTables   bool
		sourceClusterID uuid.UUID
		errMsg          string
	}{
		{
			name:          "tables without source cluster",
			description:   "restore reads the broken manifest of the other cluster sharing the tag",
			restoreTables: true,
			errMsg:        "init units",
		},
		{
			name:          "schema without source cluster",
			description:   "restore finds the schema file of the other cluster sharing the tag",
			restoreTables: false,
			errMsg:        "multiple cql schema files",
		},
		{
			name:            "tables with source cluster",
			description:     "restore reads only the manifests of the source cluster",
			restoreTables:   true,
			sourceClusterID: h.srcCluster.ClusterID,
		},
		{
			name:            "schema with source cluster",
			description:     "restore reads only the manifests and the schema file of the source cluster",
			restoreTables:   false,
			sourceClusterID: h.srcCluster.ClusterID,
		},
		{
			name:            "unknown source cluster",
			description:     "restore fails as for an unknown snapshot tag",
			restoreTables:   true,
			sourceClusterID: uuid.NewTime(),
			errMsg:          "no snapshot with tag",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			Printf("Validate restore target: %s", tc.description)
			props := sourceClusterRestoreProperties(loc, tag, ks, tc.restoreTables, tc.sourceClusterID)
			rawProps, err := json.Marshal(props)
			if err != nil {
				t.Fatal(err)
			}
			target, units, _, err := h.dstRestoreSvc.GetTargetUnitsViews(context.Background(), h.dstCluster.ClusterID, rawProps)
			if tc.errMsg != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errMsg) {
					t.Fatalf("Expected error containing %q, got: %v", tc.errMsg, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if target.SourceClusterID != tc.sourceClusterID {
				t.Fatalf("Expected source cluster %s in target, got %s", tc.sourceClusterID, target.SourceClusterID)
			}
			if len(units) == 0 {
				t.Fatal("Expected restore units")
			}
		})
	}

	t.Run("restore tables with source cluster", func(t *testing.T) {
		Print("When: restore tables with source cluster")
		h.runRestore(t, sourceClusterRestoreProperties(loc, tag, ks, true, h.srcCluster.ClusterID))

		Print("Then: restored table is identical to the backed up one")
		h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
	})
}

// setupSourceClusterBackup creates and fills table in src cluster, creates its schema in dst cluster,
// and backs it up to the location. It returns the snapshot tag.
func setupSourceClusterBackup(t *testing.T, h *testHelper, loc backupspec.Location, ks, tab string) string {
	Print("Given: table in both clusters, filled in src cluster")
	ksStmt := fmt.Sprintf("CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}", ks)
	tabStmt := fmt.Sprintf("CREATE TABLE %q.%q (id int PRIMARY KEY, data int)", ks, tab)
	ExecStmt(t, h.srcCluster.rootSession, ksStmt)
	ExecStmt(t, h.srcCluster.rootSession, tabStmt)
	ExecStmt(t, h.dstCluster.rootSession, ksStmt)
	ExecStmt(t, h.dstCluster.rootSession, tabStmt)
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab)

	Print("And: backup of src cluster")
	InitBucket(t, loc.Path)
	return h.runBackup(t, defaultTestBackupProperties(loc, ks))
}

// setupForeignClusterBackupFiles uploads backup files of another cluster with the same snapshot tag
// to the location. They are broken on purpose, so that restore fails when it reads them.
func setupForeignClusterBackupFiles(t *testing.T, h *testHelper, loc backupspec.Location, tag string, clusterID uuid.UUID) {
	Printf("And: broken backup files of cluster %s with the same snapshot tag", clusterID)
	ctx := context.Background()
	host := h.srcCluster.Client.Config().Hosts[0]
	taskID := uuid.NewTime()
	files := []string{
		backupspec.RemoteManifestFile(clusterID, taskID, tag, "dc1", uuid.NewTime().String()),
		backupspec.RemoteSchemaFile(clusterID, taskID, tag),
	}
	for _, f := range files {
		if err := h.srcCluster.Client.RclonePut(ctx, host, loc.RemotePath(f), bytes.NewBufferString("broken backup file")); err != nil {
			t.Fatal(err)
		}
	}
}

func sourceClusterRestoreProperties(loc backupspec.Location, tag, ks string, restoreTables bool, sourceClusterID uuid.UUID) map[string]any {
	props := defaultTestProperties(loc, tag, restoreTables)
	if restoreTables {
		props["keyspace"] = []string{ks}
	}
	if sourceClusterID != uuid.Nil {
		props["source_cluster_id"] = sourceClusterID.String()
	}
	return props
}
