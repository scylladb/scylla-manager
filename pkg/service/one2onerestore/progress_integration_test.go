// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package one2onerestore

import (
	"context"
	"os"
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestGetProgressIntegration(t *testing.T) {
	if tablets := os.Getenv("TABLETS"); tablets == "enabled" {
		t.Skip("1-1-restore is available only for v-nodes")
	}
	loc := backupspec.Location{
		Provider: backupspec.S3,
		Path:     "my-1-1-restore-test",
	}
	testutils.S3InitBucket(t, loc.Path)
	w, _ := newTestWorker(t, testconfig.ManagedClusterHosts())
	h := newTestHelper(t, testconfig.ManagedClusterHosts())
	w.runInfo = struct{ ClusterID, TaskID, RunID uuid.UUID }{
		ClusterID: h.clusterID,
		TaskID:    h.taskID,
		RunID:     h.runID,
	}
	clusterSession := db.CreateSessionAndDropAllKeyspaces(t, h.client)
	ksName := "testgetprogress"
	db.WriteData(t, clusterSession, ksName, 10)
	mvName := "testmv"
	db.CreateMaterializedView(t, clusterSession, ksName, db.BigTableName, mvName)

	if srcCnt := rowCount(t, clusterSession, ksName, db.BigTableName); srcCnt == 0 {
		t.Fatalf("Unexpected row count in table: 0")
	}

	snapshotTag := h.runBackup(t, map[string]any{
		"location": []backupspec.Location{loc},
	})

	target := Target{
		Keyspace:        []string{ksName},
		SourceClusterID: h.clusterID,
		SnapshotTag:     snapshotTag,
		Location:        []backupspec.Location{loc},
		NodesMapping:    getNodeMappings(t, w.client),
	}
	manifests, hosts, err := w.getAllSnapshotManifestsAndTargetHosts(context.Background(), target)
	if err != nil {
		t.Fatalf("Unexpected err, getAllSnapshotManifestsAndTargetHosts: %v", err)
	}

	workload, err := w.prepareHostWorkload(context.Background(), manifests, hosts, target)
	if err != nil {
		t.Fatalf("Unexpected err, prepareHostWorkload: %v", err)
	}
	pr, err := w.getProgress(context.Background())
	if err != nil {
		t.Fatalf("Unexpected err, getProgress: %v", err)
	}
	if !progressIsEmpty(pr) {
		t.Fatalf("Expected empty progress, but got: %v", pr)
	}

	w.initProgress(context.Background(), workload)

	pr, err = w.getProgress(context.Background())
	if err != nil {
		t.Fatalf("Unexpected err, getProgress: %v", err)
	}
	if progressIsEmpty(pr) {
		t.Fatalf("Expected not empty progress, but got empty")
	}
	expectProgressStatus(t, pr, ProgressStatusNotStarted)

	testutils.Print("ALTER_TGC Stage")
	err = w.setTombstoneGCModeRepair(context.Background(), workload)
	if err != nil {
		t.Fatalf("Unexpected err, setTombstoneGCModeRepair: %v", err)
	}

	testutils.Print("DROP_VIEWS Stage")
	views, err := w.dropViews(context.Background(), workload)
	if err != nil {
		t.Fatalf("Unexpected err, dropViews: %v", err)
	}

	testutils.Print("DATA Stage")
	if err := w.restoreTables(context.Background(), workload, target.Keyspace); err != nil {
		t.Fatalf("Unexpected err, restoreTables: %v", err)
	}

	pr, err = w.getProgress(context.Background())
	if err != nil {
		t.Fatalf("Unexpected err, getProgress: %v", err)
	}
	if progressIsEmpty(pr) {
		t.Fatalf("Expected not empty progress, but got empty")
	}
	expectTablesStatus(t, pr.Tables, ProgressStatusDone)
	expectViewsStatus(t, pr.Views, ProgressStatusNotStarted)

	testutils.Print("RECREATE_VIEWS Stage")
	if err := w.reCreateViews(context.Background(), views); err != nil {
		t.Fatalf("Unexpected err, reCreateViews: %v", err)
	}

	pr, err = w.getProgress(context.Background())
	if err != nil {
		t.Fatalf("Unexpected err, getProgress: %v", err)
	}
	if progressIsEmpty(pr) {
		t.Fatalf("Expected not empty progress, but got empty")
	}
	expectProgressStatus(t, pr, ProgressStatusDone)
}

func progressIsEmpty(pr Progress) bool {
	return len(pr.Tables) == 0 && len(pr.Views) == 0
}

func expectProgressStatus(t *testing.T, pr Progress, status ProgressStatus) {
	t.Helper()
	expectTablesStatus(t, pr.Tables, status)
	expectViewsStatus(t, pr.Views, status)
}

func expectTablesStatus(t *testing.T, tables []TableProgress, status ProgressStatus) {
	t.Helper()
	for _, tp := range tables {
		if tp.Status != status {
			t.Fatalf("Expected table %q status, but got %q", status, tp.Status)
		}
	}
}

func expectViewsStatus(t *testing.T, views []ViewProgress, status ProgressStatus) {
	t.Helper()
	for _, vp := range views {
		if vp.Status != status {
			t.Fatalf("Expected view %q status, but got %q", status, vp.Status)
		}
	}
}

func validateGetProgress(t *testing.T, pr Progress) {
	t.Helper()
	validateTablesProgress(t, pr.Tables)
	validateViewsProgress(t, pr.Views)

}

func validateTablesProgress(t *testing.T, tables []TableProgress) {
	t.Helper()
	for _, tp := range tables {
		validateProgress(t, tp.progress)
	}
}

func validateViewsProgress(t *testing.T, views []ViewProgress) {
	t.Helper()

	for _, vp := range views {
		validateProgress(t, vp.progress)
	}
}

func validateProgress(t *testing.T, p progress) {
	t.Helper()
	if p.Size != p.Restored {
		t.Fatalf("Expected to restore %d, but restored %d", p.Size, p.Restored)
	}
	if p.StartedAt == nil {
		t.Fatalf("Expected StartedAt != nil, but got nil")
	}
	if p.CompletedAt == nil {
		t.Fatalf("Expected CompletedAt != nil, but got nil")
	}
	if p.Status != ProgressStatusDone {
		t.Fatalf("Expected status %q, but got %q", ProgressStatusDone, p.Status)
	}
}
