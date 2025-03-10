// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package one2onerestore

import (
	"context"
	"os"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
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
	snapshotTag := h.runBackup(t, map[string]any{
		"location": []backupspec.Location{loc},
	})
	clusterSession := db.CreateSessionAndDropAllKeyspaces(t, h.client)
	ksName := "testgetprogress"
	db.WriteData(t, clusterSession, ksName, 10)
	mvName := "testmv"
	db.CreateMaterializedView(t, clusterSession, ksName, db.BigTableName, mvName)

	if srcCnt := rowCount(t, clusterSession, ksName, db.BigTableName); srcCnt == 0 {
		t.Fatalf("Unexpected row count in table: 0")
	}

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

	testutils.Print("ALTER_TGC Stage")
	err = w.setTombstoneGCModeRepair(context.Background(), workload)
	if err != nil {
		t.Fatalf("Unexpected err, setTombstoneGCModeRepair: %v", err)
	}

	pr, err := w.getProgress(context.Background(), target)
	if err != nil {
		t.Fatalf("Unexpected err, getProgress: %v", err)
	}
	if pr.Stage != StageAlterTGC {
		t.Fatalf("Expected stage ALTER_TGC, but got %s", pr.Stage)
	}
	validateKeyspaces(t, pr.Keyspaces)

	testutils.Print("DROP_VIEWS Stage")
	views, err := w.dropViews(context.Background(), workload)
	if err != nil {
		t.Fatalf("Unexpected err, dropViews: %v", err)
	}

	pr, err = w.getProgress(context.Background(), target)
	if err != nil {
		t.Fatalf("Unexpected err, getProgress: %v", err)
	}
	if pr.Stage != StageDropViews {
		t.Fatalf("Expected stage DROP_VIEW, but got %s", pr.Stage)
	}
	validateViews(t, pr.Views, "")

	testutils.Print("DATA Stage")
	if err := w.restoreTables(context.Background(), workload, target.Keyspace); err != nil {
		t.Fatalf("Unexpected err, restoreTables: %v", err)
	}
	pr, err = w.getProgress(context.Background(), target)
	if err != nil {
		t.Fatalf("Unexpected err, getProgress: %v", err)
	}
	if pr.Stage != StageData {
		t.Fatalf("Expected stage DATA, but got %s", pr.Stage)
	}
	validateKeyspaces(t, pr.Keyspaces)
	validateHosts(t, pr.Hosts)

	testutils.Print("RECREATE_VIEWS Stage")
	if err := w.reCreateViews(context.Background(), views); err != nil {
		t.Fatalf("Unexpected err, reCreateViews: %v", err)
	}
	pr, err = w.getProgress(context.Background(), target)
	if err != nil {
		t.Fatalf("Unexpected err, getProgress: %v", err)
	}
	if pr.Stage != StageRecreateViews {
		t.Fatalf("Expected stage RECREATE_VIEW, but got %s", pr.Stage)
	}
	validateViews(t, pr.Views, scyllaclient.StatusSuccess)

	testutils.Print("DONE Stage")
	w.progressDone(context.Background(), timeutc.Now(), timeutc.Now())
	pr, err = w.getProgress(context.Background(), target)
	if err != nil {
		t.Fatalf("Unexpected err, getProgress: %v", err)
	}
	validateGetProgress(t, pr, snapshotTag)
}

func validateGetProgress(t *testing.T, pr Progress, tag string) {
	t.Helper()
	if pr.SnapshotTag != tag {
		t.Fatalf("Expected snapshot tag %s, but got %s", tag, pr.SnapshotTag)
	}
	if pr.Stage != StageDone {
		t.Fatalf("Expected stage DONE, but got %s", pr.Stage)
	}
	validateProgress(t, pr.progress)
	validateKeyspaces(t, pr.Keyspaces)
	validateHosts(t, pr.Hosts)
	validateViews(t, pr.Views, scyllaclient.StatusSuccess)
}

func validateProgress(t *testing.T, p progress) {
	t.Helper()
	if p.Size != p.Restored {
		t.Fatalf("Expected to restore %d, but restored %d", p.Size, p.Restored)
	}
	if p.Size != p.Downloaded {
		t.Fatalf("Expected to download %d, but got %d", p.Size, p.Downloaded)
	}
	if p.Failed != 0 {
		t.Fatalf("Expected failed to be 0, but got: %d", p.Failed)
	}
	if p.StartedAt == nil {
		t.Fatalf("Expected StartedAt != nil, but got nil")
	}
	if p.CompletedAt == nil {
		t.Fatalf("Expected CompletedAt != nil, but got nil")
	}
}

func validateKeyspaces(t *testing.T, keyspaces []KeyspaceProgress) {
	t.Helper()
	if len(keyspaces) == 0 {
		t.Fatalf("Expected len(keyspaces) > 0, but got 0")
	}
	for _, kp := range keyspaces {
		validateProgress(t, kp.progress)
		if kp.Keyspace == "" {
			t.Fatalf("Expected not empty keyspace name, but got ''")
		}
		if len(kp.Tables) == 0 {
			t.Fatalf("Expected %s len(tables) > 0, but got 0", kp.Keyspace)
		}
		for _, tp := range kp.Tables {
			validateProgress(t, tp.progress)
			if tp.Table == "" {
				t.Fatalf("Expected not empty %s table name, but got ''", kp.Keyspace)
			}
			if tp.TombstoneGC != modeRepair {
				t.Fatalf("Expected tombstone_gc mode of %s.%s to be repair, but got %s", kp.Keyspace, tp.Table, tp.TombstoneGC)
			}
		}
	}
}

func validateHosts(t *testing.T, hosts []HostProgress) {
	t.Helper()
	if len(hosts) != 6 {
		t.Fatalf("Expected len(hosts) == 6, but got %d", len(hosts))
	}
	for _, hp := range hosts {
		if hp.Host == "" {
			t.Fatalf("Expected not empty host, but got ''")
		}
		if hp.ShardCnt == 0 {
			t.Fatalf("Expected %s host ShardCound != 0, but got 0", hp.Host)
		}
		if hp.DownloadedBytes == 0 {
			t.Fatalf("Expected not empty %s host DownloadedBytes, but got 0", hp.Host)
		}
		if hp.DownloadDuration == 0 {
			t.Fatalf("Expected not empty %s host DownloadDuration, but got 0", hp.Host)
		}
	}
}

func validateViews(t *testing.T, views []View, expectedBuildStatus scyllaclient.ViewBuildStatus) {
	t.Helper()
	if len(views) == 0 {
		t.Fatalf("Expected len(views) > 0, but got 0")
	}
	for _, v := range views {
		if v.Keyspace == "" {
			t.Fatalf("Expected not empty view.Keyspace, but got ''")
		}
		if v.BaseTable == "" {
			t.Fatalf("Expected not empty view.BaseTable, but got ''")
		}
		if v.View == "" {
			t.Fatalf("Expected not empty view.View, but got ''")
		}
		if v.Type == "" {
			t.Fatalf("Expected not empty view.Type, but got ''")
		}
		if v.BuildStatus != expectedBuildStatus {
			t.Fatalf("Expected view.BuildStatus '%s', but got '%s'", expectedBuildStatus, v.BuildStatus)
		}
	}
}
