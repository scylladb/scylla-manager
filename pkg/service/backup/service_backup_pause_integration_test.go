// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package backup_test

import (
	"context"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/backupspec"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func pathPrefixMatcher(paths ...string) func(req *http.Request) bool {
	return func(req *http.Request) bool {
		for _, p := range paths {
			if strings.HasPrefix(req.URL.Path, p) {
				return true
			}
		}
		return false
	}
}

// onAllHostsMatcher matches the first request matching match once such
// requests were issued to n distinct hosts. Pausing on job progress this way
// guarantees that SM knows the ID of every started upload job and stops them,
// otherwise a job started by a request canceled mid-flight keeps running.
func onAllHostsMatcher(n int, match func(req *http.Request) bool) func(req *http.Request) bool {
	var (
		mu    sync.Mutex
		hosts = strset.New()
	)
	return func(req *http.Request) bool {
		if !match(req) {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		hosts.Add(req.URL.Host)
		return hosts.Size() >= n
	}
}

func schemaUploadMatcher(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, "/agent/rclone/operations/put") &&
		strings.Contains(req.URL.Query().Get("remote"), "backup/schema/")
}

// setInterceptorPauseOn cancels the backup context on the first request
// matching match. It simulates a pause (task stop) in the stage issuing it.
func (h *backupTestHelper) setInterceptorPauseOn(cancel context.CancelFunc, match func(req *http.Request) bool) {
	var once sync.Once
	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if match(req) {
			once.Do(func() {
				h.T.Log("Pausing backup on", req.Method, req.URL.Path)
				cancel()
			})
			return nil, context.Canceled
		}
		return nil, nil
	}))
}

// pauseBackup runs backup and pauses it on the first request matching match.
// It returns the paused run.
func (h *backupTestHelper) pauseBackup(t *testing.T, runID uuid.UUID, target backup.Target, match func(req *http.Request) bool) *backup.Run {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.setInterceptorPauseOn(cancel, match)
	defer h.Hrt.SetInterceptor(nil)

	err := h.service.Backup(ctx, h.ClusterID, h.TaskID, runID, target)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected %q error, got: %v", context.Canceled, err)
	}
	run, err := h.service.GetRun(context.Background(), h.ClusterID, h.TaskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

// assertBackupComplete checks that the snapshot has a manifest per node, that
// each manifest references an uploaded schema file and that all sstables
// listed in the manifests are present in the backup location.
func (h *backupTestHelper) assertBackupComplete(t *testing.T, snapshotTag string, nodes int) {
	t.Helper()

	manifests, schemas, files, _ := h.listS3Files()
	allFiles := strset.New(files...)

	var tagSchemas []string
	for _, s := range schemas {
		if strings.Contains(s, snapshotTag) {
			tagSchemas = append(tagSchemas, s)
		}
	}
	if len(tagSchemas) == 0 {
		t.Errorf("No schema file uploaded for snapshot %s, schemas: %s", snapshotTag, schemas)
	}
	schemaSet := strset.New(tagSchemas...)

	var tagManifests []string
	for _, m := range manifests {
		if !strings.HasSuffix(m, backupspec.TempFileExt) && backup.SnapshotTagFromManifestPath(t, m) == snapshotTag {
			tagManifests = append(tagManifests, m)
		}
	}
	if len(tagManifests) != nodes {
		t.Fatalf("Expected %d manifests for snapshot %s, got %s", nodes, snapshotTag, manifests)
	}

	for _, m := range tagManifests {
		var mi backupspec.ManifestInfo
		if err := mi.ParsePath(m); err != nil {
			t.Fatal(err)
		}
		mi.Location = h.location

		r, err := h.Client.RcloneOpen(context.Background(), ManagedClusterHost(), h.location.RemotePath(m))
		if err != nil {
			t.Fatal(err)
		}
		var mc backupspec.ManifestContentWithIndex
		err = mc.Read(r)
		r.Close()
		if err != nil {
			t.Fatalf("Read manifest %s: %s", m, err)
		}

		if !schemaSet.Has(mc.Schema) {
			t.Errorf("Manifest %s references schema %q, uploaded schemas: %s", m, mc.Schema, tagSchemas)
		}

		var missing, total int
		err = mc.ForEachIndexIterFiles(nil, &mi, func(dir string, sstables []string) {
			for _, f := range sstables {
				total++
				if !allFiles.Has(path.Join(dir, f)) {
					missing++
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if total == 0 {
			t.Errorf("Manifest %s lists no sstables", m)
		}
		if missing > 0 {
			t.Errorf("Manifest %s: %d of %d sstables are missing from the backup location", m, missing, total)
		}
	}
}

// TestBackupPauseBeforeSchemaUploadIntegration pauses backup in each stage
// between dumping and uploading the schema and checks that the backup
// completed after resume contains the schema referenced by its manifests.
func TestBackupPauseBeforeSchemaUploadIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-pause-schema"
		testKeyspace = "backuptest_pause_schema"
	)

	location := testBackupLocation(testBucket)
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)
	WriteData(t, clusterSession, testKeyspace, 3)

	testCases := []struct {
		stage backup.Stage
		match func(req *http.Request) bool
	}{
		{stage: backup.StageIndex, match: pathPrefixMatcher("/agent/rclone/operations/list")},
		{stage: backup.StageManifest, match: pathPrefixMatcher("/agent/rclone/operations/put")},
		{stage: backup.StageSchema, match: schemaUploadMatcher},
	}

	for _, tc := range testCases {
		t.Run("pause in "+string(tc.stage), func(t *testing.T) {
			h := newBackupTestHelper(t, session, config, location, nil)
			target := defaultTestTarget(location, testKeyspace, "dc1", 2)
			target.Continue = true
			if err := h.service.InitTarget(context.Background(), h.ClusterID, &target); err != nil {
				t.Fatal(err)
			}

			Printf("Given: backup paused in stage %s", tc.stage)
			paused := h.pauseBackup(t, h.RunID, target, tc.match)
			if paused.Stage != tc.stage {
				t.Fatalf("Expected backup to be paused in stage %s, got %s", tc.stage, paused.Stage)
			}

			Print("When: backup is resumed with new RunID")
			runID := uuid.NewTime()
			if err := h.service.Backup(context.Background(), h.ClusterID, h.TaskID, runID, target); err != nil {
				t.Fatal("Unexpected error", err)
			}
			run, err := h.service.GetRun(context.Background(), h.ClusterID, h.TaskID, runID)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Paused run: tag=%s stage=%s, resumed run: tag=%s prev_id=%s stage=%s",
				paused.SnapshotTag, paused.Stage, run.SnapshotTag, run.PrevID, run.Stage)
			if run.Stage != backup.StageDone {
				t.Fatalf("Expected run stage %s, got %s", backup.StageDone, run.Stage)
			}

			Print("Then: backup is complete and its manifests reference the uploaded schema")
			h.assertBackupComplete(t, run.SnapshotTag, 3)
		})
	}
}

// TestBackupPauseReindexOfResumedRunIntegration pauses a run which resumed
// a run paused in UPLOAD while it re-indexes the snapshot. The snapshot of
// the first run must survive, so that the next run can finish the backup.
func TestBackupPauseReindexOfResumedRunIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-pause-reindex"
		testKeyspace = "backuptest_pause_reindex"
	)

	location := testBackupLocation(testBucket)
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)
	WriteData(t, clusterSession, testKeyspace, 3)

	target := defaultTestTarget(location, testKeyspace, "dc1", 2)
	target.RateLimit = []backup.DCLimit{{"dc1", 1}}
	target.Continue = true
	if err := h.service.InitTarget(context.Background(), h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("Given: run A paused in UPLOAD")
	uploadJobs := pathPrefixMatcher("/task_manager/wait_task", "/agent/rclone/job/progress")
	runA := h.pauseBackup(t, h.RunID, target, onAllHostsMatcher(len(h.GetHostsFromDC("dc1")), uploadJobs))
	if runA.Stage != backup.StageUpload {
		t.Fatalf("Expected run A to be paused in stage %s, got %s", backup.StageUpload, runA.Stage)
	}
	h.waitNoTransfers()

	Print("And: run B resumes run A and is paused while re-indexing")
	runBID := uuid.NewTime()
	runB := h.pauseBackup(t, runBID, target, pathPrefixMatcher("/agent/rclone/operations/list"))
	t.Logf("Run B: prev_id=%s tag=%s stage=%s", runB.PrevID, runB.SnapshotTag, runB.Stage)
	if runB.PrevID != runA.ID || runB.SnapshotTag != runA.SnapshotTag {
		t.Fatalf("Expected run B to resume run A (%s, %s), got (%s, %s)", runA.ID, runA.SnapshotTag, runB.PrevID, runB.SnapshotTag)
	}
	h.waitNoTransfers()

	Print("Then: snapshot of run A is kept on all nodes")
	for _, host := range h.GetHostsFromDC("dc1") {
		tags, err := h.Client.Snapshots(context.Background(), host)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(tags, runA.SnapshotTag) {
			t.Errorf("Snapshot %s was deleted from host %s, snapshots: %s", runA.SnapshotTag, host, tags)
		}
	}

	Print("When: run C is started")
	runCID := uuid.NewTime()
	err := h.service.Backup(context.Background(), h.ClusterID, h.TaskID, runCID, target)
	runC, getErr := h.service.GetRun(context.Background(), h.ClusterID, h.TaskID, runCID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	t.Logf("Run C: err=%v prev_id=%s tag=%s stage=%s", err, runC.PrevID, runC.SnapshotTag, runC.Stage)
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	Print("Then: backup of run C is complete")
	h.assertBackupComplete(t, runC.SnapshotTag, 3)
}
