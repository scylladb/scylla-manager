// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package backup_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/maps"
	"go.uber.org/atomic"
	"google.golang.org/api/option"
)

type retentionLockHandler struct {
	t      *testing.T
	client *storage.Client
	bucket string
}

func newRetentionLockHandler(t *testing.T, bucket string) *retentionLockHandler {
	t.Helper()

	endpoint, _ := GCSCredentials()
	if endpoint == "" {
		t.Fatal("GCS endpoint not configured")
	}

	client, err := storage.NewClient(t.Context(),
		option.WithEndpoint(endpoint+"/storage/v1/"),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("create GCS client: %v", err)
	}

	return &retentionLockHandler{
		t:      t,
		client: client,
		bucket: bucket,
	}
}

func (c *retentionLockHandler) getRetention(object string) (mode string, retainUntil time.Time) {
	attrs, err := c.client.Bucket(c.bucket).Object(object).Attrs(c.t.Context())
	if err != nil {
		c.t.Fatalf("get attrs for %s/%s: %v", c.bucket, object, err)
	}
	if attrs.Retention == nil {
		return "", time.Time{}
	}
	return attrs.Retention.Mode, attrs.Retention.RetainUntil
}

func (c *retentionLockHandler) assertRetention(expectedMode string, expectedRetainUntil time.Time, objects ...string) {
	c.t.Helper()

	for _, object := range objects {
		mode, retainUntil := c.getRetention(object)
		if mode != expectedMode {
			c.t.Errorf("object %s: expected retention mode %q, got %q", object, expectedMode, mode)
		}
		if !retainUntil.Equal(expectedRetainUntil) {
			c.t.Errorf("object %s: expected retainUntil %v, got %v", object, expectedRetainUntil, retainUntil)
		}
	}
}

func snapshotTagRetainUntil(t *testing.T, snapshotTag string, retentionDays int) time.Time {
	t.Helper()

	until, err := backup.RetentionLockUntil(snapshotTag, retentionDays)
	if err != nil {
		t.Fatal(err)
	}
	return until
}

func TestBackupRetentionLockIntegration(t *testing.T) {
	// This test validates that object retention locks are correctly applied
	// across multiple backup executions with different retention lock configurations.
	// It verifies that:
	// - All backup files receive correct retention mode and retainUntil time
	// - Deduplicated files from previous backups get updated lock when re-referenced
	// - Changing retention config (days, mode, override) correctly affects subsequent backups
	// - Files not referenced by newer backups retain their original locks
	const (
		testBucket   = "backuptest-retention-lock"
		testKeyspace = "backuptest_retention_lock"
	)

	location := backupspec.Location{
		Provider: backupspec.GCS,
		Path:     testBucket,
	}
	GCSInitBucket(t, testBucket)
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
		lockHandler    = newRetentionLockHandler(t, testBucket)
	)

	ni, err := h.Client.AnyNodeInfo(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	WriteData(t, clusterSession, testKeyspace, 1)

	getTargetAndValidate := func(lockMode string, overrideLock bool, retentionDays int) backup.Target {
		props := defaultTestProperties(location, testKeyspace)
		props["retention_lock_mode"] = lockMode
		props["override_retention_lock"] = overrideLock
		props["retention_days"] = retentionDays
		if CheckConstraint(t, ni.ScyllaVersion, "< 2026.1") {
			props["method"] = "rclone"
		}

		rawProps, err := json.Marshal(props)
		if err != nil {
			t.Fatal(err)
		}

		target, err := h.service.GetTarget(t.Context(), h.ClusterID, rawProps)
		if err != nil {
			t.Fatal(err)
		}
		if string(target.RetentionLockMode) != lockMode {
			t.Fatalf("Expected retention lock mode %q, got %q", lockMode, target.RetentionLockMode)
		}
		if target.OverrideRetentionLock != overrideLock {
			t.Fatalf("Expected override retention lock %v, got %v", overrideLock, target.OverrideRetentionLock)
		}
		if target.RetentionDays != retentionDays {
			t.Fatalf("Expected retention days %d, got %d", retentionDays, target.RetentionDays)
		}

		return target
	}

	getTagAndValidate := func(runID uuid.UUID, lockMode string, overrideLock bool, retentionDays int) string {
		pr, err := h.service.GetProgress(t.Context(), h.ClusterID, h.TaskID, runID)
		if err != nil {
			t.Fatal(err)
		}
		if string(pr.RetentionLockMode) != lockMode {
			t.Fatalf("Expected retention lock mode %q, got %q", lockMode, pr.RetentionLockMode)
		}
		if pr.OverrideRetentionLock != overrideLock {
			t.Fatalf("Expected override retention lock %v, got %v", overrideLock, pr.OverrideRetentionLock)
		}
		if pr.RetentionDays != retentionDays {
			t.Fatalf("Expected retention days %d, got %d", retentionDays, pr.RetentionDays)
		}

		return pr.SnapshotTag
	}

	Print("When: first backup with retention lock (unlocked, 1 day) is executed")
	target := getTargetAndValidate("unlocked", false, 1)

	runID := uuid.NewTime()
	if err := h.service.Backup(t.Context(), h.ClusterID, h.TaskID, runID, target); err != nil {
		t.Fatal(err)
	}

	tagA := getTagAndValidate(runID, "unlocked", false, 1)
	Print("Then: first backup completed with tag " + tagA)

	Print("Then: all objects have correct retention locks (tag A)")
	tagAFiles := listSnapshotFiles(t, h, tagA)
	expectedRetainA := snapshotTagRetainUntil(t, tagA, 1)
	lockHandler.assertRetention("Unlocked", expectedRetainA, tagAFiles...)

	Print("When: new data is written")
	WriteData(t, clusterSession, testKeyspace, 1)

	Print("And: second backup is executed with the same config")
	runID = uuid.NewTime()
	if err := h.service.Backup(t.Context(), h.ClusterID, h.TaskID, runID, target); err != nil {
		t.Fatal(err)
	}

	tagB := getTagAndValidate(runID, "unlocked", false, 1)
	Print("Then: second backup completed with tag " + tagB)

	Print("Then: all objects have correct retention locks (tag B)")
	tagBFiles := listSnapshotFiles(t, h, tagB)
	tagBFilesSet := maps.SetFromSlice(tagBFiles)
	expectedRetainB := snapshotTagRetainUntil(t, tagB, 1)
	// Files referenced by tag B should have tag B locks.
	// Files referenced by tag A, but not by tag B should have tag A locks.
	for _, f := range tagBFiles {
		lockHandler.assertRetention("Unlocked", expectedRetainB, f)
	}
	for _, f := range tagAFiles {
		if _, ok := tagBFilesSet[f]; !ok {
			lockHandler.assertRetention("Unlocked", expectedRetainA, f)
		}
	}

	Print("When: target is updated to locked mode, 2 days, with override")
	target = getTargetAndValidate("locked", true, 2)

	Print("And: new data is written")
	WriteData(t, clusterSession, testKeyspace, 1)

	Print("And: third backup is executed with updated config (locked, 2 day, override)")
	runID = uuid.NewTime()
	if err := h.service.Backup(t.Context(), h.ClusterID, h.TaskID, runID, target); err != nil {
		t.Fatal(err)
	}

	tagC := getTagAndValidate(runID, "locked", true, 2)
	Print("Then: third backup completed with tag " + tagC)

	Print("Then: all objects have correct retention locks (tag C)")
	tagCFiles := listSnapshotFiles(t, h, tagC)
	tagCFilesSet := maps.SetFromSlice(tagCFiles)
	expectedRetainC := snapshotTagRetainUntil(t, tagC, 2)

	for _, f := range tagCFiles {
		lockHandler.assertRetention("Locked", expectedRetainC, f)
	}
	for _, f := range tagBFiles {
		if _, ok := tagCFilesSet[f]; !ok {
			lockHandler.assertRetention("Unlocked", expectedRetainB, f)
		}
	}
	for _, f := range tagAFiles {
		_, okB := tagBFilesSet[f]
		_, okC := tagCFilesSet[f]
		if !okB && !okC {
			lockHandler.assertRetention("Unlocked", expectedRetainA, f)
		}
	}
}

func TestBackupResumeOnRetentionLockStageIntegration(t *testing.T) {
	// This test validates that a backup interrupted during the RETENTION_LOCK stage
	// can be successfully resumed. It verifies that:
	// - Backup can be interrupted mid-way through retention lock application
	// - After interruption, some objects have locks applied (partial progress)
	// - Resumed backup completes successfully and all objects end up with correct locks
	const (
		testBucket   = "backuptest-retention-lock-resume"
		testKeyspace = "backuptest_retention_lock_resume"
	)

	location := backupspec.Location{
		Provider: backupspec.GCS,
		Path:     testBucket,
	}
	GCSInitBucket(t, testBucket)
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
		lockHandler    = newRetentionLockHandler(t, testBucket)
	)

	ni, err := h.Client.AnyNodeInfo(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	WriteData(t, clusterSession, testKeyspace, 1)

	props := defaultTestProperties(location, testKeyspace)
	props["retention_days"] = 1
	props["retention_lock_mode"] = "unlocked"
	props["continue"] = true
	if CheckConstraint(t, ni.ScyllaVersion, "< 2026.1") {
		props["method"] = "rclone"
	}

	stopCtx, stop := context.WithCancel(t.Context())
	defer stop()

	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(err)
	}
	target, err := h.service.GetTarget(stopCtx, h.ClusterID, rawProps)
	if err != nil {
		t.Fatal(err)
	}

	// Set interceptor that allows first 3 retention-lock calls through,
	// then cancels context on the 3rd call.
	Print("Given: interceptor that cancels after 2 retention-lock calls")
	var lockCalls atomic.Int32
	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.Path, "/agent/rclone/operations/retention-lock") {
			if lockCalls.Add(1) > 3 {
				stop()
				return nil, context.Canceled
			}
		}
		return nil, nil
	}))

	Print("When: backup is started")
	done := make(chan error, 1)
	go func() {
		done <- h.service.Backup(stopCtx, h.ClusterID, h.TaskID, h.RunID, target)
		close(done)
	}()

	select {
	case <-time.After(5 * time.Minute):
		t.Fatal("Backup did not complete within timeout")
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Expected backup to be paused, got %v", err)
		}
		Print("Then: backup was interrupted during retention lock stage")
	}

	Print("And: some objects have retention locks (partial progress)")
	pr, err := h.service.GetProgress(t.Context(), h.ClusterID, h.TaskID, h.RunID)
	if err != nil {
		t.Fatal(err)
	}
	tag := pr.SnapshotTag

	snapshotFiles := listSnapshotFiles(t, h, tag)
	var encounteredLocked, encounteredNotLocked bool
	for _, f := range snapshotFiles {
		if mode, _ := lockHandler.getRetention(f); mode != "" {
			encounteredLocked = true
		} else {
			encounteredNotLocked = true
		}
	}
	if !encounteredLocked {
		t.Fatal("Expected some objects to have retention locks after partial execution")
	}
	if !encounteredNotLocked {
		t.Fatal("Expected not all objects to have retention locks after partial execution")
	}

	Print("When: interceptor is cleared and backup is resumed")
	h.Hrt.SetInterceptor(nil)

	resumeRunID := uuid.NewTime()
	if err := h.service.Backup(t.Context(), h.ClusterID, h.TaskID, resumeRunID, target); err != nil {
		t.Fatalf("Resumed backup failed: %v", err)
	}

	Print("Then: all objects have correct retention locks after resume")
	expectedRetain := snapshotTagRetainUntil(t, tag, 1)
	lockHandler.assertRetention("Unlocked", expectedRetain, listSnapshotFiles(t, h, tag)...)
}

func listSnapshotFiles(t *testing.T, h *backupTestHelper, snapshotTag string) []string {
	t.Helper()

	filesInfo, err := h.service.ListFiles(t.Context(), h.ClusterID, []backupspec.Location{h.location}, backup.ListFilter{
		ClusterID:   h.ClusterID,
		TaskID:      h.TaskID,
		SnapshotTag: snapshotTag,
	})
	if err != nil {
		t.Fatalf("ListFiles for tag %s: %v", snapshotTag, err)
	}

	var files []string
	for _, fi := range filesInfo {
		// Add schema file
		if fi.Schema != "" {
			files = append(files, fi.Schema)
		}
		// Add sstable files and scylla manifests
		for _, fm := range fi.Files {
			for _, f := range fm.Files {
				files = append(files, path.Join(fm.Path, f))
			}
			for _, sm := range fm.ScyllaManifests {
				files = append(files, path.Join(fm.Path, sm))
			}
		}
	}

	// Add SM manifests
	manifests, _, _, _ := h.listS3Files()
	for _, manifestPath := range manifests {
		if strings.Contains(manifestPath, snapshotTag) {
			files = append(files, manifestPath)
		}
	}

	return files
}
