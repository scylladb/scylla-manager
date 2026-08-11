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
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
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

func TestBackupProtectedManifestsIntegration(t *testing.T) {
	// This test verifies that snapshots whose manifests are protected by a retention
	// lock or an event based hold are not removed. It checks that:
	// - purge skips them even when the retention policy marks them as stale
	// - explicit snapshot deletion fails instead of removing them
	// and that once protection is cleared, those methods can remove them.
	testCases := []struct {
		name              string
		bucket            string
		keyspace          string
		retentionLockMode backup.RetentionLockMode
		setupInterceptor  func(ctx context.Context, h *backupTestHelper) *eventBasedHoldInterceptor
		clearProtection   func(ctx context.Context, h *backupTestHelper, i *eventBasedHoldInterceptor, remotePath string) error
	}{
		{
			name:              "retention lock",
			bucket:            "backuptest-purge-protected-lock",
			keyspace:          "backuptest_purge_protected_lock",
			retentionLockMode: backup.RetentionLockUnlocked,
			clearProtection: func(ctx context.Context, h *backupTestHelper, _ *eventBasedHoldInterceptor, remotePath string) error {
				// GCS mock used in test env does not support clearing retention policy,
				// but it allows to override it and set it in the past.
				return h.Client.RcloneRetentionLock(ctx, ManagedClusterHost(), remotePath,
					scyllaclient.RetentionModeUnlocked, timeutc.Now().Add(-24*time.Hour), true)
			},
		},
		{
			name:              "event based hold",
			bucket:            "backuptest-purge-protected-hold",
			keyspace:          "backuptest_purge_protected_hold",
			retentionLockMode: backup.RetentionLockEventBasedHold,
			setupInterceptor: func(ctx context.Context, h *backupTestHelper) *eventBasedHoldInterceptor {
				i := newEventBasedHoldInterceptor(t, h.Hrt)
				i.defaultEventBasedHold = true
				i.defaultBucketRetentionPolicy = 24 * time.Hour
				return i
			},
			clearProtection: func(ctx context.Context, h *backupTestHelper, i *eventBasedHoldInterceptor, remotePath string) error {
				// Simple hold removal triggers bucket retention policy, so we need to first
				// remove it, apply the hold and only then remove it, so that it correctly
				// handles objects with already started retention periods.
				i.defaultBucketRetentionPolicy = 0
				if err := h.Client.RcloneEventBasedHold(ctx, ManagedClusterHost(), remotePath, true); err != nil {
					return err
				}
				return h.Client.RcloneEventBasedHold(ctx, ManagedClusterHost(), remotePath, false)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			location := backupspec.Location{Provider: backupspec.GCS, Path: tc.bucket}
			GCSInitBucket(t, tc.bucket)

			var (
				session        = CreateScyllaManagerDBSession(t)
				h              = newBackupTestHelper(t, session, defaultConfig(), location, nil)
				ctx            = t.Context()
				clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
			)

			var i *eventBasedHoldInterceptor
			if tc.setupInterceptor != nil {
				i = tc.setupInterceptor(ctx, h)
			}

			const replication = "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}"
			nextID := RawWriteData(t, clusterSession, tc.keyspace, 0, 1, replication, true)

			props := defaultTestProperties(location, tc.keyspace)
			// Just to satisfy retention lock validation.
			// Different retention policy will be used for purge purposes.
			props["retention_days"] = 1
			// Tests forcing GCS provider need to use method auto to avoid
			// CI workflows using scylla version without native backup
			// support for gcs aimed to test native backup for s3.
			props["method"] = backup.MethodAuto
			rawProps, err := json.Marshal(props)
			if err != nil {
				t.Fatal(err)
			}
			target, err := h.service.GetTarget(ctx, h.ClusterID, rawProps)
			if err != nil {
				t.Fatal(err)
			}
			// Added here to bypass validation, as this method is not yet exposed
			target.RetentionLockMode = tc.retentionLockMode
			// Inject purge so that it keeps only the last snapshot
			target.RetentionMap = backup.RetentionMap{h.TaskID: {Retention: 1}}
			// To skip not interesting schema sstables
			target.Units = []backup.Unit{{Keyspace: tc.keyspace}}

			// runBackup executes a backup and returns its snapshot tag
			runBackup := func(target backup.Target) string {
				runID := uuid.NewTime()
				if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, runID, target); err != nil {
					t.Fatal(err)
				}
				pr, err := h.service.GetProgress(ctx, h.ClusterID, h.TaskID, runID)
				if err != nil {
					t.Fatal(err)
				}
				return pr.SnapshotTag
			}

			// assertSnapshotFiles verifies that the snapshot consists of exactly the expected set of files
			assertSnapshotFiles := func(tag string, want *strset.Set) {
				if got := strset.New(listSnapshotFiles(t, h, tag)...); !want.IsEqual(got) {
					t.Fatalf("snapshot %s files changed, want: \n%v\n, got: \n%v\n", tag, want.List(), got.List())
				}
			}

			Print("When: protected backup is executed")
			tagA := runBackup(target)
			tagAFiles := strset.New(listSnapshotFiles(t, h, tagA)...)

			Print("Then: deleting protected snapshot fails")
			if err := h.service.DeleteSnapshot(ctx, h.ClusterID, []backupspec.Location{location}, []string{tagA}); err == nil {
				t.Fatalf("Expected protected snapshot %s deletion to fail", tagA)
			}

			Print("And: protected snapshot is still there")
			assertSnapshotFiles(tagA, tagAFiles)

			Print("When: new backup with new data is executed")
			nextID = RawWriteData(t, clusterSession, tc.keyspace, nextID, 1, replication, true)
			tagB := runBackup(target)
			tagBFiles := strset.New(listSnapshotFiles(t, h, tagB)...)

			Print("Then: first snapshot exists despite retention=1")
			assertSnapshotFiles(tagA, tagAFiles)

			Print("And: deleting any protected snapshot fails")
			for _, tag := range []string{tagA, tagB} {
				if err := h.service.DeleteSnapshot(ctx, h.ClusterID, []backupspec.Location{location}, []string{tag}); err == nil {
					t.Fatalf("Expected protected snapshot %s deletion to fail", tag)
				}
			}

			Print("And: both protected snapshots are still there")
			assertSnapshotFiles(tagA, tagAFiles)
			assertSnapshotFiles(tagB, tagBFiles)

			Print("When: all protection is manually cleared")
			strset.Union(tagAFiles, tagBFiles).Each(func(f string) bool {
				if err := tc.clearProtection(ctx, h, i, h.location.RemotePath(f)); err != nil {
					t.Fatal(err)
				}
				return true
			})

			Print("And: purge is executed")
			purgeTarget := target
			purgeTarget.PurgeOnly = true
			runBackup(purgeTarget)

			Print("Then: stale snapshot is purged")
			if got := listSnapshotFiles(t, h, tagA); len(got) != 0 {
				t.Fatalf("Expected stale snapshot %s to be purged", tagA)
			}

			Print("And: last snapshot is kept")
			assertSnapshotFiles(tagB, tagBFiles)

			Print("When: last snapshot is deleted")
			if err := h.service.DeleteSnapshot(ctx, h.ClusterID, []backupspec.Location{location}, []string{tagB}); err != nil {
				t.Fatal(err)
			}

			Print("Then: last snapshot is deleted")
			if got := listSnapshotFiles(t, h, tagB); len(got) != 0 {
				t.Fatalf("Expected last snapshot %s to be deleted", tagB)
			}
		})
	}
}

func listSnapshotFiles(t *testing.T, h *backupTestHelper, snapshotTag string) []string {
	t.Helper()

	manifests, schemas, files, scyllaManifests := listGroupedSnapshotFiles(t, h, snapshotTag)
	all := make([]string, 0, len(manifests)+len(schemas)+len(files)+len(scyllaManifests))
	all = append(all, manifests...)
	all = append(all, schemas...)
	all = append(all, files...)
	all = append(all, scyllaManifests...)
	return all
}

func listGroupedSnapshotFiles(t *testing.T, h *backupTestHelper, snapshotTag string) (manifests, schemas, files, scyllaManifests []string) {
	t.Helper()

	filesInfo, err := h.service.ListFiles(t.Context(), h.ClusterID, []backupspec.Location{h.location}, backup.ListFilter{
		ClusterID:   h.ClusterID,
		TaskID:      h.TaskID,
		SnapshotTag: snapshotTag,
	})
	if err != nil {
		t.Fatalf("ListFiles for tag %s: %v", snapshotTag, err)
	}

	for _, fi := range filesInfo {
		if fi.Schema != "" {
			schemas = append(schemas, fi.Schema)
		}
		for _, fm := range fi.Files {
			for _, f := range fm.Files {
				files = append(files, path.Join(fm.Path, f))
			}
			for _, sm := range fm.ScyllaManifests {
				scyllaManifests = append(scyllaManifests, path.Join(fm.Path, sm))
			}
		}
	}

	allManifests, _, _, _ := h.listS3Files()
	for _, manifestPath := range allManifests {
		if strings.Contains(manifestPath, snapshotTag) {
			manifests = append(manifests, manifestPath)
		}
	}

	return manifests, schemas, files, scyllaManifests
}
