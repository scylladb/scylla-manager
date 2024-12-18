// Copyright (C) 2024 ScyllaDB

//go:build all || integration
// +build all integration

package backup_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestBackupPauseResumeOnDeduplicationStageIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-deduplication"
		testKeyspace = "backuptest_deduplication"
	)

	location := s3Location(testBucket)
	config := defaultConfig()

	var (
		session        = db.CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		clusterSession = db.CreateSessionAndDropAllKeyspaces(t, h.Client)
		ctx, cancel    = context.WithCancel(context.Background())
	)

	db.WriteData(t, clusterSession, testKeyspace, 3)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []backupspec.Location{location},
		Retention: 2,
		RateLimit: []backupspec.DCLimit{
			{"dc1", 1},
		},
		Continue: true,
	}
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("Given: backup is created out of the current cluster state")
	var originalTotalSize int64
	func() {
		var deduplicatedOnFreshBackup int64
		after := func(skipped, uploaded, size int64) {
			deduplicatedOnFreshBackup += skipped
			originalTotalSize += size
		}
		defer h.service.RemoveDeduplicateTestHooks()
		h.service.SetDeduplicateTestHooks(func() {}, after)

		if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
			t.Fatal(err)
		}
		Print("Then: nothing is deduplicated")
		if deduplicatedOnFreshBackup != 0 {
			t.Fatalf("Expected deduplicated 0 bytes on fresh backup, but was %v", deduplicatedOnFreshBackup)
		}
	}()

	Print("Then: another backup to deduplicate everything (no delta)")
	time.Sleep(time.Second)
	func() {
		var totalDeduplicated, totalUploaded, totalSize int64
		after := func(skipped, uploaded, size int64) {
			totalDeduplicated += skipped
			totalUploaded += uploaded
			totalSize += size
		}
		defer h.service.RemoveDeduplicateTestHooks()
		h.service.SetDeduplicateTestHooks(func() {}, after)

		if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
			t.Fatal(err)
		}

		Print("Then: everything is deduplicated")
		if totalDeduplicated != totalSize {
			t.Fatalf("Expected deduplicated %v bytes on delta 0 backup, but was %v", totalSize, totalDeduplicated)
		}
		Print("Then: nothing is uploaded")
		if totalUploaded != 0 {
			t.Fatalf("Expected uploaded 0 bytes on delta 0 backup, but was %v", totalUploaded)
		}
		Printf("Then: total size is equal to first backup")
		if totalSize != originalTotalSize {
			t.Fatalf("Expected total size to be %v, but was %v", originalTotalSize, totalSize)
		}
	}()

	Print("Given: yet  another backup is started on empty delta (no data changes) and paused od DEDUPLICATE stage")
	time.Sleep(time.Second)
	func() {
		onlyOneHostToProcessMutex := sync.Mutex{}
		var totalDeduplicated, totalUploaded, totalSize int64
		before := func() {
			onlyOneHostToProcessMutex.Lock()
		}
		after := func(skipped, uploaded, size int64) {
			totalDeduplicated += skipped
			totalUploaded += uploaded
			totalSize += size
			cancel()
			onlyOneHostToProcessMutex.Unlock()
		}
		defer h.service.RemoveDeduplicateTestHooks()
		h.service.SetDeduplicateTestHooks(before, after)

		if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}

		Print("Then: not everything is either deduplicated and 0 is uploaded")
		if totalDeduplicated == totalSize || totalUploaded > 0 {
			t.Fatalf("Expected backup to be paused in the middle")
		}
	}()

	Print("When: backup is resumed with the new RunID")
	func() {
		var totalDeduplicated, totalUploaded, totalSize int64
		after := func(skipped, uploaded, size int64) {
			totalDeduplicated += skipped
			totalUploaded += uploaded
			totalSize += size
		}
		defer h.service.RemoveDeduplicateTestHooks()
		h.service.SetDeduplicateTestHooks(func() {}, after)

		if err := h.service.Backup(context.Background(), h.ClusterID, h.TaskID, uuid.NewTime(), target); err != nil {
			t.Fatal(err)
		}
		Printf("Then: total size is equal to first backup")
		if totalSize != originalTotalSize {
			t.Fatalf("Expected total size to be %v, but was %v", originalTotalSize, totalSize)
		}
		Print("Then: everything is deduplicated")
		if totalDeduplicated != totalSize {
			t.Fatalf("Expected deduplicated %v bytes on delta 0 backup, but was %v", totalSize, totalDeduplicated)
		}
		Print("Then: nothing is uploaded")
		if totalUploaded != 0 {
			t.Fatalf("Expected uploaded 0 bytes on delta 0 backup, but was %v", totalUploaded)
		}
	}()
}
