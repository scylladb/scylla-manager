// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"sync"
	"testing"
)

func TestAverageBandwidthPerShard(t *testing.T) {
	testCases := []struct {
		name     string
		stats    *restoreStats
		expected string
	}{
		{
			name:     "no data",
			stats:    &restoreStats{},
			expected: "unknown",
		},
		{
			name: "no shards",
			stats: &restoreStats{
				totalDownloadTimeMS: 1000,
				totalDownloadBytes:  1024,
			},
			expected: "1KiB/s",
		},
		{
			name: "shards",
			stats: &restoreStats{
				totalDownloadTimeMS: 1000,
				totalDownloadBytes:  1024,
				totalShards:         2,
			},
			expected: "512B/s/shard",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.stats.averageBandwidthPerShard(); got != tc.expected {
				t.Fatalf("Expected bandwidth %s, but got %s", tc.expected, got)
			}
		})
	}
}

func TestIncrementDownloadStats(t *testing.T) {
	stats := newRestoreStats([]hostWorkload{
		{host: Host{ShardCount: 2}},
		{host: Host{ShardCount: 2}},
	})

	var (
		wg              sync.WaitGroup
		numberOfWorkers = 10
		bytesPerWorker  = 1024
		timePerWorker   = 1000
	)

	wg.Add(numberOfWorkers)
	for range numberOfWorkers {
		go func() {
			stats.incrementDownloadStats(int64(bytesPerWorker), int64(timePerWorker))
			wg.Done()
		}()
	}
	wg.Wait()

	if stats.totalShards != 4 {
		t.Fatalf("Expected totalShards 4, but got %d", stats.totalShards)
	}
	if stats.totalDownloadBytes != int64(numberOfWorkers*bytesPerWorker) {
		t.Fatalf("Expected totalDownloadBytes %d, but got %d", int64(numberOfWorkers*bytesPerWorker), stats.totalDownloadBytes)
	}
	if stats.totalDownloadTimeMS != int64(numberOfWorkers*timePerWorker) {
		t.Fatalf("Expected totalDownloadTimeMS %d, but got %d", int64(numberOfWorkers*timePerWorker), stats.totalDownloadTimeMS)
	}
}
