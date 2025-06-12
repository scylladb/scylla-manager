// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"sync"
	"testing"
)

func TestAverageBandwidthPerShard(t *testing.T) {
	testCases := []struct {
		name                string
		workload            []hostWorkload
		totalDownloadBytes  int64
		totalDownloadTimeMS int64
		expected            string
	}{
		{
			name:     "no data",
			workload: nil,
			expected: "unknown",
		},
		{
			name: "no shards",

			workload: []hostWorkload{
				{host: Host{ShardCount: 0}},
				{host: Host{ShardCount: 0}},
			},
			totalDownloadBytes:  1024,
			totalDownloadTimeMS: 1000,
			expected:            "1KiB/s",
		},
		{
			name: "shards",
			workload: []hostWorkload{
				{host: Host{ShardCount: 1}},
				{host: Host{ShardCount: 1}},
			},
			totalDownloadBytes:  1024,
			totalDownloadTimeMS: 1000,
			expected:            "512B/s/shard",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stats := newRestoreStats(tc.workload)
			stats.incrementDownloadStats(tc.totalDownloadBytes, tc.totalDownloadTimeMS)
			if got := stats.averageBandwidthPerShard(); got != tc.expected {
				t.Fatalf("Expected bandwidth %s, but got %s", tc.expected, got)
			}
		})
	}
}

func TestProgress(t *testing.T) {
	testCases := []struct {
		name                string
		workload            []hostWorkload
		totalDownloadBytes  int64
		totalDownloadTimeMS int64
		expected            float64
	}{
		{
			name:     "no data",
			workload: nil,
			expected: 100.0,
		},
		{
			name: "no tables",
			workload: []hostWorkload{
				{host: Host{ShardCount: 0}},
				{host: Host{ShardCount: 0}},
			},
			totalDownloadBytes:  1024,
			totalDownloadTimeMS: 1000,
			expected:            100.0,
		},
		{
			name: "everything is restored",
			workload: []hostWorkload{
				{host: Host{ShardCount: 1}, tablesToRestore: []scyllaTableWithSize{
					{scyllaTable: scyllaTable{keyspace: "ks1", table: "table1"}, size: 512},
				}},
				{host: Host{ShardCount: 1}, tablesToRestore: []scyllaTableWithSize{
					{scyllaTable: scyllaTable{keyspace: "ks1", table: "table1"}, size: 512},
				}},
			},
			totalDownloadBytes:  1024,
			totalDownloadTimeMS: 1000,
			expected:            100,
		},
		{
			name: "half is restored",
			workload: []hostWorkload{
				{host: Host{ShardCount: 1}, tablesToRestore: []scyllaTableWithSize{
					{scyllaTable: scyllaTable{keyspace: "ks1", table: "table1"}, size: 512},
				}},
				{host: Host{ShardCount: 1}, tablesToRestore: []scyllaTableWithSize{
					{scyllaTable: scyllaTable{keyspace: "ks1", table: "table1"}, size: 512},
				}},
			},
			totalDownloadBytes:  512,
			totalDownloadTimeMS: 1000,
			expected:            50,
		},
		{
			name: "nothing is restored",
			workload: []hostWorkload{
				{host: Host{ShardCount: 1}, tablesToRestore: []scyllaTableWithSize{
					{scyllaTable: scyllaTable{keyspace: "ks1", table: "table1"}, size: 512},
				}},
				{host: Host{ShardCount: 1}, tablesToRestore: []scyllaTableWithSize{
					{scyllaTable: scyllaTable{keyspace: "ks1", table: "table1"}, size: 512},
				}},
			},
			totalDownloadBytes:  0,
			totalDownloadTimeMS: 0,
			expected:            0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stats := newRestoreStats(tc.workload)
			stats.incrementDownloadStats(tc.totalDownloadBytes, tc.totalDownloadTimeMS)
			if got := stats.progress(); got != tc.expected {
				t.Fatalf("Expected progress %.2f, but got %.2f", tc.expected, got)
			}
		})
	}
}

func TestIncrementDownloadStats(t *testing.T) {
	stats := newRestoreStats([]hostWorkload{
		{host: Host{ShardCount: 2}, tablesToRestore: []scyllaTableWithSize{
			{scyllaTable: scyllaTable{keyspace: "ks1", table: "table1"}, size: 5 * 1024},
		}},
		{host: Host{ShardCount: 2}, tablesToRestore: []scyllaTableWithSize{
			{scyllaTable: scyllaTable{keyspace: "ks2", table: "table1"}, size: 5 * 1024},
		}},
	})

	if stats.totalBytesToRestore != 10*1024 {
		t.Fatalf("Expected totalBytesToRestore 10*1024, but got %d", stats.totalBytesToRestore)
	}

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
	if stats.totalDownloadBytes.Load() != int64(numberOfWorkers*bytesPerWorker) {
		t.Fatalf("Expected totalDownloadBytes %d, but got %d", int64(numberOfWorkers*bytesPerWorker), stats.totalDownloadBytes.Load())
	}
	if stats.totalDownloadTimeMS.Load() != int64(numberOfWorkers*timePerWorker) {
		t.Fatalf("Expected totalDownloadTimeMS %d, but got %d", int64(numberOfWorkers*timePerWorker), stats.totalDownloadTimeMS.Load())
	}
}
