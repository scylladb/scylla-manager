// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"sync/atomic"

	"github.com/scylladb/scylla-manager/v3/pkg/util/sizesuffix"
)

// restoreStats is used to collect some run statistics.
type restoreStats struct {
	totalShards         int64
	totalBytesToRestore int64

	totalDownloadTimeMS atomic.Int64
	totalDownloadBytes  atomic.Int64
}

func newRestoreStats(workload []hostWorkload) *restoreStats {
	var sumShards, sumBytesToRestore int64
	for _, w := range workload {
		sumShards += int64(w.host.ShardCount)
		for _, table := range w.tablesToRestore {
			sumBytesToRestore += table.size
		}
	}
	return &restoreStats{
		totalShards:         sumShards,
		totalBytesToRestore: sumBytesToRestore,
	}
}

// incrementDownloadStats atomically increment downloaded bytes and duration in milliseconds.
func (rs *restoreStats) incrementDownloadStats(downloadedBytes, tookMS int64) {
	rs.totalDownloadBytes.Add(downloadedBytes)
	rs.totalDownloadTimeMS.Add(tookMS)
}

// averageBandwidthPerShard provides download bandwidth in the same
// format as `sctool progress` for regular restore task.
func (rs *restoreStats) averageBandwidthPerShard() string {
	if rs.totalDownloadTimeMS.Load() <= 0 {
		return "unknown"
	}
	bs := rs.totalDownloadBytes.Load() * 1000 / rs.totalDownloadTimeMS.Load()
	if rs.totalShards <= 0 {
		return sizesuffix.SizeSuffix(bs).String() + "/s"
	}
	return sizesuffix.SizeSuffix(bs/rs.totalShards).String() + "/s/shard"
}

// progress returns the percentage of restored bytes so far.
func (rs *restoreStats) progress() float64 {
	if rs.totalBytesToRestore == 0 {
		return 100
	}
	restoredBytes := rs.totalDownloadBytes.Load()
	if restoredBytes == 0 {
		return 0
	}
	return float64(restoredBytes) / float64(rs.totalBytesToRestore) * 100.0
}
