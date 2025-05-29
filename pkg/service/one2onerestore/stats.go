// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"sync/atomic"

	"github.com/scylladb/scylla-manager/v3/pkg/util/sizesuffix"
)

// restoreStats is used to collect some run statistics.
type restoreStats struct {
	totalShards         int64
	totalDownloadTimeMS int64
	totalDownloadBytes  int64
}

func newRestoreStats(workload []hostWorkload) *restoreStats {
	var sumShards int
	for _, w := range workload {
		sumShards += w.host.ShardCount
	}
	return &restoreStats{
		totalShards: int64(sumShards),
	}
}

// incrementDownloadStats atomically increment downloaded bytes and duration in milliseconds.
func (rs *restoreStats) incrementDownloadStats(downloadedBytes, tookMS int64) {
	atomic.AddInt64(&rs.totalDownloadBytes, downloadedBytes)
	atomic.AddInt64(&rs.totalDownloadTimeMS, tookMS)
}

// averageBandwidthPerShard provides download bandwidth in the same
// format as `sctool progress` for regular restore task.
func (rs *restoreStats) averageBandwidthPerShard() string {
	if rs.totalDownloadTimeMS <= 0 {
		return "unknown"
	}
	bs := rs.totalDownloadBytes * 1000 / rs.totalDownloadTimeMS
	if rs.totalShards <= 0 {
		return sizesuffix.SizeSuffix(bs).String() + "/s"
	}
	return sizesuffix.SizeSuffix(bs/rs.totalShards).String() + "/s/shard"
}
