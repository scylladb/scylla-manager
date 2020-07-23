// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
)

var (
	backupPercentProgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "backup",
		Name:      "percent_progress",
		Help:      "Current backup percent progress.",
	}, []string{"cluster", "task", "host", "keyspace"})

	backupBytesDone = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "backup",
		Name:      "bytes_done",
		Help:      "Number of bytes uploaded so far.",
	}, []string{"cluster", "task", "host", "keyspace"})

	backupBytesLeft = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "backup",
		Name:      "bytes_left",
		Help:      "Number of bytes left for backup completion.",
	}, []string{"cluster", "task", "host", "keyspace"})

	backupUploadRetries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "backup",
		Name:      "upload_retries",
		Help:      "Number of upload retries",
	}, []string{"cluster", "task", "host", "keyspace"})
)

func init() {
	prometheus.MustRegister(
		backupPercentProgress,
		backupBytesDone,
		backupBytesLeft,
		backupUploadRetries,
	)
}

type runProgressFunc func(ctx context.Context) (*Run, Progress, error)

func saveMetrics(p progress, labels prometheus.Labels) {
	totalDone, totalLeft := p.ByteProgress()
	backupBytesLeft.With(labels).Set(float64(totalLeft))
	backupBytesDone.With(labels).Set(float64(totalDone))
	backupPercentProgress.With(labels).Set(float64(p.PercentComplete()))
}

func updateFunc(ctx context.Context, runProgress runProgressFunc, logger log.Logger) func() {
	return func() {
		run, p, err := runProgress(ctx)
		if err != nil {
			logger.Error(ctx, "Failed to aggregate backup progress metrics", "error", err)
			return
		}

		taskID := run.TaskID.String()

		// Aggregated total progress
		totalLabels := prometheus.Labels{
			"cluster":  run.clusterName,
			"task":     taskID,
			"host":     "",
			"keyspace": "",
		}
		saveMetrics(p.progress, totalLabels)

		for _, host := range p.Hosts {
			// Aggregated keyspace progress
			for _, keyspace := range host.Keyspaces {
				keyspaceLabels := prometheus.Labels{
					"cluster":  run.clusterName,
					"task":     taskID,
					"host":     host.Host,
					"keyspace": keyspace.Keyspace,
				}
				saveMetrics(keyspace.progress, keyspaceLabels)
			}

			// Aggregated host progress
			hostLabels := prometheus.Labels{
				"cluster":  run.clusterName,
				"task":     taskID,
				"host":     host.Host,
				"keyspace": "",
			}
			saveMetrics(host.progress, hostLabels)
		}
	}
}
