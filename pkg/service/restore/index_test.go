// Copyright (C) 2026 ScyllaDB

package restore

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestInitMetricsProgress(t *testing.T) {
	testCases := []struct {
		name         string
		unitsSize    int64
		workloadSize int64
		expected     float64
	}{
		{
			name:         "fresh run",
			unitsSize:    100,
			workloadSize: 100,
			expected:     0,
		},
		{
			name:         "resumed run",
			unitsSize:    100,
			workloadSize: 40,
			expected:     60,
		},
		{
			name:         "nothing to restore",
			unitsSize:    0,
			workloadSize: 0,
			expected:     100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			w := &tablesWorker{
				worker: worker{
					metrics: metrics.NewRestoreMetrics().MustRegisterWith(reg),
					run: &Run{
						ClusterID:   uuid.MustRandom(),
						SnapshotTag: "sm_20260101000000UTC",
						Units:       []Unit{{Keyspace: "ks", Size: tc.unitsSize}},
					},
				},
				progress: NewTotalRestoreProgress(tc.unitsSize),
			}

			w.initMetrics(Workload{TotalSize: tc.workloadSize})

			if got := w.progress.CurrentProgress(); got != tc.expected {
				t.Errorf("Expected total progress %v after init, got %v", tc.expected, got)
			}
			if got := gatherProgress(t, reg); got != tc.expected {
				t.Errorf("Expected progress metric %v after init, got %v", tc.expected, got)
			}

			// Restoring the whole workload should complete the progress
			w.progress.Update(tc.workloadSize)
			if got := w.progress.CurrentProgress(); got != 100 {
				t.Errorf("Expected total progress 100 after restoring the workload, got %v", got)
			}
		})
	}
}

// gatherProgress returns the value of the single restore progress metric series.
func gatherProgress(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "scylla_manager_restore_progress" {
			continue
		}
		if cnt := len(mf.GetMetric()); cnt != 1 {
			t.Fatalf("Expected a single progress series, got %d", cnt)
		}
		return mf.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatal("Progress metric not found")
	return 0
}
