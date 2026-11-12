// Copyright (C) 2026 ScyllaDB

package restore

import (
	"maps"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/service/restore/tablet"
	cqlTable "github.com/scylladb/scylla-manager/v3/pkg/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
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

			w.initMetrics(Workload{RcloneNativeWorkload: RcloneNativeWorkload{TotalSize: tc.workloadSize}})

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

func TestTabletAwareRestoreMetrics(t *testing.T) {
	const (
		ks   = "ks"
		tab1 = "tab1"
		tab2 = "tab2"
		tag  = "sm_20260101000000UTC"
	)
	var (
		reg             = prometheus.NewPedanticRegistry()
		clusterID       = uuid.MustRandom()
		backupClusterID = uuid.MustRandom()
		w               = &tablesWorker{
			worker: worker{
				metrics: metrics.NewRestoreMetrics().MustRegisterWith(reg),
				run: &Run{
					ClusterID:   clusterID,
					SnapshotTag: tag,
					Units:       []Unit{{Keyspace: ks, Size: 300}},
				},
			},
			progress: NewTotalRestoreProgress(300),
		}
		tab1Meta  = tablet.TableMeta{Table: cqlTable.CQLTable{Keyspace: ks, Name: tab1}, ClusterID: backupClusterID, SnapshotTag: tag, Size: 100}
		tab2Meta  = tablet.TableMeta{Table: cqlTable.CQLTable{Keyspace: ks, Name: tab2}, ClusterID: backupClusterID, SnapshotTag: tag, Size: 200}
		workload  = tablet.Workload{tab1Meta.Table: tab1Meta, tab2Meta.Table: tab2Meta}
		remaining = func(tab string) float64 {
			// Tablet aware restore series are labeled with the backed up cluster, but not with location, dc or node
			return gatherGauge(t, reg, "scylla_manager_restore_remaining_bytes", prometheus.Labels{
				"cluster":      backupClusterID.String(),
				"snapshot_tag": tag,
				"location":     "",
				"dc":           "",
				"node":         "",
				"keyspace":     ks,
				"table":        tab,
			})
		}
		// pct mirrors CurrentProgress to avoid float rounding differences
		pct = func(restored int64) float64 {
			return float64(restored) / float64(w.progress.totalBytesToRestore) * 100
		}
		validate = func(remainingTab1, remainingTab2, expectedProgress float64) {
			t.Helper()
			if got := remaining(tab1); got != remainingTab1 {
				t.Errorf("Expected %s remaining bytes %v, got %v", tab1, remainingTab1, got)
			}
			if got := remaining(tab2); got != remainingTab2 {
				t.Errorf("Expected %s remaining bytes %v, got %v", tab2, remainingTab2, got)
			}
			// Progress is labeled with the restored cluster
			got := gatherGauge(t, reg, "scylla_manager_restore_progress", prometheus.Labels{
				"cluster":      clusterID.String(),
				"snapshot_tag": tag,
			})
			if got != expectedProgress {
				t.Errorf("Expected progress %v, got %v", expectedProgress, got)
			}
		}
	)

	w.initMetrics(Workload{TabletAwareWorkload: workload})
	validate(100, 200, 0)

	// Restore 1 of 4 tab1 sstables
	scheduled := tablet.RunProgress{Keyspace: ks, Table: tab1, Size: 100}
	polled := scheduled
	polled.RestoredSSTables = 1
	polled.TotalSSTables = 4
	w.onTabletProgress(tab1Meta, scheduled, polled)
	validate(75, 200, pct(25))

	// Skip tab2 restored by the previous run
	cloned := tablet.RunProgress{Keyspace: ks, Table: tab2, Size: 200, RestoredSSTables: 8, TotalSSTables: 8, CompletedAt: timeutc.Now()}
	w.onTabletProgress(tab2Meta, tablet.RunProgress{}, cloned)
	validate(75, 0, pct(225))

	// Finish tab1
	done := polled
	done.RestoredSSTables = 4
	done.CompletedAt = timeutc.Now()
	w.onTabletProgress(tab1Meta, polled, done)
	validate(0, 0, pct(300))
}

// gatherGauge returns the value of the single series of the gauge with the given labels.
func gatherGauge(t *testing.T, reg *prometheus.Registry, name string, labels prometheus.Labels) float64 {
	t.Helper()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var found []float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := make(prometheus.Labels, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			if maps.Equal(got, labels) {
				found = append(found, m.GetGauge().GetValue())
			}
		}
	}
	if len(found) != 1 {
		t.Fatalf("Expected a single %s series with labels %v, got %d", name, labels, len(found))
	}
	return found[0]
}
