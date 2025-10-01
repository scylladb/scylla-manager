// Copyright (C) 2025 ScyllaDB

package restore

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestAggregateProgress(t *testing.T) {
	var (
		taskStart = timeutc.MustParse(time.RFC3339, "2024-02-23T01:12:00Z")
		now       = taskStart.Add(10 * time.Second)
		host      = "h"
		ks        = "ks"
		tab1      = "tab1"
		tab2      = "tab2"
		// Make sure that the sizes can be divided by 2 and 3
		tab1size = int64(2 * 3 * 10)
		tab2size = int64(2 * 3 * 20)
	)

	run := Run{
		ClusterID:   uuid.NewFromUint64(10, 100),
		TaskID:      uuid.NewFromUint64(20, 200),
		ID:          uuid.NewFromUint64(30, 300),
		SnapshotTag: backupspec.SnapshotTagAt(taskStart.Add(-time.Second)),
		Stage:       StageData,
		Units: []Unit{
			{
				Keyspace: ks,
				Size:     tab1size + tab2size,
				Tables: []Table{
					{Table: tab1, TombstoneGC: modeRepair, Size: tab1size},
					{Table: tab2, TombstoneGC: modeTimeout, Size: tab2size},
				},
			},
		},
		Views: []RestoredView{
			{
				View: View{
					Keyspace:  ks,
					Name:      "mv",
					Type:      MaterializedView,
					BaseTable: tab1,
				},
				CreateStmt: "CREATE",
			},
		},
	}

	timePtr := func(t time.Time) *time.Time {
		return &t
	}

	// The template RunProgresses use native Scylla API
	fullTab1 := &RunProgress{
		ClusterID:           run.ClusterID,
		TaskID:              run.TaskID,
		RunID:               run.ID,
		RemoteSSTableDir:    "path/" + tab1,
		Keyspace:            ks,
		Table:               tab1,
		SSTableID:           []string{"1", "2"},
		Host:                host,
		ShardCnt:            1,
		AgentJobID:          1,
		RestoreStartedAt:    &taskStart,
		RestoreCompletedAt:  timePtr(taskStart.Add(2 * time.Second)),
		DownloadStartedAt:   &taskStart,
		DownloadCompletedAt: timePtr(taskStart.Add(time.Second)),
		Restored:            tab1size,
		Downloaded:          tab1size,
	}
	fullTab2 := &RunProgress{
		ClusterID:           run.ClusterID,
		TaskID:              run.TaskID,
		RunID:               run.ID,
		RemoteSSTableDir:    "path/" + tab2,
		Keyspace:            ks,
		Table:               tab2,
		SSTableID:           []string{"3", "4"},
		Host:                host,
		ShardCnt:            1,
		AgentJobID:          1,
		RestoreStartedAt:    &taskStart,
		RestoreCompletedAt:  timePtr(taskStart.Add(10 * time.Second)),
		DownloadStartedAt:   &taskStart,
		DownloadCompletedAt: timePtr(taskStart.Add(5 * time.Second)),
		Restored:            tab2size,
		Downloaded:          tab2size,
	}

	// Transforms RunProgress from native Scylla API to Rclone API
	const unsetCompletedAt = -1
	setRclone := func(rp *RunProgress, d, v int64,
		downloadStartFromNowInSec, downloadEndFromNowInSec, restoreEndFromNowInSec int) *RunProgress {
		clone := *rp
		clone.AgentJobID = 1
		clone.ScyllaTaskID = ""

		clone.RestoreStartedAt = timePtr(taskStart.Add(time.Duration(downloadStartFromNowInSec) * time.Second))
		clone.DownloadStartedAt = timePtr(taskStart.Add(time.Duration(downloadStartFromNowInSec) * time.Second))
		if downloadEndFromNowInSec != unsetCompletedAt {
			clone.DownloadCompletedAt = timePtr(taskStart.Add(time.Duration(downloadEndFromNowInSec) * time.Second))
		} else {
			clone.DownloadCompletedAt = nil
		}
		if restoreEndFromNowInSec != unsetCompletedAt {
			clone.Restored = d + v
			clone.RestoreCompletedAt = timePtr(taskStart.Add(time.Duration(restoreEndFromNowInSec) * time.Second))
		} else {
			clone.Restored = 0
			clone.RestoreCompletedAt = nil
		}
		clone.Downloaded = d
		clone.VersionedDownloaded = v
		return &clone
	}
	setScylla := func(rp *RunProgress, r int64, restoreStartFromNowInSec, restoreEndFromNowInSec int) *RunProgress {
		clone := *rp
		clone.AgentJobID = 0
		clone.ScyllaTaskID = "1"
		clone.DownloadStartedAt = nil
		clone.DownloadCompletedAt = nil
		clone.Downloaded = 0
		clone.VersionedDownloaded = 0

		clone.RestoreStartedAt = timePtr(taskStart.Add(time.Duration(restoreStartFromNowInSec) * time.Second))
		if restoreEndFromNowInSec != unsetCompletedAt {
			clone.RestoreCompletedAt = timePtr(taskStart.Add(time.Duration(restoreEndFromNowInSec) * time.Second))
		} else {
			clone.RestoreCompletedAt = nil
		}
		clone.Restored = r
		return &clone
	}

	testCases := []struct {
		name     string
		progress []*RunProgress
	}{
		{
			name: "complete restore",
			progress: []*RunProgress{
				fullTab1,
				fullTab2,
			},
		},
		{
			name:     "no progress",
			progress: []*RunProgress{},
		},
		{
			name: "zero progress",
			progress: []*RunProgress{
				setRclone(fullTab1, 0, 0, 0, unsetCompletedAt, unsetCompletedAt),
				setRclone(fullTab2, 0, 0, 0, unsetCompletedAt, unsetCompletedAt),
			},
		},
		{
			name: "full tab1 zero tab2",
			progress: []*RunProgress{
				fullTab1,
				setRclone(fullTab2, 0, 0, 0, unsetCompletedAt, unsetCompletedAt),
			},
		},
		{
			name: "partial tab1 partial tab2",
			progress: []*RunProgress{
				setRclone(fullTab1, tab1size/2, tab1size/2, 0, 5, unsetCompletedAt),
				setRclone(fullTab2, tab2size/3, 0, 0, 3, 7),
				setRclone(fullTab2, tab2size/3, 0, 2, unsetCompletedAt, unsetCompletedAt),
			},
		},
		{
			name: "full tab1 partial tab2",
			progress: []*RunProgress{
				setRclone(fullTab1, tab1size/2, tab1size/2, 5, 7, 10),
				setRclone(fullTab2, tab2size/3, 0, 1, 2, 3),
				setRclone(fullTab2, tab2size/3, tab2size/3, 1, 2, unsetCompletedAt),
			},
		},
		{
			name: "scylla API full tab1 partial tab2",
			progress: []*RunProgress{
				setScylla(fullTab1, tab1size, 0, 10),
				setScylla(fullTab2, tab2size/3, 1, 3),
				setScylla(fullTab2, tab2size/3, 1, unsetCompletedAt),
			},
		},
		{
			name: "scylla API partial tab1 partial tab2",
			progress: []*RunProgress{
				setScylla(fullTab1, tab1size/2, 0, unsetCompletedAt),
				setScylla(fullTab2, tab2size/3, 0, 7),
				setScylla(fullTab2, tab2size/3, 0, unsetCompletedAt),
			},
		},
		{
			name: "mixed API full tab1 partial tab2",
			progress: []*RunProgress{
				fullTab1,
				setScylla(fullTab2, tab2size/3, 1, 5),
				setScylla(fullTab2, tab2size/3, 0, unsetCompletedAt),
			},
		},
		{
			name: "mixed API full tab1 full tab2",
			progress: []*RunProgress{
				fullTab1,
				setScylla(fullTab2, tab2size/3, 1, 3),
				setRclone(fullTab2, tab2size/3, tab2size/3, 1, 2, 4),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pr := aggregateProgress(&run, slices.Values(tc.progress), now)
			testutils.SaveGoldenJSONFileIfNeeded(t, pr)
			var expected Progress
			testutils.LoadGoldenJSONFile(t, &expected)
			if diff := cmp.Diff(pr, expected, cmp.Exporter(func(r reflect.Type) bool {
				return true
			})); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
