// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fs/rc/jobs"
	"github.com/scylladb/scylla-manager/pkg/rclone/rcserver/internal"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

func filterRcCallsForTests() {
	internal.RcloneSupportedCalls.Add(
		"rc/noop",
		"rc/error",
	)
	filterRcCalls()
}

func TestAggregateJobInfo(t *testing.T) {
	dur1 := 10 * time.Second
	time1 := timeutc.Now()
	time2 := time1.Add(dur1)

	table := []struct {
		Name     string
		Job      *jobs.Job
		Stat     rc.Params
		Trans    rc.Params
		Progress jobProgress
	}{
		{
			Name:  "Global stats",
			Job:   nil,
			Stat:  rc.Params{"transferring": []rc.Params{{"name": "file1", "bytes": int64(10)}}},
			Trans: rc.Params{"transferred": []accounting.TransferSnapshot{{Name: "file2"}}},
			Progress: jobProgress{
				Status:   JobNotFound,
				Uploaded: 10,
			},
		},
		{
			Name:  "Empty stats",
			Job:   nil,
			Stat:  nil,
			Trans: rc.Params{"transferred": []accounting.TransferSnapshot{{}}},
			Progress: jobProgress{
				Status: JobNotFound,
			},
		},
		{
			Name:  "Empty transferred",
			Job:   nil,
			Stat:  rc.Params{"transferring": []rc.Params{{"name": "file1", "bytes": int64(10)}}},
			Trans: nil,
			Progress: jobProgress{
				Status:   JobNotFound,
				Uploaded: 10,
			},
		},
		{
			Name: "Job success",
			Job: &jobs.Job{
				ID:        111,
				Group:     "jobs/111",
				StartTime: time1,
				EndTime:   time2,
				Error:     "",
				Finished:  true,
				Success:   true,
				Duration:  dur1.Seconds(),
			},
			Stat:  rc.Params{"transferring": []rc.Params{{"name": "file1", "bytes": int64(10)}}},
			Trans: rc.Params{"transferred": []accounting.TransferSnapshot{{Name: "file2"}}},
			Progress: jobProgress{
				Status:      JobSuccess,
				StartedAt:   time1,
				CompletedAt: time2,
				Uploaded:    10,
			},
		},
		{
			Name: "Job error",
			Job: &jobs.Job{
				ID:        111,
				Group:     "jobs/111",
				StartTime: time1,
				EndTime:   time2,
				Error:     "joberror",
				Finished:  true,
				Success:   false,
				Duration:  dur1.Seconds(),
			},
			Stat:  rc.Params{"transferring": []rc.Params{{"name": "file1", "bytes": int64(10)}}},
			Trans: rc.Params{"transferred": []accounting.TransferSnapshot{{Name: "file2"}}},
			Progress: jobProgress{
				Status:      JobError,
				StartedAt:   time1,
				CompletedAt: time2,
				Uploaded:    10,
				Error:       "joberror;",
			},
		},
		{
			Name: "Job running",
			Job: &jobs.Job{
				ID:        111,
				Group:     "jobs/111",
				StartTime: time1,
				Error:     "",
				Finished:  false,
				Success:   false,
			},
			Stat: rc.Params{"transferring": []rc.Params{{"name": "file1", "bytes": int64(10)}}},
			Trans: rc.Params{"transferred": []accounting.TransferSnapshot{
				{Name: "file2", Checked: true, Bytes: 13, Size: 13},
			}},
			Progress: jobProgress{
				Status:    JobRunning,
				StartedAt: time1,
				Uploaded:  10,
				Skipped:   13,
			},
		},
		{
			Name: "Transfer errors",
			Job: &jobs.Job{
				ID:        111,
				Group:     "jobs/111",
				StartTime: time1,
				EndTime:   time2,
				Error:     "",
				Finished:  true,
				Success:   false,
				Duration:  dur1.Seconds(),
			},
			Stat: rc.Params{"transferring": []rc.Params{{"name": "file1", "bytes": int64(10)}}},
			Trans: rc.Params{"transferred": []accounting.TransferSnapshot{
				// 12 failed bytes
				{Name: "file2", Error: errors.New("error1"), Bytes: 1, Size: 13},
				// 0 failed bytes but got error
				{Name: "file3", Error: errors.New("error2"), Bytes: 7, Size: 7},
				{Name: "file4", Bytes: 5, Size: 5},
			}},
			Progress: jobProgress{
				Status:      JobError,
				StartedAt:   time1,
				CompletedAt: time2,
				Uploaded:    23, // 10+1+7+5
				Failed:      12,
				Error:       "file2 error1; file3 error2",
			},
		},
	}

	for i := range table {
		t.Run(table[i].Name, func(t *testing.T) {
			job := rc.Params{}
			if err := rc.Reshape(&job, table[i].Job); err != nil {
				t.Fatal(err)
			}
			p := aggregateJobInfo(job, table[i].Stat, table[i].Trans)
			if diff := cmp.Diff(table[i].Progress, p); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
