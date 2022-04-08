// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fs/rc/jobs"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone/rcserver/internal"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
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
				Error:       "joberror",
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
				{Name: "file2", Error: errors.New("error1"), Bytes: 1, Size: 13},
				{Name: "file3", Error: errors.New("error2"), Bytes: 7, Size: 7},
				{Name: "file4", Bytes: 5, Size: 5},
			}},
			Progress: jobProgress{
				Status:      JobError,
				StartedAt:   time1,
				CompletedAt: time2,
				Uploaded:    15, // 10+5
				Failed:      20, // 13+7
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

func TestAggregateJobInfoIssue2828(t *testing.T) {
	table := []struct {
		Name     string
		Job      *jobs.Job
		Trans    []accounting.TransferSnapshot
		Progress jobProgress
	}{
		{
			Name: "Move 2 files",
			Job: &jobs.Job{
				ID:    1626342033,
				Group: "job/1626342033",
			},
			Trans: []accounting.TransferSnapshot{
				mustParseTransferSnapshot(`{"error":"","name":"1626342032","size":104857600,"bytes":0,"checked":true,"started_at":"2021-07-15T11:40:32.245168386+02:00","completed_at":"2021-07-15T11:40:42.255325603+02:00","group":"job/1626342033"}`),
				mustParseTransferSnapshot(`{"error":"","name":"1626342032","size":104857600,"bytes":104857600,"checked":false,"started_at":"2021-07-15T11:40:32.245171561+02:00","completed_at":"2021-07-15T11:40:42.254909299+02:00","group":"job/1626342033"}`),
				mustParseTransferSnapshot(`{"error":"","name":"1626342032","size":104857600,"bytes":0,"checked":true,"started_at":"2021-07-15T11:40:42.254925004+02:00","completed_at":"2021-07-15T11:40:42.255321278+02:00","group":"job/1626342033"}`),
				mustParseTransferSnapshot(`{"error":"","name":"xxx","size":22779,"bytes":0,"checked":true,"started_at":"2021-07-15T11:40:32.245171925+02:00","completed_at":"2021-07-15T11:40:32.260600003+02:00","group":"job/1626342033"}`),
				mustParseTransferSnapshot(`{"error":"","name":"xxx","size":22779,"bytes":22779,"checked":false,"started_at":"2021-07-15T11:40:32.245180301+02:00","completed_at":"2021-07-15T11:40:32.260488786+02:00","group":"job/1626342033"}`),
				mustParseTransferSnapshot(`{"error":"","name":"xxx","size":22779,"bytes":0,"checked":true,"started_at":"2021-07-15T11:40:32.260491542+02:00","completed_at":"2021-07-15T11:40:32.260598585+02:00","group":"job/1626342033"}`),
			},
			Progress: jobProgress{
				Status:   JobRunning,
				Uploaded: 104880379, // 104857600+22779
			},
		},
		{
			Name: "Move deduplicate xxx",
			Job: &jobs.Job{
				ID:    1626342551,
				Group: "job/1626342551",
			},
			Trans: []accounting.TransferSnapshot{
				mustParseTransferSnapshot(`{"error":"","name":"1626342550","size":104857600,"bytes":0,"checked":true,"started_at":"2021-07-15T11:49:10.180542084+02:00","completed_at":"2021-07-15T11:49:20.198279729+02:00","group":"job/1626342551"}`),
				mustParseTransferSnapshot(`{"error":"","name":"1626342550","size":104857600,"bytes":104857600,"checked":false,"started_at":"2021-07-15T11:49:10.180544173+02:00","completed_at":"2021-07-15T11:49:20.197642735+02:00","group":"job/1626342551"}`),
				mustParseTransferSnapshot(`{"error":"","name":"xxx","size":22779,"bytes":0,"checked":true,"started_at":"2021-07-15T11:49:10.180547843+02:00","completed_at":"2021-07-15T11:49:10.180683285+02:00","group":"job/1626342551"}`),
				mustParseTransferSnapshot(`{"error":"","name":"xxx","size":22779,"bytes":0,"checked":true,"started_at":"2021-07-15T11:49:10.180576422+02:00","completed_at":"2021-07-15T11:49:10.180682212+02:00","group":"job/1626342551"}`),
				mustParseTransferSnapshot(`{"error":"","name":"1626342550","size":104857600,"bytes":0,"checked":true,"started_at":"2021-07-15T11:49:20.197650303+02:00","completed_at":"2021-07-15T11:49:20.198272788+02:00","group":"job/1626342551"}`),
			},
			Progress: jobProgress{
				Status:   JobRunning,
				Uploaded: 104857600,
				Skipped:  22779,
			},
		},
		{
			Name: "Copy 2 files",
			Job: &jobs.Job{
				ID:    1626343140,
				Group: "job/1626343140",
			},
			Trans: []accounting.TransferSnapshot{
				mustParseTransferSnapshot(`{"error":"","name":"1626343139","size":104857600,"bytes":104857600,"checked":false,"started_at":"2021-07-15T11:58:59.696697821+02:00","completed_at":"2021-07-15T11:59:09.713673731+02:00","group":"job/1626343140"}`),
				mustParseTransferSnapshot(`{"error":"","name":"xxx","size":22779,"bytes":22779,"checked":false,"started_at":"2021-07-15T11:58:59.696709814+02:00","completed_at":"2021-07-15T11:58:59.709815197+02:00","group":"job/1626343140"}`),
			},
			Progress: jobProgress{
				Status:   JobRunning,
				Uploaded: 104880379, // 104857600+22779
			},
		},
		{
			Name: "Move deduplicate xxx",
			Job: &jobs.Job{
				ID:    1626343294,
				Group: "job/1626343294",
			},
			Trans: []accounting.TransferSnapshot{
				mustParseTransferSnapshot(`{"error":"","name":"1626343293","size":104857600,"bytes":104857600,"checked":false,"started_at":"2021-07-15T12:01:33.717027651+02:00","completed_at":"2021-07-15T12:01:43.72221275+02:00","group":"job/1626343294"}`),
				mustParseTransferSnapshot(`{"error":"","name":"xxx","size":22779,"bytes":0,"checked":true,"started_at":"2021-07-15T12:01:33.717030897+02:00","completed_at":"2021-07-15T12:01:33.717079728+02:00","group":"job/1626343294"}`),
			},
			Progress: jobProgress{
				Status:   JobRunning,
				Uploaded: 104857600,
				Skipped:  22779,
			},
		},
	}

	for i := range table {
		t.Run(table[i].Name, func(t *testing.T) {
			job := rc.Params{}
			if err := rc.Reshape(&job, table[i].Job); err != nil {
				t.Fatal(err)
			}
			p := aggregateJobInfo(job, rc.Params{}, rc.Params{"transferred": table[i].Trans})
			if diff := cmp.Diff(table[i].Progress, p); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func mustParseTransferSnapshot(s string) accounting.TransferSnapshot {
	var a accounting.TransferSnapshot
	if err := json.Unmarshal([]byte(s), &a); err != nil {
		panic(err)
	}
	return a
}
