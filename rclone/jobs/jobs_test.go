// Copyright (C) 2017 ScyllaDB

package jobs

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/uuid"
)

func TestNewJobs(t *testing.T) {
	defer cleanupJobs()
	jobs := newJobs()
	assertEqual(t, 0, len(jobs.jobs))
}

func TestJobsKickExpire(t *testing.T) {
	defer cleanupJobs()
	jobs := newJobs()
	jobs.expireInterval = time.Millisecond
	assertEqual(t, false, jobs.expireRunning)
	jobs.kickExpire()
	jobs.mu.Lock()
	assertEqual(t, true, jobs.expireRunning)
	jobs.mu.Unlock()
	time.Sleep(10 * time.Millisecond)
	jobs.mu.Lock()
	assertEqual(t, false, jobs.expireRunning)
	jobs.mu.Unlock()
}

func TestJobsExpire(t *testing.T) {
	defer cleanupJobs()
	wait := make(chan struct{})
	jobs := newJobs()
	jobs.expireInterval = time.Millisecond
	assertEqual(t, false, jobs.expireRunning)
	job := jobs.NewAsyncJob(func(ctx context.Context, in rc.Params) (rc.Params, error) {
		defer close(wait)
		return in, nil
	}, rc.Params{})
	<-wait
	assertEqual(t, 1, len(jobs.jobs))
	jobs.Expire()
	assertEqual(t, 1, len(jobs.jobs))
	jobs.mu.Lock()
	job.mu.Lock()
	job.EndTime = timeutc.Now().Add(-fs.Config.RcJobExpireDuration - 60*time.Second)
	assertEqual(t, true, jobs.expireRunning)
	job.mu.Unlock()
	jobs.mu.Unlock()
	time.Sleep(10 * time.Millisecond)
	jobs.mu.Lock()
	assertEqual(t, false, jobs.expireRunning)
	assertEqual(t, 0, len(jobs.jobs))
	jobs.mu.Unlock()
}

var noopFn = func(ctx context.Context, in rc.Params) (rc.Params, error) {
	return nil, nil
}

func TestJobsIDs(t *testing.T) {
	defer cleanupJobs()
	jobs := newJobs()
	job1 := jobs.NewAsyncJob(noopFn, rc.Params{})
	job2 := jobs.NewAsyncJob(noopFn, rc.Params{})
	wantIDs := []string{job1.ID.String(), job2.ID.String()}
	gotIDs := jobs.IDs()
	assertEqual(t, 2, len(gotIDs))
	if gotIDs[0] != wantIDs[0] {
		gotIDs[0], gotIDs[1] = gotIDs[1], gotIDs[0]
	}
	assertEqual(t, wantIDs, gotIDs)
}

func TestJobsGet(t *testing.T) {
	defer cleanupJobs()
	jobs := newJobs()
	job := jobs.NewAsyncJob(noopFn, rc.Params{})
	assertEqual(t, job, jobs.Get(job.ID.String()))
	if j := jobs.Get("asdfsadfadsf"); j != nil {
		t.Errorf("Expected nil got %+v", j)
	}
}

var longFn = func(ctx context.Context, in rc.Params) (rc.Params, error) {
	time.Sleep(1 * time.Hour)
	return nil, nil
}

var shortFn = func(ctx context.Context, in rc.Params) (rc.Params, error) {
	time.Sleep(time.Millisecond)
	return nil, nil
}

var ctxFn = func(ctx context.Context, in rc.Params) (rc.Params, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

const (
	sleepTime      = 100 * time.Millisecond
	floatSleepTime = float64(sleepTime) / 1E9 / 2
)

// sleep for some time so job.Duration is non-0
func sleepJob() {
	time.Sleep(sleepTime)
}

func TestJobFinish(t *testing.T) {
	defer cleanupJobs()
	jobs := newJobs()
	job := jobs.NewAsyncJob(longFn, rc.Params{})
	sleepJob()

	assertEqual(t, true, job.EndTime.IsZero())
	assertEqual(t, rc.Params(nil), job.Output)
	assertEqual(t, 0.0, job.Duration)
	assertEqual(t, "", job.Error)
	assertEqual(t, false, job.Success)
	assertEqual(t, false, job.Finished)

	wantOut := rc.Params{"a": 1}
	job.finish(wantOut, nil)

	assertEqual(t, false, job.EndTime.IsZero())
	assertEqual(t, wantOut, job.Output)
	assertEqual(t, job.Duration >= floatSleepTime, true)
	assertEqual(t, "", job.Error)
	assertEqual(t, true, job.Success)
	assertEqual(t, true, job.Finished)

	job = jobs.NewAsyncJob(longFn, rc.Params{})
	sleepJob()
	job.finish(nil, nil)

	assertEqual(t, false, job.EndTime.IsZero())
	assertEqual(t, rc.Params{}, job.Output)
	assertEqual(t, job.Duration >= floatSleepTime, true)
	assertEqual(t, "", job.Error)
	assertEqual(t, true, job.Success)
	assertEqual(t, true, job.Finished)

	job = jobs.NewAsyncJob(longFn, rc.Params{})
	sleepJob()
	job.finish(wantOut, errors.New("potato"))

	assertEqual(t, false, job.EndTime.IsZero())
	assertEqual(t, wantOut, job.Output)
	assertEqual(t, job.Duration >= floatSleepTime, true)
	assertEqual(t, "potato", job.Error)
	assertEqual(t, false, job.Success)
	assertEqual(t, true, job.Finished)
}

// We've tested the functionality of run() already as it is
// part of NewJob, now just test the panic catching
func TestJobRunPanic(t *testing.T) {
	defer cleanupJobs()
	wait := make(chan struct{})
	boom := func(ctx context.Context, in rc.Params) (rc.Params, error) {
		sleepJob()
		defer close(wait)
		panic("boom")
	}

	jobs := newJobs()
	job := jobs.NewAsyncJob(boom, rc.Params{})
	<-wait
	runtime.Gosched() // yield to make sure job is updated

	// Wait a short time for the panic to propagate
	for i := uint(0); i < 10; i++ {
		job.mu.Lock()
		e := job.Error
		job.mu.Unlock()
		if e != "" {
			break
		}
		time.Sleep(time.Millisecond << i)
	}

	job.mu.Lock()
	assertEqual(t, false, job.EndTime.IsZero())
	assertEqual(t, rc.Params{}, job.Output)
	assertEqual(t, job.Duration >= floatSleepTime, true)
	assertContains(t, job.Error, "panic received: boom")
	assertEqual(t, false, job.Success)
	assertEqual(t, true, job.Finished)
	job.mu.Unlock()
}

func TestJobsNewJob(t *testing.T) {
	defer cleanupJobs()
	jobs := newJobs()
	job := jobs.NewAsyncJob(noopFn, rc.Params{})
	assertEqual(t, true, job.ID != uuid.Nil)
	assertEqual(t, job, jobs.Get(job.ID.String()))
	if job.Stop == nil {
		t.Error("Expected Stop to has value")
	}
}

func TestStartJob(t *testing.T) {
	defer cleanupJobs()
	out, err := StartAsyncJob(shortFn, rc.Params{})
	assertEqual(t, nil, err)
	if len(out["jobid"].(string)) < 36 {
		t.Errorf("Expected jobid to have uuid value got %s", out["jobid"])
	}
}

func TestExecuteJob(t *testing.T) {
	defer cleanupJobs()
	_, id, err := ExecuteJob(context.Background(), shortFn, rc.Params{})
	assertEqual(t, nil, err)
	if len(id) < 36 {
		t.Errorf("Expected jobid to have uuid value got %s", id)
	}
}

func TestRcJobStatus(t *testing.T) {
	defer cleanupJobs()
	res, err := StartAsyncJob(longFn, rc.Params{})
	assertEqual(t, nil, err)

	call := rc.Calls.Get("job/status")
	if call == nil {
		t.Error("Expected call to not be nil")
	}
	in := rc.Params{"jobid": res["jobid"]}
	out, err := call.Fn(context.Background(), in)
	assertEqual(t, nil, err)
	if out == nil || reflect.DeepEqual(out, rc.Params{}) {
		t.Errorf("Expected non empty result")
	}
	assertEqual(t, res["jobid"], out["id"])
	assertEqual(t, "", out["error"])
	assertEqual(t, false, out["finished"])
	assertEqual(t, false, out["success"])

	in = rc.Params{"jobid": "123123123"}
	_, err = call.Fn(context.Background(), in)
	if err == nil {
		t.Error("Expected error got nil")
	}
	assertContains(t, err.Error(), "job not found")

	in = rc.Params{"jobidx": "123123123"}
	_, err = call.Fn(context.Background(), in)
	if err == nil {
		t.Error("Expected error got nil")
	}
	assertContains(t, err.Error(), "Didn't find key")
}

func TestRcJobList(t *testing.T) {
	defer cleanupJobs()
	_, err := StartAsyncJob(longFn, rc.Params{})
	assertEqual(t, nil, err)

	call := rc.Calls.Get("job/list")
	if call == nil {
		t.Error("Expected call to not be nil")
	}
	in := rc.Params{}
	out, err := call.Fn(context.Background(), in)
	assertEqual(t, nil, err)
	if out == nil || reflect.DeepEqual(out, rc.Params{}) {
		t.Errorf("Expected non empty result")
	}
	assertEqual(t, 1, len(out["jobids"].([]string)))
}

func TestRcAsyncJobStop(t *testing.T) {
	defer cleanupJobs()
	res, err := StartAsyncJob(ctxFn, rc.Params{})
	assertEqual(t, nil, err)

	call := rc.Calls.Get("job/stop")
	if call == nil {
		t.Error("Expected call to not be nil")
	}
	in := rc.Params{"jobid": res["jobid"]}
	out, err := call.Fn(context.Background(), in)
	assertEqual(t, nil, err)
	assertEqual(t, rc.Params{}, out)

	in = rc.Params{"jobid": "123123123"}
	_, err = call.Fn(context.Background(), in)
	if err == nil {
		t.Error("Expected error got nil")
	}
	assertContains(t, err.Error(), "job not found")

	in = rc.Params{"jobidx": "123123123"}
	_, err = call.Fn(context.Background(), in)
	if err == nil {
		t.Error("Expected error got nil")
	}
	assertContains(t, err.Error(), "Didn't find key")

	time.Sleep(10 * time.Millisecond)

	call = rc.Calls.Get("job/status")
	if call == nil {
		t.Error("Expected call to not be nil")
	}
	in = rc.Params{"jobid": res["jobid"]}
	out, err = call.Fn(context.Background(), in)
	assertEqual(t, nil, err)
	if out == nil || reflect.DeepEqual(out, rc.Params{}) {
		t.Errorf("Expected non empty result")
	}
	assertEqual(t, res["jobid"], out["id"])
	assertEqual(t, "context canceled", out["error"])
	assertEqual(t, true, out["finished"])
	assertEqual(t, false, out["success"])
}

func TestRcSyncJobStop(t *testing.T) {
	defer cleanupJobs()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, id, err := ExecuteJob(ctx, ctxFn, rc.Params{})
		if err == nil {
			t.Error("Expected error got nil")
		}
		if len(id) < 36 {
			t.Errorf("Expected jobid to have uuid value got %s", id)
		}
	}()

	time.Sleep(10 * time.Millisecond)

	call := rc.Calls.Get("job/list")
	if call == nil {
		t.Error("Expected call to not be nil")
	}
	out, err := call.Fn(context.Background(), rc.Params{})
	id := out["jobids"].([]string)[0]

	call = rc.Calls.Get("job/stop")
	if call == nil {
		t.Error("Expected call to not be nil")
	}
	in := rc.Params{"jobid": id}
	out, err = call.Fn(context.Background(), in)
	assertEqual(t, nil, err)
	assertEqual(t, rc.Params{}, out)

	in = rc.Params{"jobid": "123123123"}
	_, err = call.Fn(context.Background(), in)
	if err == nil {
		t.Error("Expected error got nil")
	}
	assertContains(t, err.Error(), "job not found")

	in = rc.Params{"jobidx": "123123123"}
	_, err = call.Fn(context.Background(), in)
	if err == nil {
		t.Error("Expected error got nil")
	}
	assertContains(t, err.Error(), "Didn't find key")

	cancel()
	time.Sleep(10 * time.Millisecond)

	call = rc.Calls.Get("job/status")
	if call == nil {
		t.Error("Expected call to not be nil")
	}
	in = rc.Params{"jobid": id}
	out, err = call.Fn(context.Background(), in)
	assertEqual(t, nil, err)
	if out == nil || reflect.DeepEqual(out, rc.Params{}) {
		t.Errorf("Expected non empty result")
	}
	assertEqual(t, id, out["id"])
	assertEqual(t, "context canceled", out["error"])
	assertEqual(t, true, out["finished"])
	assertEqual(t, false, out["success"])
}

func assertEqual(t *testing.T, expect, got interface{}) {
	t.Helper()
	if reflect.DeepEqual(expect, got) {
		return
	}
	t.Errorf("Not equal %+v != %+v", expect, got)
}

func assertContains(t *testing.T, s, subs string) {
	t.Helper()
	if !strings.Contains(s, subs) {
		t.Errorf("Expected %s to contain %s", s, subs)
	}
}

func cleanupJobs() {
	running.cleanup()
}
