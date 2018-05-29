// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/ssh"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap/zapcore"
)

func TestServiceStorageIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)

	s, err := repair.NewService(
		session,
		repair.DefaultConfig(),
		func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return nil, errors.New("not implemented")
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return nil, errors.New("not implemented")
		},
		log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	putRun := func(t *testing.T, r *repair.Run) {
		stmt, names := schema.RepairRun.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(r).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}
	putRunProgress := func(t *testing.T, r *repair.Run) {
		p := repair.RunProgress{
			ClusterID: r.ClusterID,
			TaskID:    r.TaskID,
			RunID:     r.ID,
			Host:      "172.16.1.3",
			Shard:     0,
		}
		stmt, names := schema.RepairRunProgress.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(&p).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("get last run", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusStopped,
		}
		putRun(t, r1)

		r, err := s.GetLastRun(ctx, clusterID, taskID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get last started run nothing to return", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r1)

		_, err := s.GetLastStartedRun(ctx, clusterID, taskID)
		if err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})

	t.Run("get last started run return first", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusStopped,
		}
		putRun(t, r1)

		r2 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r2)

		r, err := s.GetLastStartedRun(ctx, clusterID, taskID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get last started run return first with error", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusDone,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r1)
		putRunProgress(t, r1)

		r2 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusError,
		}
		putRun(t, r2)

		r, err := s.GetLastStartedRun(ctx, clusterID, taskID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r1, mermaidtest.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("stop run", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &repair.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Status:    repair.StatusRunning,
		}

		putRun(t, r0)

		if err := s.StopRepair(ctx, clusterID, taskID, r0.ID); err != nil {
			t.Fatal(err)
		}

		if r1, err := s.GetRun(ctx, clusterID, taskID, r0.ID); err != nil {
			t.Fatal(err)
		} else if r1.Status != repair.StatusStopping {
			t.Fatal(r1.Status)
		}
	})

	t.Run("fix run status", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		task0 := uuid.MustRandom()
		task1 := uuid.MustRandom()

		r0 := &repair.Run{
			ID:        uuid.NewTime(),
			ClusterID: clusterID,
			TaskID:    task0,
			Status:    repair.StatusRunning,
		}
		putRun(t, r0)

		r1 := &repair.Run{
			ID:        uuid.NewTime(),
			ClusterID: clusterID,
			TaskID:    task0,
			Status:    repair.StatusRunning,
		}
		putRun(t, r1)

		r2 := &repair.Run{
			ID:        uuid.NewTime(),
			ClusterID: clusterID,
			TaskID:    task1,
			Status:    repair.StatusStopping,
		}
		putRun(t, r2)

		r3 := &repair.Run{
			ID:        uuid.NewTime(),
			ClusterID: clusterID,
			TaskID:    task1,
			Status:    repair.StatusStopping,
		}
		putRun(t, r3)

		if err := s.FixRunStatus(ctx); err != nil {
			t.Fatal(err)
		}

		if r, err := s.GetRun(ctx, r1.ClusterID, r1.TaskID, r1.ID); err != nil {
			t.Fatal(err)
		} else if r.Status != repair.StatusStopped {
			t.Fatal("invalid status", r.Status)
		}

		if r, err := s.GetRun(ctx, r3.ClusterID, r3.TaskID, r3.ID); err != nil {
			t.Fatal(err)
		} else if r.Status != repair.StatusStopped {
			t.Fatal("invalid status", r.Status)
		}
	})
}

func TestServiceRepairIntegration(t *testing.T) {
	// fix values for testing...
	config := repair.DefaultConfig()
	config.SegmentsPerRepair = 5
	config.PollInterval = 100 * time.Millisecond
	config.ErrorBackoff = 1 * time.Second

	session := mermaidtest.CreateSession(t)
	clusterSession := mermaidtest.CreateManagedClusterSession(t)
	createKeyspace(t, clusterSession, "test_repair")
	mermaidtest.ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table (id int PRIMARY KEY)")

	const (
		node0 = "172.16.1.3"
		node1 = "172.16.1.10"
	)

	var (
		s, hrt    = newTestService(t, session, config)
		clusterID = uuid.MustRandom()
		taskID    = uuid.MustRandom()
		runID     = uuid.NewTime()
		unit      = repair.Unit{Keyspace: "test_repair"}
		ctx       = context.Background()
	)

	assertStatus := func(expected repair.Status) {
		if r, err := s.GetRun(ctx, clusterID, taskID, runID); err != nil {
			t.Fatal(err)
		} else if r.Status != expected {
			t.Fatal("wrong status", r, "expected", expected, "got", r.Status)
		}
	}

	nodeProgress := func(ip string) int {
		prog, err := s.GetProgress(ctx, clusterID, taskID, runID)
		if err != nil {
			t.Fatal(err)
		}

		v := 0
		t := 0
		for _, p := range prog {
			if p.Host == ip {
				v += p.PercentComplete()
				t += 1
			}
		}
		if t != 0 {
			v /= t
		}
		return v
	}

	waitNodeProgress := func(ip string, percent int) {
		for {
			p := nodeProgress(ip)
			if p >= percent {
				break
			}

			time.Sleep(500 * time.Millisecond)
		}
	}

	assertNodeProgress := func(ip string, percent int) {
		p := nodeProgress(ip)
		if p < percent {
			t.Fatal("no progress", "expected", percent, "got", p)
		}
	}

	wait := func() {
		time.Sleep(2 * time.Second)
	}

	// When run repair
	if err := s.Repair(ctx, clusterID, taskID, runID, unit); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusRunning)

	// When wait
	wait()

	// Then repair of node0 advances
	assertNodeProgress(node0, 1)

	// When run another repair
	// Then run fails
	if err := s.Repair(ctx, clusterID, taskID, uuid.NewTime(), unit); err == nil || err.Error() != "repair already in progress" {
		t.Fatal("expected error", err)
	}

	// When node0 is 1/2 repaired
	waitNodeProgress(node0, 50)

	// And
	if err := s.StopRepair(ctx, clusterID, taskID, runID); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusStopping
	assertStatus(repair.StatusStopping)

	// When wait
	wait()

	// Then status is StatusStopped
	assertStatus(repair.StatusStopped)

	// When connectivity fails
	hrt.SetInterceptor(mermaidtest.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		defer hrt.SetInterceptor(nil)
		return nil, errors.New("test")
	}))

	// And create a new task
	runID = uuid.NewTime()

	// Then run fails
	if err := s.Repair(ctx, clusterID, taskID, runID, unit); err == nil || !strings.Contains(err.Error(), "test") {
		t.Fatal(err)
	}

	// And status is StatusError
	assertStatus(repair.StatusError)

	// When create a new task
	runID = uuid.NewTime()

	// And run repair
	if err := s.Repair(ctx, clusterID, taskID, runID, unit); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusRunning)

	// When wait
	wait()

	// Then repair of node0 continues
	assertNodeProgress(node0, 50)

	// When errors occur
	hrt.SetInterceptor(failRepairInterceptor)
	time.AfterFunc(5*time.Second, func() {
		hrt.SetInterceptor(nil)
	})

	// When node0 is repaired
	waitNodeProgress(node0, 97)

	// And wait
	wait()

	// Then status is StatusError
	assertStatus(repair.StatusError)

	// When create a new task
	runID = uuid.NewTime()

	// And run repair
	if err := s.Repair(ctx, clusterID, taskID, runID, unit); err != nil {
		t.Fatal(err)
	}

	// And wait
	wait()

	// Then
	assertNodeProgress(node0, 100)

	// When node1 is 1/2 repaired
	waitNodeProgress(node1, 50)

	// And restart
	s.Close()
	wait()
	s, hrt = newTestService(t, session, config)
	s.FixRunStatus(ctx)

	// And create a new task
	runID = uuid.NewTime()

	// And run repair
	if err := s.Repair(ctx, clusterID, taskID, runID, unit); err != nil {
		t.Fatal(err)
	}

	// Then status is StatusRunning
	assertStatus(repair.StatusRunning)

	// When wait
	wait()

	// Then repair of node1 continues
	assertNodeProgress(node1, 50)

	// When node1 is repaired
	waitNodeProgress(node1, 100)
}

func TestServiceRepairStopOnErrorIntegration(t *testing.T) {
	session := mermaidtest.CreateSession(t)
	clusterSession := mermaidtest.CreateManagedClusterSession(t)
	createKeyspace(t, clusterSession, "test_repair")
	mermaidtest.ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table (id int PRIMARY KEY)")

	const node0 = "172.16.1.3"

	// Given stop on error is true
	config := repair.DefaultConfig()
	config.StopOnError = true

	var (
		s, hrt    = newTestService(t, session, config)
		clusterID = uuid.MustRandom()
		taskID    = uuid.MustRandom()
		runID     = uuid.NewTime()
		unit      = repair.Unit{Keyspace: "test_repair"}
		ctx       = context.Background()
	)

	assertStatus := func(expected repair.Status) {
		if r, err := s.GetRun(ctx, clusterID, taskID, runID); err != nil {
			t.Fatal(err)
		} else if r.Status != expected {
			t.Fatal("wrong status", r, "expected", expected, "got", r.Status)
		}
	}

	wait := func() {
		time.Sleep(2 * time.Second)
	}

	// And repair failing repair
	hrt.SetInterceptor(failRepairInterceptor)

	// When run repair
	if err := s.Repair(ctx, clusterID, taskID, runID, unit); err != nil {
		t.Fatal(err)
	}

	// And wait
	wait()

	// Then repair stopped
	assertStatus(repair.StatusError)

	// And errors are recorded
	prog, err := s.GetProgress(ctx, clusterID, taskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	hostProg := prog[:0]
	for _, p := range prog {
		if p.Host == node0 {
			hostProg = append(hostProg, p)
		}
	}

	if len(hostProg) != 2 {
		t.Fatal("expected 2 shards")
	}
	for _, p := range hostProg {
		if p.SegmentError != config.SegmentsPerRepair {
			t.Error("expected", config.SegmentsPerRepair, "failed segments, got", p.SegmentError)
		}
		if p.SegmentSuccess != 0 {
			t.Error("expected no successful segments")
		}
		if len(p.SegmentErrorStartTokens) != 1 {
			t.Error("expected 1 error start token, got", len(p.SegmentErrorStartTokens))
		}
	}
}

func newTestService(t *testing.T, session *gocql.Session, c repair.Config) (*repair.Service, *mermaidtest.HackableRoundTripper) {
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)

	rt := mermaidtest.NewHackableRoundTripper(ssh.NewDevelopmentTransport())

	s, err := repair.NewService(
		session,
		c,
		func(_ context.Context, id uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{
				ID:   id,
				Name: "test_cluster",
			}, nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			c, err := scyllaclient.NewClient(mermaidtest.ManagedClusterHosts, rt, logger.Named("scylla"))
			if err != nil {
				return nil, err
			}
			return scyllaclient.WithConfig(c, scyllaclient.Config{
				"shard_count": float64(2),
			}), nil
		},
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s, rt
}

func createKeyspace(t *testing.T, session *gocql.Session, keyspace string) {
	mermaidtest.ExecStmt(t, session, "CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
}

var failRepairInterceptor = mermaidtest.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || !strings.HasPrefix(req.URL.Path, "/storage_service/repair_async/") {
		return nil, nil
	}

	return &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Body:       ioutil.NopCloser(bytes.NewBufferString(`"FAILED"`)),
		Request:    req,
		Header:     make(http.Header, 0),
	}, nil
})
