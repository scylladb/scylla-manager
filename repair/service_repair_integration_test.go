// Copyright (C) 2017 ScyllaDB

// +build all integration

package repair_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/ssh"
	"github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap/zapcore"
)

var (
	print = mermaidtest.Print
	wait  = func() {
		time.Sleep(2 * time.Second)
	}
)

const (
	node0 = "172.16.1.3"
	node1 = "172.16.1.10"
)

type repairTestHelper struct {
	session *gocql.Session
	hrt     *mermaidtest.HackableRoundTripper
	service *repair.Service

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newRepairTestHelper(t *testing.T, c repair.Config) *repairTestHelper {
	session := mermaidtest.CreateSession(t)
	hrt := mermaidtest.NewHackableRoundTripper(ssh.NewDevelopmentTransport())
	s := newTestService(t, session, hrt, c)

	return &repairTestHelper{
		session: session,
		hrt:     hrt,
		service: s,

		clusterID: uuid.MustRandom(),
		taskID:    uuid.MustRandom(),
		runID:     uuid.NewTime(),

		t: t,
	}
}

func (h *repairTestHelper) assertStatus(expected runner.Status) {
	h.t.Helper()

	if r, err := h.service.GetRun(context.Background(), h.clusterID, h.taskID, h.runID); err != nil {
		h.t.Fatal(err)
	} else if r.Status != expected {
		h.t.Fatal("wrong status", r, "expected", expected, "got", r.Status)
	}
}

func (h *repairTestHelper) assertProgress(node string, percent int) {
	h.t.Helper()

	p := h.progress(node)
	if p < percent {
		h.t.Fatal("no progress", "expected", percent, "got", p)
	}
}

func (h *repairTestHelper) waitProgress(node string, percent int) {
	for {
		p := h.progress(node)
		if p >= percent {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (h *repairTestHelper) progress(node string) int {
	p, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
	if err != nil {
		h.t.Fatal(err)
	}

	for _, u := range p.Units {
		for _, n := range u.Nodes {
			if n.Host == node {
				return n.PercentComplete
			}
		}
	}

	return -1
}

func newTestService(t *testing.T, session *gocql.Session, hrt *mermaidtest.HackableRoundTripper, c repair.Config) *repair.Service {
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)

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
			c, err := scyllaclient.NewClient(mermaidtest.ManagedClusterHosts, hrt, logger.Named("scylla"))
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
	return s
}

func repairInterceptor(s scyllaclient.CommandStatus) http.RoundTripper {
	return mermaidtest.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(req.URL.Path, "/storage_service/repair_async/") {
			return nil, nil
		}

		resp := &http.Response{
			Status:     "200 OK",
			StatusCode: 200,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Request:    req,
			Header:     make(http.Header, 0),
		}

		switch req.Method {
		case http.MethodGet:
			resp.Body = ioutil.NopCloser(bytes.NewBufferString(fmt.Sprintf(`"%s"`, s)))
		case http.MethodPost:
			resp.Body = ioutil.NopCloser(bytes.NewBufferString(`1`))
		}

		return resp, nil
	})
}

func createKeyspace(t *testing.T, session *gocql.Session, keyspace string) {
	mermaidtest.ExecStmt(t, session, "CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
}

func TestServiceRepairIntegration(t *testing.T) {
	// set config values for testing
	config := repair.DefaultConfig()
	config.SegmentsPerRepair = 5
	config.PollInterval = 100 * time.Millisecond
	config.ErrorBackoff = 1 * time.Second

	var (
		h     = newRepairTestHelper(t, config)
		units = []repair.Unit{{Keyspace: "test_repair"}}
		ctx   = context.Background()
	)

	defer h.service.Close()

	print("Given: keyspace test_repair")
	clusterSession := mermaidtest.CreateManagedClusterSession(t)
	createKeyspace(t, clusterSession, "test_repair")
	mermaidtest.ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table (id int PRIMARY KEY)")

	print("When: run repair")
	if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, units); err != nil {
		t.Fatal(err)
	}

	print("Then: status is StatusRunning")
	h.assertStatus(runner.StatusRunning)

	print("When: wait")
	wait()

	print("Then: repair of node0 advances")
	h.assertProgress(node0, 1)

	print("And: another repair fails")
	if err := h.service.Repair(ctx, h.clusterID, h.taskID, uuid.NewTime(), units); err == nil || err.Error() != "repair already in progress" {
		t.Fatal("expected error", err)
	}

	print("When: node0 is 50% repaired")
	h.waitProgress(node0, 50)

	print("And: stop repair")
	if err := h.service.StopRepair(ctx, h.clusterID, h.taskID, h.runID); err != nil {
		t.Fatal(err)
	}

	print("Then status is StatusStopping")
	h.assertStatus(runner.StatusStopping)

	print("When: wait")
	wait()

	print("Then status is StatusStopped")
	h.assertStatus(runner.StatusStopped)

	print("When: connectivity fails")
	h.hrt.SetInterceptor(mermaidtest.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		defer h.hrt.SetInterceptor(nil)
		return nil, errors.New("test")
	}))

	print("And: create a new task")
	h.runID = uuid.NewTime()

	print("Then: run fails")
	if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, units); err == nil || !strings.Contains(err.Error(), "test") {
		t.Fatal(err)
	}

	print("And: status is StatusError")
	h.assertStatus(runner.StatusError)

	print("When: create a new task")
	h.runID = uuid.NewTime()

	print("And: run repair")
	if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, units); err != nil {
		t.Fatal(err)
	}

	print("Then: status is StatusRunning")
	h.assertStatus(runner.StatusRunning)

	print("When: wait")
	wait()

	print("Then: repair of node0 continues")
	h.assertProgress(node0, 50)

	print("When: errors occur")
	h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandFailed))
	time.AfterFunc(5*time.Second, func() {
		h.hrt.SetInterceptor(nil)
	})

	print("When: node0 is repaired")
	h.waitProgress(node0, 97)

	print("And: wait")
	wait()

	print("Then: status is StatusError")
	h.assertStatus(runner.StatusError)

	print("When: create a new task")
	h.runID = uuid.NewTime()

	print("And: run repair")
	if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, units); err != nil {
		t.Fatal(err)
	}

	print("And: wait")
	wait()

	print("Then: node0 is 100% repaired ")
	h.assertProgress(node0, 100)

	print("When: node1 is 50% repaired")
	h.waitProgress(node1, 50)

	print("And: restart service")
	h.service.Close()
	wait()
	h.service = newTestService(t, h.session, h.hrt, config)
	h.service.Init(ctx)

	print("And: create a new task")
	h.runID = uuid.NewTime()

	print("And: run repair")
	if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, units); err != nil {
		t.Fatal(err)
	}

	print("Then: status is StatusRunning")
	h.assertStatus(runner.StatusRunning)

	print("When: wait")
	wait()

	print("Then: node1 is 50% repaired")
	h.assertProgress(node1, 50)
}

func TestServiceRepairStopOnErrorIntegration(t *testing.T) {
	// set config values for testing
	config := repair.DefaultConfig()
	config.StopOnError = true

	var (
		h     = newRepairTestHelper(t, config)
		units = []repair.Unit{{Keyspace: "test_repair"}}
		ctx   = context.Background()
	)

	defer h.service.Close()

	print("Given: keyspace test_repair")
	clusterSession := mermaidtest.CreateManagedClusterSession(t)
	createKeyspace(t, clusterSession, "test_repair")
	mermaidtest.ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table (id int PRIMARY KEY)")

	print("When: repair fails")
	h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandFailed))

	print("And: repair")
	if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, units); err != nil {
		t.Fatal(err)
	}

	print("And: wait")
	wait()

	print("Then: repair is stopped")
	h.assertStatus(runner.StatusError)

	print("And: errors are recorded")
	p, err := h.service.GetProgress(ctx, h.clusterID, h.taskID, h.runID)
	if err != nil {
		t.Fatal(err)
	}
	var sp []repair.ShardProgress
	for _, u := range p.Units {
		for _, n := range u.Nodes {
			if n.Host == node0 {
				sp = n.Shards
				break
			}
		}
	}

	if len(sp) != 2 {
		t.Fatal("expected 2 shards")
	}
	for _, p := range sp {
		if p.SegmentError != config.SegmentsPerRepair {
			t.Error("expected", config.SegmentsPerRepair, "failed segments, got", p.SegmentError)
		}
		if p.SegmentSuccess != 0 {
			t.Error("expected no successful segments")
		}
	}
}
