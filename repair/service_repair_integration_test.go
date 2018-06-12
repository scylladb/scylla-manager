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
)

const (
	node0 = "192.168.100.11"
	node1 = "192.168.100.12"
	node2 = "192.168.100.13"
)

const (
	_interval = 100 * time.Millisecond
	now       = 0
	shortWait = time.Second
	longWait  = 20 * time.Second
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
	mermaidtest.ExecStmt(t, session, "TRUNCATE TABLE repair_run")
	mermaidtest.ExecStmt(t, session, "TRUNCATE TABLE repair_run_progress")

	hrt := mermaidtest.NewHackableRoundTripper(ssh.NewDevelopmentTransport())
	hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
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

func (h *repairTestHelper) assertStatus(s runner.Status, wait time.Duration) {
	h.t.Helper()

	mermaidtest.WaitCond(h.t, func() bool {
		r, err := h.service.GetRun(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}
		return r.Status == s
	}, _interval, wait)
}

func (h *repairTestHelper) assertProgress(unit int, node string, percent int, wait time.Duration) {
	h.t.Helper()

	mermaidtest.WaitCond(h.t, func() bool {
		p := h.progress(unit, node)
		return p >= percent
	}, _interval, wait)
}

func (h *repairTestHelper) progress(unit int, node string) int {
	h.t.Helper()

	p, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
	if err != nil {
		h.t.Fatal(err)
	}

	for _, n := range p.Units[unit].Nodes {
		if n.Host == node {
			return n.PercentComplete
		}
	}

	return -1
}

func (h *repairTestHelper) close() {
	h.service.Close()
	h.session.Close()
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
			resp.Body = ioutil.NopCloser(bytes.NewBufferString(fmt.Sprintf("\"%s\"", s)))
		case http.MethodPost:
			resp.Body = ioutil.NopCloser(bytes.NewBufferString("1"))
		}

		return resp, nil
	})
}

func createKeyspace(t *testing.T, session *gocql.Session, keyspace string) {
	mermaidtest.ExecStmt(t, session, "CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
}

func TestServiceRepairIntegration(t *testing.T) {
	clusterSession := mermaidtest.CreateManagedClusterSession(t)
	createKeyspace(t, clusterSession, "test_repair")
	mermaidtest.ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table_0 (id int PRIMARY KEY)")
	mermaidtest.ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table_1 (id int PRIMARY KEY)")

	defaultConfig := func() repair.Config {
		c := repair.DefaultConfig()
		c.SegmentsPerRepair = 10
		c.PollInterval = 10 * time.Millisecond
		return c
	}

	unit := []repair.Unit{{Keyspace: "test_repair", Tables: []string{"test_table_0"}}}

	multipleUnits := []repair.Unit{
		{Keyspace: "test_repair", Tables: []string{"test_table_0"}},
		{Keyspace: "test_repair", Tables: []string{"test_table_1"}},
	}

	t.Run("repair", func(t *testing.T) {
		h := newRepairTestHelper(t, defaultConfig())
		defer h.close()
		ctx := context.Background()

		print("When: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, multipleUnits); err != nil {
			t.Fatal(err)
		}

		print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		print("And: repair of node0 advances")
		h.assertProgress(0, node0, 1, shortWait)

		print("When: node0 is 100% repaired")
		h.assertProgress(0, node0, 100, longWait)

		print("Then: repair of node1 advances")
		h.assertProgress(0, node1, 1, shortWait)

		print("When: node1 is 100% repaired")
		h.assertProgress(0, node1, 100, longWait)

		print("Then: repair of node2 advances")
		h.assertProgress(0, node2, 1, shortWait)

		print("When: node2 is 100% repaired")
		h.assertProgress(0, node2, 100, longWait)

		print("And: repair of U1 node0 advances")
		h.assertProgress(1, node0, 1, shortWait)

		print("When: U1 node0 is 100% repaired")
		h.assertProgress(1, node0, 100, longWait)

		print("Then: repair of U1 node1 advances")
		h.assertProgress(1, node1, 1, shortWait)

		print("When: U1 node1 is 100% repaired")
		h.assertProgress(1, node1, 100, longWait)

		print("Then: repair of U1 node2 advances")
		h.assertProgress(1, node2, 1, shortWait)

		print("When: U1 node2 is 100% repaired")
		h.assertProgress(1, node2, 100, longWait)

		print("Then: status is StatusDone")
		h.assertStatus(runner.StatusDone, shortWait)
	})

	t.Run("repair stop", func(t *testing.T) {
		h := newRepairTestHelper(t, defaultConfig())
		defer h.close()
		ctx := context.Background()

		print("Given: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, unit); err != nil {
			t.Fatal(err)
		}

		print("When: node0 is 50% repaired")
		h.assertProgress(0, node0, 50, longWait)

		print("And: stop repair")
		if err := h.service.StopRepair(ctx, h.clusterID, h.taskID, h.runID); err != nil {
			t.Fatal(err)
		}

		print("Then: status is StatusStopping")
		h.assertStatus(runner.StatusStopping, now)

		print("And: status is StatusStopped")
		h.assertStatus(runner.StatusStopped, shortWait)
	})

	t.Run("repair restart", func(t *testing.T) {
		h := newRepairTestHelper(t, defaultConfig())
		defer h.close()
		ctx := context.Background()

		print("Given: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, multipleUnits); err != nil {
			t.Fatal(err)
		}

		print("When: node1 is 50% repaired")
		h.assertProgress(0, node1, 50, longWait)

		print("And: stop repair")
		if err := h.service.StopRepair(ctx, h.clusterID, h.taskID, h.runID); err != nil {
			t.Fatal(err)
		}

		print("Then: status is StatusStopped")
		h.assertStatus(runner.StatusStopped, shortWait)

		print("When: create a new task")
		h.runID = uuid.NewTime()

		print("And: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, multipleUnits); err != nil {
			t.Fatal(err)
		}

		print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		print("And: repair of node1 continues")
		h.assertProgress(0, node0, 100, shortWait)
		h.assertProgress(0, node1, 50, now)
		h.assertProgress(0, node2, 0, now)

		print("When: U1 node0 is 10% repaired")
		h.assertProgress(1, node0, 10, longWait)

		print("And: stop repair")
		if err := h.service.StopRepair(ctx, h.clusterID, h.taskID, h.runID); err != nil {
			t.Fatal(err)
		}

		print("Then: status is StatusStopped")
		h.assertStatus(runner.StatusStopped, shortWait)

		print("When: create a new task")
		h.runID = uuid.NewTime()

		print("And: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, multipleUnits); err != nil {
			t.Fatal(err)
		}

		print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		print("And: repair of U1 node0 continues")
		h.assertProgress(0, node0, 100, shortWait)
		h.assertProgress(0, node1, 100, now)
		h.assertProgress(0, node2, 100, now)
		h.assertProgress(1, node0, 10, shortWait)
	})

	t.Run("repair error", func(t *testing.T) {
		c := defaultConfig()
		c.ErrorBackoff = 1 * time.Second

		h := newRepairTestHelper(t, c)
		defer h.close()
		ctx := context.Background()

		print("Given: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, unit); err != nil {
			t.Fatal(err)
		}

		print("When: node1 is 50% repaired")
		h.assertProgress(0, node1, 50, longWait)

		print("And: errors occur for 5s with 1s backoff")
		h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandFailed))
		time.AfterFunc(5*time.Second, func() {
			h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
		})

		print("Then: node1 repair continues")
		h.assertProgress(0, node1, 60, longWait)

		print("When: node1 is 95% repaired")
		h.assertProgress(0, node1, 95, longWait)

		print("Then: status is StatusError")
		h.assertStatus(runner.StatusError, shortWait)

		// FIXME this should be changed to handle errors internally see scylladb/mermaid#461
		//print("Then: repair of node2 advances")
		//h.assertProgress(0, node2, 1, longWait)
		//
		//print("When: node2 is 100% repaired")
		//h.assertProgress(0, node2, 100, longWait)
		//
		//print("Then: status is StatusError")
		//h.assertStatus(runner.StatusError, shortWait)

		print("When: create a new task")
		h.runID = uuid.NewTime()

		print("And: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, unit); err != nil {
			t.Fatal(err)
		}

		print("Then: repair of node1 continues")
		h.assertProgress(0, node0, 100, shortWait)
		h.assertProgress(0, node1, 95, now)
		h.assertProgress(0, node2, 0, now)

		print("When: repair of node2 advances")
		h.assertProgress(0, node2, 1, shortWait)

		print("Then: node1 is 100% repaired")
		h.assertProgress(0, node1, 100, now)

		print("When: node2 is 100% repaired")
		h.assertProgress(0, node2, 100, longWait)

		print("Then: status is StatusDone")
		h.assertStatus(runner.StatusDone, shortWait)
	})

	t.Run("repair stop on error", func(t *testing.T) {
		c := defaultConfig()
		c.StopOnError = true

		h := newRepairTestHelper(t, c)
		defer h.close()
		ctx := context.Background()

		print("Given: repair error occurs")
		h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandFailed))

		print("And: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, unit); err != nil {
			t.Fatal(err)
		}

		print("Then: status is StatusError")
		h.assertStatus(runner.StatusError, longWait) // should be shortWait but becomes flaky

		print("And: errors are recorded")
		p, err := h.service.GetProgress(ctx, h.clusterID, h.taskID, h.runID)
		if err != nil {
			t.Fatal(err)
		}
		for _, u := range p.Units {
			for _, n := range u.Nodes {
				if n.Host == node0 {
					for _, s := range n.Shards {
						if s.SegmentError != c.SegmentsPerRepair {
							t.Error(s)
						}
						if s.SegmentSuccess != 0 {
							t.Error(s)
						}
					}
					break
				}
			}
		}
	})
}
