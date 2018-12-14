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
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/internal/httputil"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap/zapcore"
)

const (
	node0 = 0
	node1 = 1
	node2 = 2
)

const (
	_interval = 100 * time.Millisecond
	now       = 0
	shortWait = 2 * time.Second
	longWait  = 20 * time.Second
)

type repairTestHelper struct {
	session *gocql.Session
	hrt     *HackableRoundTripper
	service *repair.Service

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newRepairTestHelper(t *testing.T, session *gocql.Session, c repair.Config) *repairTestHelper {
	ExecStmt(t, session, "TRUNCATE TABLE repair_run")
	ExecStmt(t, session, "TRUNCATE TABLE repair_run_progress")

	hrt := NewHackableRoundTripper(NewSSHTransport())
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

	WaitCond(h.t, func() bool {
		r, err := h.service.GetRun(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}
		return r.Status == s
	}, _interval, wait)
}

func (h *repairTestHelper) assertCause(cause string, wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		r, err := h.service.GetRun(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}
		return strings.Contains(r.Cause, cause)
	}, _interval, wait)
}

func (h *repairTestHelper) assertProgress(unit, node, percent int, wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		p := h.progress(unit, node)
		return p >= percent
	}, _interval, wait)
}

func (h *repairTestHelper) progress(unit, node int) int {
	h.t.Helper()

	p, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
	if err != nil {
		h.t.Fatal(err)
	}

	if len(p.Units[unit].Nodes) <= node {
		return -1
	}

	return p.Units[unit].Nodes[node].PercentComplete
}

func (h *repairTestHelper) close() {
	h.service.Close()
}

func newTestService(t *testing.T, session *gocql.Session, hrt *HackableRoundTripper, c repair.Config) *repair.Service {
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
			return scyllaclient.NewClient(ManagedClusterHosts, hrt, logger.Named("scylla"))
		},
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func repairInterceptor(s scyllaclient.CommandStatus) http.RoundTripper {
	return httputil.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
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
	ExecStmt(t, session, "CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
}

func singleUnit() repair.Target {
	return repair.Target{
		Units: []repair.Unit{
			{
				Keyspace: "test_repair",
				Tables:   []string{"test_table_0"},
			},
		},
		DC:          []string{"dc1", "dc2"},
		TokenRanges: repair.PrimaryTokenRanges,
		Opts:        runner.DefaultOpts,
	}
}

func multipleUnits() repair.Target {
	return repair.Target{
		Units: []repair.Unit{
			{Keyspace: "test_repair", Tables: []string{"test_table_0"}},
			{Keyspace: "test_repair", Tables: []string{"test_table_1"}},
		},
		DC:          []string{"dc1", "dc2"},
		TokenRanges: repair.PrimaryTokenRanges,
		Opts:        runner.DefaultOpts,
	}
}

func TestServiceRepairIntegration(t *testing.T) {
	clusterSession := CreateManagedClusterSession(t)
	createKeyspace(t, clusterSession, "test_repair")
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table_0 (id int PRIMARY KEY)")
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table_1 (id int PRIMARY KEY)")

	defaultConfig := func() repair.Config {
		c := repair.DefaultConfig()
		c.SegmentsPerRepair = 10
		c.PollInterval = 10 * time.Millisecond
		return c
	}

	session := CreateSession(t)

	t.Run("repair", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		defer h.close()
		ctx := context.Background()

		Print("When: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, multipleUnits()); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		Print("And: repair of node0 advances")
		h.assertProgress(0, node0, 1, shortWait)

		Print("When: node0 is 100% repaired")
		h.assertProgress(0, node0, 100, longWait)

		Print("Then: repair of node1 advances")
		h.assertProgress(0, node1, 1, shortWait)

		Print("When: node1 is 100% repaired")
		h.assertProgress(0, node1, 100, longWait)

		Print("Then: repair of node2 advances")
		h.assertProgress(0, node2, 1, shortWait)

		Print("When: node2 is 100% repaired")
		h.assertProgress(0, node2, 100, longWait)

		Print("And: repair of U1 node0 advances")
		h.assertProgress(1, node0, 1, shortWait)

		Print("When: U1 node0 is 100% repaired")
		h.assertProgress(1, node0, 100, longWait)

		Print("Then: repair of U1 node1 advances")
		h.assertProgress(1, node1, 1, shortWait)

		Print("When: U1 node1 is 100% repaired")
		h.assertProgress(1, node1, 100, longWait)

		Print("Then: repair of U1 node2 advances")
		h.assertProgress(1, node2, 1, shortWait)

		Print("When: U1 node2 is 100% repaired")
		h.assertProgress(1, node2, 100, longWait)

		Print("Then: status is StatusDone")
		h.assertStatus(runner.StatusDone, shortWait)
	})

	t.Run("repair dc", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		defer h.close()
		ctx := context.Background()

		units := multipleUnits()
		units.DC = []string{"dc2"}

		Print("When: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, units); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		Print("When: status is StatusDone")
		h.assertStatus(runner.StatusDone, 2*longWait)

		Print("Then: dc2 is used for repair")
		prog, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}
		if diff := cmp.Diff(prog.DC, []string{"dc2"}); diff != "" {
			h.t.Fatal(diff)
		}
		for _, u := range prog.Units {
			for _, n := range u.Nodes {
				if !strings.HasPrefix(n.Host, "192.168.100.2") {
					t.Error(n.Host)
				}
			}
		}
	})

	t.Run("repair dc local keyspace mismatch", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		defer h.close()
		ctx := context.Background()

		Print("Given: dc2 only keyspace")
		ExecStmt(t, clusterSession, "CREATE KEYSPACE IF NOT EXISTS test_repair_dc2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc2': 3}")
		ExecStmt(t, clusterSession, "CREATE TABLE IF NOT EXISTS test_repair_dc2.test_table_0 (id int PRIMARY KEY)")

		units := singleUnit()
		units.Units = []repair.Unit{
			{
				Keyspace: "test_repair_dc2",
			},
		}
		units.DC = []string{"dc1"}

		Print("When: run repair with dc1")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, units); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		Print("When: status is StatusError")
		h.assertStatus(runner.StatusError, shortWait)

		Print("And: cause is no matching DCs")
		h.assertCause("no matching DCs", now)
	})

	t.Run("repair host", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		defer h.close()
		ctx := context.Background()

		units := multipleUnits()
		units.Host = ManagedClusterHosts[0]

		Print("When: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, units); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		Print("And: repair of node0 advances")
		h.assertProgress(0, node0, 1, shortWait)

		Print("When: node0 is 100% repaired")
		h.assertProgress(0, node0, 100, longWait)

		Print("Then: status is StatusDone")
		h.assertStatus(runner.StatusDone, 2*longWait)

		Print(fmt.Sprintf("Then: host %s is used for repair", units.Host))
		prog, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}
		for _, u := range prog.Units {
			for _, n := range u.Nodes {
				if n.Host != units.Host {
					t.Error(n.Host)
				}
			}
		}
	})

	t.Run("repair abort", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		defer h.close()
		ctx := context.Background()

		Print("Given: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, singleUnit()); err != nil {
			t.Fatal(err)
		}

		Print("When: node0 is 50% repaired")
		h.assertProgress(0, node0, 50, longWait)

		Print("And: service close")
		h.service.Close()

		Print("Then: status is StatusStopping")
		h.assertStatus(runner.StatusAborted, shortWait)
	})

	t.Run("repair stop", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		defer h.close()
		ctx := context.Background()

		Print("Given: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, singleUnit()); err != nil {
			t.Fatal(err)
		}

		Print("When: node0 is 50% repaired")
		h.assertProgress(0, node0, 50, longWait)

		Print("And: stop repair")
		if err := h.service.StopRepair(ctx, h.clusterID, h.taskID, h.runID); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusStopping")
		h.assertStatus(runner.StatusStopping, now)

		Print("And: status is StatusStopped")
		h.assertStatus(runner.StatusStopped, shortWait)
	})

	t.Run("repair restart", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		defer h.close()
		ctx := context.Background()

		Print("Given: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, multipleUnits()); err != nil {
			t.Fatal(err)
		}

		Print("When: node1 is 50% repaired")
		h.assertProgress(0, node1, 50, longWait)

		Print("And: stop repair")
		if err := h.service.StopRepair(ctx, h.clusterID, h.taskID, h.runID); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusStopped")
		h.assertStatus(runner.StatusStopped, shortWait)

		Print("When: create a new task")
		h.runID = uuid.NewTime()

		Print("And: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, multipleUnits()); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		Print("And: repair of node1 continues")
		h.assertProgress(0, node0, 100, shortWait)
		h.assertProgress(0, node1, 50, now)
		h.assertProgress(0, node2, 0, now)

		Print("When: U1 node0 is 10% repaired")
		h.assertProgress(1, node0, 10, longWait)

		Print("And: stop repair")
		if err := h.service.StopRepair(ctx, h.clusterID, h.taskID, h.runID); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusStopped")
		h.assertStatus(runner.StatusStopped, shortWait)

		Print("When: create a new task")
		h.runID = uuid.NewTime()

		Print("And: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, multipleUnits()); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		Print("And: repair of U1 node0 continues")
		h.assertProgress(0, node0, 100, shortWait)
		h.assertProgress(0, node1, 100, now)
		h.assertProgress(0, node2, 100, now)
		h.assertProgress(1, node0, 10, shortWait)
	})

	t.Run("repair restart no continue", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		defer h.close()
		ctx := context.Background()

		unit := singleUnit()
		unit.Opts.Continue = false

		Print("Given: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, unit); err != nil {
			t.Fatal(err)
		}

		Print("When: node0 is 50% repaired")
		h.assertProgress(0, node0, 50, longWait)

		Print("And: stop repair")
		if err := h.service.StopRepair(ctx, h.clusterID, h.taskID, h.runID); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusStopped")
		h.assertStatus(runner.StatusStopped, shortWait)

		Print("When: create a new task")
		h.runID = uuid.NewTime()

		Print("And: run repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, unit); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		Print("And: repair of node0 starts from scratch")
		h.assertProgress(0, node0, 1, shortWait)
		if h.progress(0, node0) >= 50 {
			t.Fatal("node0 should start from schratch")
		}
	})

	t.Run("repair restart task properties changed", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		defer h.close()
		ctx := context.Background()

		Print("Given: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, multipleUnits()); err != nil {
			t.Fatal(err)
		}

		Print("When: node1 is 50% repaired")
		h.assertProgress(0, node1, 50, longWait)

		Print("And: stop repair")
		if err := h.service.StopRepair(ctx, h.clusterID, h.taskID, h.runID); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusStopped")
		h.assertStatus(runner.StatusStopped, shortWait)

		Print("When: create a new task")
		h.runID = uuid.NewTime()

		Print("And: run repair with modified units")
		modifiedUnits := multipleUnits()
		modifiedUnits.Units = []repair.Unit{{Keyspace: "test_repair", Tables: []string{"test_table_1"}}}
		modifiedUnits.DC = []string{"dc2"}
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, modifiedUnits); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusRunning")
		h.assertStatus(runner.StatusRunning, now)

		Print("And: repair of node1 continues")
		h.assertProgress(0, node0, 100, shortWait)
		h.assertProgress(0, node1, 50, now)
		h.assertProgress(0, node2, 0, now)

		Print("And: dc1 is used for repair")
		prog, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}
		if diff := cmp.Diff(prog.DC, multipleUnits().DC); diff != "" {
			h.t.Fatal(diff, prog)
		}
		if len(prog.Units) != 2 {
			t.Fatal(prog.Units)
		}
	})

	t.Run("repair error retry", func(t *testing.T) {
		c := defaultConfig()
		c.ErrorBackoff = 1 * time.Second

		h := newRepairTestHelper(t, session, c)
		defer h.close()
		ctx := context.Background()

		Print("Given: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, singleUnit()); err != nil {
			t.Fatal(err)
		}

		Print("When: node1 is 50% repaired")
		h.assertProgress(0, node1, 50, longWait)

		Print("And: errors occur for 5s with 1s backoff")
		h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandFailed))
		time.AfterFunc(5*time.Second, func() {
			h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
		})

		Print("Then: node1 repair continues")
		h.assertProgress(0, node1, 60, longWait)

		Print("When: node1 is 95% repaired")
		h.assertProgress(0, node1, 95, longWait)

		Print("Then: repair of node2 advances")
		h.assertProgress(0, node2, 1, longWait)

		Print("When: node2 is 100% repaired")
		h.assertProgress(0, node2, 100, longWait)

		Print("Then: node1 is retries repair")
		h.assertProgress(0, node1, 100, shortWait)

		Print("And: status is StatusDone")
		h.assertStatus(runner.StatusDone, shortWait)
	})

	t.Run("repair error fail fast", func(t *testing.T) {
		c := defaultConfig()
		h := newRepairTestHelper(t, session, c)
		defer h.close()
		ctx := context.Background()

		unit := singleUnit()
		unit.FailFast = true

		Print("Given: one repair fails")
		var (
			mu sync.Mutex
			ic = repairInterceptor(scyllaclient.CommandFailed)
		)
		h.hrt.SetInterceptor(httputil.RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
			if !strings.HasPrefix(req.URL.Path, "/storage_service/repair_async/") {
				return nil, nil
			}

			mu.Lock()
			defer mu.Unlock()
			resp, err = ic.RoundTrip(req)

			if req.Method == http.MethodGet {
				ic = repairInterceptor(scyllaclient.CommandSuccessful)
			}
			return
		}))

		Print("And: repair")
		if err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, unit); err != nil {
			t.Fatal(err)
		}

		Print("Then: status is StatusError")
		h.assertStatus(runner.StatusError, longWait) // should be shortWait but becomes flaky

		Print("And: errors are recorded")
		p, err := h.service.GetProgress(ctx, h.clusterID, h.taskID, h.runID)
		if err != nil {
			t.Fatal(err)
		}

		se := 0
		ss := 0
		for _, u := range p.Units {
			for _, n := range u.Nodes {
				for _, s := range n.Shards {
					se += s.SegmentError
					ss += s.SegmentSuccess
				}
			}
		}
		if se != c.SegmentsPerRepair {
			t.Fatal("expected", c.SegmentsPerRepair, "got", se)
		}
		if ss > 10*c.SegmentsPerRepair {
			t.Fatal("got", ss) // sometimes can be 0 or 20
		}
	})
}
