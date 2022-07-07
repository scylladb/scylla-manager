// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package repair_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

type repairTestHelper struct {
	logger  log.Logger
	session gocqlx.Session
	hrt     *HackableRoundTripper
	client  *scyllaclient.Client
	service *repair.Service

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	mu     sync.RWMutex
	done   bool
	result error

	t *testing.T
}

func newRepairTestHelper(t *testing.T, session gocqlx.Session, config repair.Config) *repairTestHelper {
	t.Helper()

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)

	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
	c := newTestClient(t, hrt, log.NopLogger)
	s := newTestService(t, session, c, config, logger)

	return &repairTestHelper{
		logger:  logger,
		session: session,
		hrt:     hrt,
		client:  c,
		service: s,

		clusterID: uuid.MustRandom(),
		taskID:    uuid.MustRandom(),
		runID:     uuid.NewTime(),

		t: t,
	}
}

func (h *repairTestHelper) runRepair(ctx context.Context, t repair.Target) {
	go func() {
		h.mu.Lock()
		h.done = false
		h.result = nil
		h.mu.Unlock()

		h.logger.Info(ctx, "Running repair", "task", h.taskID, "run", h.runID)
		err := h.service.Repair(ctx, h.clusterID, h.taskID, h.runID, t)
		h.logger.Info(ctx, "Repair ended", "task", h.taskID, "run", h.runID, "error", err)

		h.mu.Lock()
		h.done = true
		h.result = err
		h.mu.Unlock()
	}()
}

// WaitCond parameters
const (
	// now specifies that condition shall be true in the current state.
	now = 0
	// shortWait specifies that condition shall be met in immediate future
	// such as repair filing on start.
	shortWait = 4 * time.Second
	// longWait specifies that condition shall be met after a while, this is
	// useful for waiting for repair to significantly advance or finish.
	longWait = 20 * time.Second

	_interval = 500 * time.Millisecond

	node11 = "192.168.100.11"
	node12 = "192.168.100.12"
	node13 = "192.168.100.13"
	node21 = "192.168.100.21"
	node22 = "192.168.100.22"
	node23 = "192.168.100.23"
)

func (h *repairTestHelper) assertRunning(wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		_, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			if errors.Is(err, service.ErrNotFound) {
				return false
			}
			h.t.Fatal(err)
		}
		return true
	}, _interval, wait)
}

func (h *repairTestHelper) assertDone(wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		return h.done && h.result == nil
	}, _interval, wait)
}

func (h *repairTestHelper) assertProgressSuccess() {
	p, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
	if err != nil {
		h.t.Fatal(err)
	}

	Print("And: there are no more errors")
	if p.Error != 0 {
		h.t.Fatal("expected", 0, "got", p.Error)
	}
	if p.Success != p.TokenRanges {
		h.t.Fatal("expected", p.TokenRanges, "got", p.Success)
	}
}

func (h *repairTestHelper) assertError(wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		return h.done && h.result != nil
	}, _interval, wait)
}

func (h *repairTestHelper) assertErrorContains(cause string, wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		return h.done && h.result != nil && strings.Contains(h.result.Error(), cause)
	}, _interval, wait)
}

func (h *repairTestHelper) assertStopped(wait time.Duration) {
	h.t.Helper()
	h.assertErrorContains(context.Canceled.Error(), wait)
}

func (h *repairTestHelper) assertProgress(node string, percent int, wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		p, _ := h.progress(node)
		return p >= percent
	}, _interval, wait)
}

func (h *repairTestHelper) assertProgressFailed(node string, percent int, wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		_, f := h.progress(node)
		return f >= percent
	}, _interval, wait)
}

func (h *repairTestHelper) assertMaxProgress(node string, percent int, wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		p, _ := h.progress(node)
		return p <= percent
	}, _interval, wait)
}

func (h *repairTestHelper) assertShardProgress(node string, percent int, wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		p, _ := h.progress(node)
		return p >= percent
	}, _interval, wait)
}

func (h *repairTestHelper) assertMaxShardProgress(node string, percent int, wait time.Duration) {
	h.t.Helper()

	WaitCond(h.t, func() bool {
		p, _ := h.progress(node)
		return p <= percent
	}, _interval, wait)
}

func (h *repairTestHelper) progress(node string) (int, int) {
	h.t.Helper()

	p, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
	if err != nil {
		h.t.Fatal(err)
	}
	if node != "" {

	}

	return percentComplete(p)
}

func percentComplete(p repair.Progress) (int, int) {
	if p.TokenRanges == 0 {
		return 0, 0
	}
	return int(p.Success * 100 / p.TokenRanges), int(p.Error * 100 / p.TokenRanges)
}

var (
	repairEndpointRegexp = regexp.MustCompile("/storage_service/repair_(async|status)")
	commandCounter       int32
)

func repairInterceptor(s scyllaclient.CommandStatus) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if !repairEndpointRegexp.MatchString(req.URL.Path) {
			return nil, nil
		}

		resp := httpx.MakeResponse(req, 200)

		switch req.Method {
		case http.MethodGet:
			resp.Body = io.NopCloser(bytes.NewBufferString(fmt.Sprintf("\"%s\"", s)))
		case http.MethodPost:
			id := atomic.AddInt32(&commandCounter, 1)
			resp.Body = io.NopCloser(bytes.NewBufferString(fmt.Sprint(id)))
		}

		return resp, nil
	})
}

func repairStatusNoResponseInterceptor(ctx context.Context) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if !repairEndpointRegexp.MatchString(req.URL.Path) {
			return nil, nil
		}

		resp := httpx.MakeResponse(req, 200)

		switch req.Method {
		case http.MethodGet:
			// do not respond until context is canceled.
			<-ctx.Done()
			resp.Body = io.NopCloser(bytes.NewBufferString(fmt.Sprintf("\"%s\"", scyllaclient.CommandRunning)))
		case http.MethodPost:
			id := atomic.AddInt32(&commandCounter, 1)
			resp.Body = io.NopCloser(bytes.NewBufferString(fmt.Sprint(id)))
		}

		return resp, nil
	})
}

func assertReplicasRepairInterceptor(t *testing.T, host string) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if repairEndpointRegexp.MatchString(req.URL.Path) && req.Method == http.MethodPost {
			hosts := req.URL.Query().Get("hosts")
			if !strings.Contains(hosts, host) {
				t.Errorf("Replicas %s missing %s", hosts, host)
			}
		}
		return nil, nil
	})
}

func countInterceptor(counter *int32, path, method string, next http.RoundTripper) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.Path, path) && (method == "" || req.Method == method) {
			atomic.AddInt32(counter, 1)
		}
		if next != nil {
			return next.RoundTrip(req)
		}
		return nil, nil
	})
}

func holdRepairInterceptor() http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if repairEndpointRegexp.MatchString(req.URL.Path) && req.Method == http.MethodGet {
			resp := httpx.MakeResponse(req, 200)
			resp.Body = io.NopCloser(bytes.NewBufferString(fmt.Sprintf("\"%s\"", scyllaclient.CommandRunning)))
			return resp, nil
		}
		return nil, nil
	})
}

func unstableRepairInterceptor() http.RoundTripper {
	failRi := repairInterceptor(scyllaclient.CommandFailed)
	successRi := repairInterceptor(scyllaclient.CommandSuccessful)
	return httpx.RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		id := atomic.LoadInt32(&commandCounter)
		if id != 0 && id%20 == 0 {
			return failRi.RoundTrip(req)
		}
		return successRi.RoundTrip(req)
	})
}

func dialErrorInterceptor() http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("mock dial error")
	})
}

func newTestClient(t *testing.T, hrt *HackableRoundTripper, logger log.Logger) *scyllaclient.Client {
	t.Helper()

	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	config.Transport = hrt
	config.Backoff.MaxRetries = 5

	c, err := scyllaclient.NewClient(config, logger.Named("scylla"))
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func newTestService(t *testing.T, session gocqlx.Session, client *scyllaclient.Client, c repair.Config, logger log.Logger) *repair.Service {
	t.Helper()

	s, err := repair.NewService(
		session,
		c,
		metrics.NewRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func createKeyspace(t *testing.T, session gocqlx.Session, keyspace string) {
	ExecStmt(t, session, "CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
}

func dropKeyspace(t *testing.T, session gocqlx.Session, keyspace string) {
	ExecStmt(t, session, "DROP KEYSPACE IF EXISTS "+keyspace)
}

// We must use -1 here to support working with empty tables or not flushed data.
const repairAllSmallTableThreshold = -1

func allUnits() repair.Target {
	return repair.Target{
		Units: []repair.Unit{
			{Keyspace: "test_repair", Tables: []string{"test_table_0"}},
			{Keyspace: "test_repair", Tables: []string{"test_table_1"}},
			{Keyspace: "system_schema", Tables: []string{"indexes", "keyspaces"}},
		},
		DC:        []string{"dc1", "dc2"},
		Continue:  true,
		Intensity: 0.1,
		Parallel:  1,
	}
}

func singleUnit() repair.Target {
	return repair.Target{
		Units: []repair.Unit{
			{
				Keyspace: "test_repair",
				Tables:   []string{"test_table_0"},
			},
		},
		DC:        []string{"dc1", "dc2"},
		Continue:  true,
		Intensity: 0.1,
		Parallel:  1,
	}
}

func multipleUnits() repair.Target {
	return repair.Target{
		Units: []repair.Unit{
			{Keyspace: "test_repair", Tables: []string{"test_table_0"}},
			{Keyspace: "test_repair", Tables: []string{"test_table_1"}},
		},
		DC:        []string{"dc1", "dc2"},
		Continue:  true,
		Intensity: 0.1,
		Parallel:  1,
	}
}

func TestServiceGetTargetIntegration(t *testing.T) {
	// Clear keyspaces
	CreateManagedClusterSessionAndDropAllKeyspaces(t)

	// Test names
	testNames := []string{
		"everything",
		"filter keyspaces",
		"filter dc",
		"continue",
		"filter tables",
		"fail_fast",
		"complex",
		//"no enough replicas",
	}

	var (
		session = CreateSession(t)
		h       = newRepairTestHelper(t, session, repair.DefaultConfig())
		ctx     = context.Background()
	)

	for _, testName := range testNames {
		t.Run(testName, func(t *testing.T) {
			input := ReadInputFile(t)
			v, err := h.service.GetTarget(ctx, h.clusterID, input)
			if err != nil {
				t.Fatal(err)
			}

			SaveGoldenJSONFileIfNeeded(t, v)

			var golden repair.Target
			LoadGoldenJSONFile(t, &golden)

			if diff := cmp.Diff(golden, v, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestServiceRepairIntegration(t *testing.T) {
	clusterSession := CreateManagedClusterSessionAndDropAllKeyspaces(t)

	createKeyspace(t, clusterSession, "test_repair")
	WriteData(t, clusterSession, "test_repair", 1, "test_table_0", "test_table_1")
	defer dropKeyspace(t, clusterSession, "test_repair")

	defaultConfig := func() repair.Config {
		c := repair.DefaultConfig()
		c.PollInterval = 10 * time.Millisecond
		return c
	}

	session := CreateSession(t)

	t.Run("repair simple", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.runRepair(ctx, multipleUnits())

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair of node11 advances")
		h.assertProgress(node11, 1, shortWait)

		Print("When: node11 is 100% repaired")
		h.assertProgress(node11, 100, longWait)

		Print("Then: repair of node1 advances")
		h.assertProgress(node12, 1, shortWait)

		Print("When: node1 is 100% repaired")
		h.assertProgress(node12, 100, longWait)

		Print("Then: repair of node2 advances")
		h.assertProgress(node13, 1, shortWait)

		Print("When: node2 is 100% repaired")
		h.assertProgress(node13, 100, longWait)

		Print("And: repair of U1 node11 advances")
		h.assertProgress(node11, 1, shortWait)

		Print("When: U1 node11 is 100% repaired")
		h.assertProgress(node11, 100, longWait)

		Print("Then: repair of U1 node1 advances")
		h.assertProgress(node12, 1, shortWait)

		Print("When: U1 node1 is 100% repaired")
		h.assertProgress(node12, 100, longWait)

		Print("Then: repair of U1 node2 advances")
		h.assertProgress(node13, 1, shortWait)

		Print("When: U1 node2 is 100% repaired")
		h.assertProgress(node13, 100, longWait)

		Print("Then: repair is done")
		h.assertDone(shortWait)
	})

	t.Run("repair dc", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		units := multipleUnits()
		units.DC = []string{"dc2"}

		Print("When: run repair")
		h.runRepair(ctx, units)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: repair is done")
		h.assertDone(2 * longWait)

		Print("Then: dc2 is used for repair")
		prog, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}
		if diff := cmp.Diff(prog.DC, []string{"dc2"}); diff != "" {
			h.t.Fatal(diff)
		}
		for _, h := range prog.Hosts {
			if !strings.HasPrefix(h.Host, "192.168.100.2") {
				t.Error(h.Host)
			}
		}
	})

	t.Run("repair ignore hosts", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const ignored = "192.168.100.12"
		unit := singleUnit()
		unit.IgnoreHosts = []string{ignored}

		Print("When: run repair")
		h.runRepair(ctx, unit)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: repair is done")
		h.assertDone(2 * longWait)

		Print("Then: ignored node is not repaired")
		prog, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}
		for _, h := range prog.Hosts {
			if h.Host == ignored {
				t.Error("Found ignored host in progress")
			}
		}
	})

	t.Run("repair host", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		units := singleUnit()
		units.Host = node13
		h.hrt.SetInterceptor(assertReplicasRepairInterceptor(t, node13))

		Print("When: run repair")
		h.runRepair(ctx, units)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: repair is done")
		h.assertDone(2 * longWait)
	})

	t.Run("repair dc local keyspace mismatch", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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
		h.runRepair(ctx, units)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: repair fails")
		h.assertErrorContains("no replicas to repair", shortWait)
	})

	t.Run("repair simple strategy multi dc", func(t *testing.T) {
		t.Skip("Unsure how to handle this case")
		testKeyspace := "test_repair_simple_multi_dc"
		ExecStmt(t, clusterSession, "CREATE KEYSPACE "+testKeyspace+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
		ExecStmt(t, clusterSession, "CREATE TABLE "+testKeyspace+".test_table_0 (id int PRIMARY KEY)")
		ExecStmt(t, clusterSession, "CREATE TABLE "+testKeyspace+".test_table_1 (id int PRIMARY KEY)")
		defer dropKeyspace(t, clusterSession, testKeyspace)

		testUnit := repair.Target{
			Units: []repair.Unit{
				{
					Keyspace: testKeyspace,
				},
			},
			DC:       []string{"dc1"},
			Continue: true,
		}

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.runRepair(ctx, testUnit)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair of node11 advances")
		h.assertProgress(node11, 1, shortWait)

		Print("When: node11 is 100% repaired")
		h.assertProgress(node11, 100, longWait)

		Print("Then: repair of node12 advances")
		h.assertProgress(node12, 1, shortWait)

		Print("When: node12 is 100% repaired")
		h.assertProgress(node12, 100, longWait)

		Print("Then: repair of node13 advances")
		h.assertProgress(node13, 1, shortWait)

		Print("When: node13 is 100% repaired")
		h.assertProgress(node13, 100, longWait)

		Print("When: node21 is 100% repaired")
		h.assertProgress(node21, 100, longWait)

		Print("When: node22 is 100% repaired")
		h.assertProgress(node22, 100, longWait)

		Print("When: node23 is 100% repaired")
		h.assertProgress(node23, 100, longWait)

		Print("Then: repair is done")
		h.assertDone(shortWait)
	})

	t.Run("repair stop", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		u := multipleUnits()
		u.SmallTableThreshold = repairAllSmallTableThreshold
		Print("When: run repair")
		h.runRepair(ctx, u)

		Print("Then: repair is running")
		h.assertRunning(shortWait)
		h.assertProgress(node11, 5, longWait)

		Print("When: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(shortWait)
	})

	t.Run("repair stop when workers are busy", func(t *testing.T) {
		c := defaultConfig()
		c.GracefulStopTimeout = time.Second
		h := newRepairTestHelper(t, session, c)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		u := multipleUnits()
		u.SmallTableThreshold = repairAllSmallTableThreshold
		Print("When: run repair")
		h.runRepair(ctx, u)

		Print("Then: repair is running")
		h.assertRunning(shortWait)
		h.assertProgress(node11, 5, longWait)
		h.hrt.SetInterceptor(holdRepairInterceptor())

		Print("When: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(shortWait)
	})

	t.Run("repair restart", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.runRepair(ctx, multipleUnits())

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(shortWait)

		Print("When: create a new run")
		h.runID = uuid.NewTime()

		Print("And: run repair")
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		h.runRepair(ctx, multipleUnits())

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(longWait)

		Print("When: create a new run")
		h.runID = uuid.NewTime()

		Print("And: run repair")
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		h.runRepair(ctx, multipleUnits())

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair of all nodes continues")
		h.assertProgress(node11, 100, longWait)
		h.assertProgress(node12, 100, shortWait)
		h.assertProgress(node13, 100, shortWait)
	})

	t.Run("repair restart no continue", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		unit := singleUnit()
		unit.Continue = false

		Print("When: run repair")
		h.runRepair(ctx, unit)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(longWait)

		Print("When: create a new run")
		h.runID = uuid.NewTime()

		Print("And: run repair")
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		h.runRepair(ctx, unit)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair of node11 starts from scratch")
		if p, _ := h.progress(node11); p >= 50 {
			t.Fatal("node11 should start from scratch")
		}
	})

	t.Run("repair restart task properties changed", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.runRepair(ctx, multipleUnits())

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(shortWait)

		Print("When: create a new run")
		h.runID = uuid.NewTime()

		Print("And: run repair with modified units")
		modifiedUnits := multipleUnits()
		modifiedUnits.Units = []repair.Unit{{Keyspace: "test_repair", Tables: []string{"test_table_1"}}}
		modifiedUnits.DC = []string{"dc2"}

		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		h.runRepair(ctx, modifiedUnits)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair of node11 continues")
		h.assertProgress(node11, 100, shortWait)
		h.assertProgress(node12, 50, now)
		h.assertProgress(node13, 0, now)

		Print("And: dc2 is used for repair")
		prog, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}
		if diff := cmp.Diff(prog.DC, modifiedUnits.DC); diff != "" {
			h.t.Fatal(diff, prog)
		}
		if len(prog.Tables) != 1 {
			t.Fatal(prog.Tables)
		}
	})

	t.Run("repair restart fixes failed ranges", func(t *testing.T) {
		c := defaultConfig()

		h := newRepairTestHelper(t, session, c)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.runRepair(ctx, allUnits())

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: errors occur")
		h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandFailed))

		Print("Then: repair completes with error")
		h.assertError(shortWait)

		Print("When: create a new run")
		h.runID = uuid.NewTime()

		h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))

		Print("And: run repair")
		h.runRepair(ctx, allUnits())

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: node12 is 50% repaired")
		h.assertProgress(node12, 50, longWait)

		Print("And: repair is done")
		h.assertDone(shortWait)
	})

	t.Run("repair temporary network outage", func(t *testing.T) {
		c := defaultConfig()

		h := newRepairTestHelper(t, session, c)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.runRepair(ctx, singleUnit())

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: node12 is 50% repaired")
		h.assertProgress(node12, 50, longWait)

		Print("And: no network for 5s with 1s backoff")
		h.hrt.SetInterceptor(dialErrorInterceptor())
		time.AfterFunc(3*h.client.Config().Timeout, func() {
			h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
		})

		Print("Then: node12 repair continues")
		h.assertProgress(node12, 80, 3*h.client.Config().Timeout+shortWait)

		Print("When: node12 is 95% repaired")
		h.assertProgress(node12, 95, longWait)

		Print("Then: repair of node13 advances")
		h.assertProgress(node13, 1, longWait)

		Print("When: node13 is 100% repaired")
		h.assertProgress(node13, 100, longWait)

		Print("Then: node12 retries repair")
		h.assertProgress(node12, 100, shortWait)

		Print("And: repair is done")
		h.assertDone(shortWait)
	})

	t.Run("repair error fail fast", func(t *testing.T) {
		c := defaultConfig()

		h := newRepairTestHelper(t, session, c)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		u := allUnits()
		u.FailFast = true

		Print("When: run repair")
		h.runRepair(ctx, u)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: no network for 5s with 1s backoff")
		h.hrt.SetInterceptor(dialErrorInterceptor())
		time.AfterFunc(3*h.client.Config().Timeout, func() {
			h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
		})

		Print("Then: repair completes with error")
		h.assertError(longWait)
	})

	t.Run("repair non existing keyspace", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("Given: non-existing keyspace")

		target := singleUnit()
		target.Units[0].Keyspace = "non_existing_keyspace"

		Print("When: run repair")
		h.runRepair(ctx, target)

		Print("Then: repair fails")
		h.assertError(shortWait)
	})

	t.Run("drop table during repair", func(t *testing.T) {
		const (
			testKeyspace = "test_repair_drop_table"
			testTable    = "test_table_0"
		)

		createKeyspace(t, clusterSession, testKeyspace)
		WriteData(t, clusterSession, testKeyspace, 1, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		target := repair.Target{
			Units: []repair.Unit{
				{
					Keyspace: testKeyspace,
					Tables:   []string{testTable},
				},
			},
			DC:                  []string{"dc1", "dc2"},
			Intensity:           0,
			Parallel:            1,
			SmallTableThreshold: repairAllSmallTableThreshold,
		}

		Print("When: run repair")
		h.runRepair(ctx, target)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: node12 is 10% repaired")
		h.assertProgress(node12, 10, longWait)

		Print("And: table is dropped during repair")
		h.hrt.SetInterceptor(holdRepairInterceptor())
		h.assertMaxProgress(node12, 20, now)
		ExecStmt(t, clusterSession, fmt.Sprintf("DROP TABLE %s.%s", testKeyspace, testTable))
		h.hrt.SetInterceptor(nil)

		Print("Then: repair is done")
		h.assertDone(shortWait)
	})

	t.Run("drop keyspace during repair", func(t *testing.T) {
		const (
			testKeyspace = "test_repair_drop_table"
			testTable    = "test_table_0"
		)

		createKeyspace(t, clusterSession, testKeyspace)
		WriteData(t, clusterSession, testKeyspace, 1, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		target := repair.Target{
			Units: []repair.Unit{
				{
					Keyspace: testKeyspace,
					Tables:   []string{testTable},
				},
			},
			DC:                  []string{"dc1", "dc2"},
			Intensity:           1,
			SmallTableThreshold: repairAllSmallTableThreshold,
		}

		Print("When: run repair")
		h.runRepair(ctx, target)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: node12 is 10% repaired")
		h.assertProgress(node12, 10, longWait)

		Print("And: keyspace is dropped during repair")
		h.hrt.SetInterceptor(holdRepairInterceptor())
		h.assertMaxProgress(node12, 20, now)
		dropKeyspace(t, clusterSession, testKeyspace)
		h.hrt.SetInterceptor(nil)

		Print("Then: repair is done")
		h.assertDone(longWait)
	})

	t.Run("kill repairs on task failure", func(t *testing.T) {
		const killPath = "/storage_service/force_terminate_repair"

		target := multipleUnits()
		target.SmallTableThreshold = repairAllSmallTableThreshold

		t.Run("when task is cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			h := newRepairTestHelper(t, session, defaultConfig())
			var killRepairCalled int32
			h.hrt.SetInterceptor(countInterceptor(&killRepairCalled, killPath, "", nil))

			Print("When: run repair")
			h.runRepair(ctx, target)

			Print("When: repair is running")
			h.assertProgress(node11, 1, longWait)

			Print("When: repair is cancelled")
			cancel()
			h.assertError(shortWait)

			Print("Then: repairs on all hosts are killed")
			if int(killRepairCalled) != len(ManagedClusterHosts()) {
				t.Errorf("not all repairs were killed")
			}
		})

		t.Run("when fail fast is enabled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			h := newRepairTestHelper(t, session, defaultConfig())
			target.FailFast = true

			Print("When: run repair")
			h.runRepair(ctx, target)

			Print("When: repair is running")
			h.assertProgress(node11, 1, longWait)

			Print("When: Scylla returns failures")
			var killRepairCalled int32
			h.hrt.SetInterceptor(countInterceptor(&killRepairCalled, killPath, "", repairInterceptor(scyllaclient.CommandFailed)))

			Print("Then: repair finish with error")
			h.assertError(longWait)

			Print("Then: repairs on all hosts are killed")
			if int(killRepairCalled) != len(ManagedClusterHosts()) {
				t.Errorf("not all repairs were killed")
			}
		})
	})

	t.Run("small table optimisation", func(t *testing.T) {
		const (
			testKeyspace = "test_repair_small_table"
			testTable    = "test_table_0"

			repairPath = "/storage_service/repair_async/"
		)

		Print("Given: small and fully replicated table")
		ExecStmt(t, clusterSession, "CREATE KEYSPACE "+testKeyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}")
		WriteData(t, clusterSession, testKeyspace, 1, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		target := repair.Target{
			Units: []repair.Unit{
				{
					Keyspace: testKeyspace,
					Tables:   []string{testTable},
				},
			},
			DC:                  []string{"dc1", "dc2"},
			Intensity:           1,
			Parallel:            1,
			SmallTableThreshold: 1 * 1024 * 1024 * 1024,
		}

		Print("When: run repair")
		var repairCalled int32
		h.hrt.SetInterceptor(countInterceptor(&repairCalled, repairPath, http.MethodPost, repairInterceptor(scyllaclient.CommandSuccessful)))
		h.runRepair(ctx, target)

		Print("Then: repair is done")
		h.assertDone(longWait)

		Print("And: one repair task was issued")
		if repairCalled != 1 {
			t.Fatalf("Expected repair in one shot got %d", repairCalled)
		}

		p, err := h.service.GetProgress(context.Background(), h.clusterID, h.taskID, h.runID)
		if err != nil {
			h.t.Fatal(err)
		}

		if p.TokenRanges != 1 {
			t.Fatalf("Expected all tokens in one range, got %d ranges", p.TokenRanges)
		}
	})

	t.Run("repair status context timeout", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		u := allUnits()
		u.FailFast = true

		Print("When: repair status is not responding in time")
		h.hrt.SetInterceptor(repairStatusNoResponseInterceptor(ctx))

		Print("And: run repair")
		h.runRepair(ctx, u)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		h.hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))

		Print("Then: repair is done")
		h.assertDone(longWait)
	})
}

func TestServiceRepairErrorNodetoolRepairRunningIntegration(t *testing.T) {
	t.Skip("nodetool repair is executing too fast skipping until solution is found")
	clusterSession := CreateManagedClusterSessionAndDropAllKeyspaces(t)
	const ks = "test_repair"

	createKeyspace(t, clusterSession, ks)
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table_0 (id int PRIMARY KEY)")
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table_1 (id int PRIMARY KEY)")
	defer dropKeyspace(t, clusterSession, ks)

	// Repair can be very fast with newer versions of Scylla.
	// This fills keyspace with data to prolong nodetool repair execution.
	WriteData(t, clusterSession, ks, 10, generateTableNames(100)...)

	session := CreateSession(t)
	h := newRepairTestHelper(t, session, repair.DefaultConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	Print("Given: repair is running on a host")
	done := make(chan struct{})
	go func() {
		time.AfterFunc(1*time.Second, func() {
			close(done)
		})
		ExecOnHost(ManagedClusterHost(), "nodetool repair -ful -seq")
	}()
	defer func() {
		if err := h.client.KillAllRepairs(context.Background(), ManagedClusterHost()); err != nil {
			t.Fatal(err)
		}
	}()

	<-done
	Print("When: repair starts")
	h.runRepair(ctx, singleUnit())

	Print("Then: repair fails")
	h.assertErrorContains("active repair on hosts", longWait)
}

func generateTableNames(n int) []string {
	var out []string
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("table_%d", n))
	}
	return out
}

func TestServiceGetTargetSkipsKeyspaceHavingNoReplicasInGivenDCIntegration(t *testing.T) {
	clusterSession := CreateManagedClusterSessionAndDropAllKeyspaces(t)

	ExecStmt(t, clusterSession, "CREATE KEYSPACE test_repair_0 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair_0.test_table_0 (id int PRIMARY KEY)")
	defer dropKeyspace(t, clusterSession, "test_repair_0")

	ExecStmt(t, clusterSession, "CREATE KEYSPACE test_repair_1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 0}")
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair_1.test_table_0 (id int PRIMARY KEY)")
	defer dropKeyspace(t, clusterSession, "test_repair_1")

	session := CreateSession(t)
	h := newRepairTestHelper(t, session, repair.DefaultConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	properties := map[string]interface{}{
		"keyspace": []string{"test_repair_0", "test_repair_1"},
		"dc":       []string{"dc2"},
	}

	props, err := json.Marshal(properties)
	if err != nil {
		t.Fatal(err)
	}

	target, err := h.service.GetTarget(ctx, h.clusterID, props)
	if err != nil {
		t.Fatal(err)
	}

	if len(target.Units) != 1 {
		t.Errorf("Expected single unit in target, get %d", len(target.Units))
	}
	if target.Units[0].Keyspace != "test_repair_0" {
		t.Errorf("Expected only 'test_repair_0' keyspace in target units, got %s", target.Units[0].Keyspace)
	}
}
