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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/dht"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testhelper"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type repairTestHelper struct {
	*CommonTestHelper
	service *repair.Service

	mu     sync.RWMutex
	done   bool
	result error
}

func newRepairTestHelper(t *testing.T, session gocqlx.Session, config repair.Config) *repairTestHelper {
	t.Helper()

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)

	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
	c := newTestClient(t, hrt, log.NopLogger)
	s := newTestService(t, session, c, config, logger)

	return &repairTestHelper{
		CommonTestHelper: &CommonTestHelper{
			Logger:    logger,
			Session:   session,
			Hrt:       hrt,
			Client:    c,
			ClusterID: uuid.MustRandom(),
			TaskID:    uuid.MustRandom(),
			RunID:     uuid.NewTime(),
			T:         t,
		},
		service: s,
	}
}

func newRepairWithClusterSessionTestHelper(t *testing.T, session gocqlx.Session,
	hrt *HackableRoundTripper, c *scyllaclient.Client, config repair.Config) *repairTestHelper {
	t.Helper()

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	s := newTestServiceWithClusterSession(t, session, c, config, logger)

	return &repairTestHelper{
		CommonTestHelper: &CommonTestHelper{
			Logger:    logger,
			Session:   session,
			Hrt:       hrt,
			Client:    c,
			ClusterID: uuid.MustRandom(),
			TaskID:    uuid.MustRandom(),
			RunID:     uuid.NewTime(),
			T:         t,
		},
		service: s,
	}
}

func (h *repairTestHelper) runRepair(ctx context.Context, properties map[string]any) {
	go func() {
		var (
			t   repair.Target
			err error
		)

		h.mu.Lock()
		h.done = false
		h.result = nil
		h.mu.Unlock()
		defer func() {
			h.mu.Lock()
			h.done = true
			h.result = err
			h.mu.Unlock()
		}()

		h.Logger.Info(ctx, "Start generating target", "task", h.TaskID, "run", h.RunID)
		t, err = h.generateTarget(properties)
		h.Logger.Info(ctx, "End generating target", "task", h.TaskID, "run", h.RunID, "error", err)
		if err != nil {
			return
		}

		h.Logger.Info(ctx, "Running repair", "task", h.TaskID, "run", h.RunID)
		err = h.service.Repair(ctx, h.ClusterID, h.TaskID, h.RunID, t)
		h.Logger.Info(ctx, "Repair ended", "task", h.TaskID, "run", h.RunID, "error", err)
	}()
}

func (h *repairTestHelper) runRegularRepair(ctx context.Context, properties map[string]any) error {
	t, err := h.generateTarget(properties)
	if err != nil {
		return err
	}
	return h.service.Repair(ctx, h.ClusterID, h.TaskID, h.RunID, t)
}

// WaitCond parameters
const (
	// now specifies that condition shall be true in the current state.
	now = 0
	// shortWait specifies that condition shall be met in immediate future
	// such as repair filing on start.
	shortWait = 60 * time.Second
	// longWait specifies that condition shall be met after a while, this is
	// useful for waiting for repair to significantly advance or finish.
	longWait = 2 * shortWait

	_interval = 500 * time.Millisecond
)

func (h *repairTestHelper) assertRunning(wait time.Duration) {
	h.T.Helper()
	WaitCond(h.T, func() bool {
		p, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			if errors.Is(err, service.ErrNotFound) {
				return false
			}
			h.T.Fatal(err)
		}
		return p.Success > 0
	}, _interval, wait)
}

func (h *repairTestHelper) assertDone(wait time.Duration) {
	h.T.Helper()
	WaitCond(h.T, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		return h.done && h.result == nil
	}, _interval, wait)
}

func (h *repairTestHelper) assertProgressSuccess() {
	p, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
	if err != nil {
		h.T.Fatal(err)
	}

	Print("And: there are no more errors")
	if p.Error != 0 {
		h.T.Fatal("expected", 0, "got", p.Error)
	}
	if p.Success != p.TokenRanges {
		h.T.Fatal("expected", p.TokenRanges, "got", p.Success)
	}
}

func (h *repairTestHelper) assertError(wait time.Duration) {
	h.T.Helper()
	WaitCond(h.T, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		return h.done && h.result != nil
	}, _interval, wait)
}

func (h *repairTestHelper) assertErrorContains(cause string, wait time.Duration) {
	h.T.Helper()

	WaitCond(h.T, func() bool {
		h.mu.RLock()
		defer h.mu.RUnlock()
		return h.done && h.result != nil && strings.Contains(h.result.Error(), cause)
	}, _interval, wait)
}

func (h *repairTestHelper) assertStopped(wait time.Duration) {
	h.T.Helper()
	h.assertErrorContains(context.Canceled.Error(), wait)
}

func (h *repairTestHelper) assertProgress(percent int, wait time.Duration) {
	h.T.Helper()
	WaitCond(h.T, func() bool {
		p, _ := h.progress()
		return p >= percent
	}, _interval, wait)
}

func (h *repairTestHelper) assertProgressFailed(percent int, wait time.Duration) {
	h.T.Helper()
	WaitCond(h.T, func() bool {
		_, f := h.progress()
		return f >= percent
	}, _interval, wait)
}

func (h *repairTestHelper) assertMaxProgress(percent int, wait time.Duration) {
	h.T.Helper()
	WaitCond(h.T, func() bool {
		p, _ := h.progress()
		return p <= percent
	}, _interval, wait)
}

func (h *repairTestHelper) progress() (int, int) {
	h.T.Helper()
	p, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
	if err != nil {
		h.T.Fatal(err)
	}

	return percentComplete(p)
}

func (h *repairTestHelper) assertParallelIntensity(parallel, intensity int) {
	h.T.Helper()

	p, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
	if err != nil {
		h.T.Fatal(err)
	}

	if p.Parallel != parallel || int(p.Intensity) != intensity {
		h.T.Fatalf("Expected parallel %d, intensity %d, got parallel %d, intensity %d", parallel, intensity, p.Parallel, int(p.Intensity))
	}
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
			resp.Body = io.NopCloser(bytes.NewBufferString(fmt.Sprintf("\"%s\"", scyllaclient.StatusSuccess)))
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

func holdRepairInterceptor(ctx context.Context, after int64) http.RoundTripper {
	cnt := &atomic.Int64{}
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if repairEndpointRegexp.MatchString(req.URL.Path) && req.Method == http.MethodGet {
			if curr := cnt.Add(1); curr > after {
				<-ctx.Done()
				return nil, nil
			}
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
		func(ctx context.Context, clusterID uuid.UUID) (gocqlx.Session, error) {
			return gocqlx.Session{}, errors.New("not implemented")
		},
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func newTestServiceWithClusterSession(t *testing.T, session gocqlx.Session, client *scyllaclient.Client, c repair.Config, logger log.Logger) *repair.Service {
	t.Helper()

	s, err := repair.NewService(
		session,
		c,
		metrics.NewRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID) (gocqlx.Session, error) {
			return CreateSession(t, client), nil
		},
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func createKeyspace(t *testing.T, session gocqlx.Session, keyspace string, rf1, rf2 int) {
	createKeyspaceStmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d}"
	ExecStmt(t, session, fmt.Sprintf(createKeyspaceStmt, keyspace, rf1, rf2))
}

func dropKeyspace(t *testing.T, session gocqlx.Session, keyspace string) {
	ExecStmt(t, session, fmt.Sprintf("DROP KEYSPACE IF EXISTS %q", keyspace))
}

// We must use -1 here to support working with empty tables or not flushed data.
const repairAllSmallTableThreshold = -1

// allUnits decorate predefined properties with provided ones.
func allUnits(properties map[string]any) map[string]any {
	m := map[string]any{
		"keyspace": []string{
			"test_repair.test_table_0", "test_repair.test_table_1",
			"system_schema.indexes", "system_schema.keyspaces"},
		"dc":        []string{"dc1", "dc2"},
		"continue":  true,
		"intensity": 10,
	}
	for k, v := range properties {
		m[k] = v
	}
	return m
}

// multipleUnits decorate predefined properties with provided ones.
func multipleUnits(properties map[string]any) map[string]any {
	m := map[string]any{
		"keyspace":  []string{"test_repair.test_table_0", "test_repair.test_table_1"},
		"dc":        []string{"dc1", "dc2"},
		"continue":  true,
		"intensity": 10,
	}
	for k, v := range properties {
		m[k] = v
	}
	return m
}

// singleUnit decorate predefined properties with provided ones.
func singleUnit(properties map[string]any) map[string]any {
	m := map[string]any{
		"keyspace":  []string{"test_repair.test_table_0"},
		"dc":        []string{"dc1", "dc2"},
		"continue":  true,
		"intensity": 10,
	}
	for k, v := range properties {
		m[k] = v
	}
	return m

}

// generateTarget applies GetTarget onto given properties.
// It's useful for filling keyspace boilerplate.
func (h *repairTestHelper) generateTarget(properties map[string]any) (repair.Target, error) {
	h.T.Helper()

	props, err := json.Marshal(properties)
	if err != nil {
		h.T.Fatal(err)
	}

	return h.service.GetTarget(context.Background(), h.ClusterID, props)
}

func TestServiceGetTargetIntegration(t *testing.T) {
	// Test names
	testNames := []string{
		"default",
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
		session = CreateScyllaManagerDBSession(t)
		h       = newRepairTestHelper(t, session, repair.DefaultConfig())
		ctx     = context.Background()
	)

	CreateSessionAndDropAllKeyspaces(t, h.Client)

	for _, testName := range testNames {
		t.Run(testName, func(t *testing.T) {
			input := ReadInputFile(t)
			v, err := h.service.GetTarget(ctx, h.ClusterID, input)
			if err != nil {
				t.Fatal(err)
			}

			SaveGoldenJSONFileIfNeeded(t, v)

			var golden repair.Target
			LoadGoldenJSONFile(t, &golden)

			if diff := cmp.Diff(golden, v,
				cmpopts.SortSlices(func(a, b string) bool { return a < b }),
				cmpopts.SortSlices(func(u1, u2 repair.Unit) bool { return u1.Keyspace < u2.Keyspace }),
				cmpopts.IgnoreUnexported(repair.Target{}),
				cmpopts.IgnoreSliceElements(func(u repair.Unit) bool { return u.Keyspace == "system_replicated_keys" || u.Keyspace == "system_auth" }),
				cmpopts.IgnoreSliceElements(func(t string) bool { return t == "dicts" })); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestServiceRepairOneJobPerHostIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)
	h := newRepairTestHelper(t, session, repair.DefaultConfig())
	clusterSession := CreateSessionAndDropAllKeyspaces(t, h.Client)

	const (
		ks1           = "test_repair_rf_1"
		ks2           = "test_repair_rf_2"
		ks3           = "test_repair_rf_3"
		t1            = "test_table_1"
		t2            = "test_table_2"
		maxJobsOnHost = 1
	)
	createKeyspace(t, clusterSession, ks1, 1, 1)
	createKeyspace(t, clusterSession, ks2, 2, 2)
	createKeyspace(t, clusterSession, ks3, 3, 3)
	WriteData(t, clusterSession, ks1, 5, t1, t2)
	WriteData(t, clusterSession, ks2, 5, t1, t2)
	WriteData(t, clusterSession, ks3, 5, t1, t2)

	t.Run("repair schedules only one job per host at any given time", func(t *testing.T) {
		ctx := context.Background()

		props := map[string]any{
			"fail_fast": true,
		}

		// The amount of currently executed repair jobs on host
		jobsPerHost := make(map[string]int)
		muJPH := sync.Mutex{}

		// Set of hosts used for given repair job
		hostsInJob := make(map[string][]string)
		muHIJ := sync.Mutex{}

		cnt := atomic.Int64{}

		// Repair request
		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if repairEndpointRegexp.MatchString(req.URL.Path) && req.Method == http.MethodPost {
				hosts := strings.Split(req.URL.Query()["hosts"][0], ",")
				muJPH.Lock()
				defer muJPH.Unlock()

				for _, host := range hosts {
					jobsPerHost[host]++
					if jobsPerHost[host] > maxJobsOnHost {
						cnt.Add(1)
						return nil, nil
					}
				}
			}

			return nil, nil
		}))

		h.Hrt.SetRespNotifier(func(resp *http.Response, err error) {
			if resp == nil {
				return
			}

			var copiedBody bytes.Buffer
			tee := io.TeeReader(resp.Body, &copiedBody)
			body, _ := io.ReadAll(tee)
			resp.Body = io.NopCloser(&copiedBody)

			// Response to repair schedule
			if repairEndpointRegexp.MatchString(resp.Request.URL.Path) && resp.Request.Method == http.MethodPost {
				muHIJ.Lock()
				hostsInJob[resp.Request.Host+string(body)] = strings.Split(resp.Request.URL.Query()["hosts"][0], ",")
				muHIJ.Unlock()
			}

			// Response to repair status
			if repairEndpointRegexp.MatchString(resp.Request.URL.Path) && resp.Request.Method == http.MethodGet {
				status := string(body)
				if status == "\"SUCCESSFUL\"" || status == "\"FAILED\"" {
					muHIJ.Lock()
					hosts := hostsInJob[resp.Request.Host+resp.Request.URL.Query()["id"][0]]
					muHIJ.Unlock()

					muJPH.Lock()
					defer muJPH.Unlock()

					for _, host := range hosts {
						jobsPerHost[host]--
					}
				}
			}
		})

		Print("When: run repair")
		if err := h.runRegularRepair(ctx, props); err != nil {
			t.Errorf("Repair failed: %s", err)
		}

		if cnt.Load() > 0 {
			t.Fatal("too many repair jobs are being executed on host")
		}
	})
}

func TestServiceRepairOrderIntegration(t *testing.T) {
	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
	c := newTestClient(t, hrt, log.NopLogger)
	ctx := context.Background()

	session := CreateScyllaManagerDBSession(t)
	clusterSession := CreateSessionAndDropAllKeyspaces(t, c)
	h := newRepairWithClusterSessionTestHelper(t, session, hrt, c, repair.DefaultConfig())

	// Add prefixes ruining lexicographic order
	const (
		ks1 = "zz_test_repair_1"
		ks2 = "hh_test_repair_2"
		ks3 = "aa_test_repair_3"
		t1  = "zz_test_table_1"
		t2  = "hh_test_table_2"
		t3  = "aa_test_table_3"
		si1 = "aa_test_si_1"
		mv1 = "zz_test_mv_1"
		mv2 = "hh_test_mv_2"
	)

	// Create keyspaces. Low RF improves repair parallelism.
	createKeyspace(t, clusterSession, ks1, 1, 1)
	createKeyspace(t, clusterSession, ks2, 1, 1)
	createKeyspace(t, clusterSession, ks3, 2, 1)

	// Create and fill tables
	WriteData(t, clusterSession, ks1, 1, t1)
	WriteData(t, clusterSession, ks1, 2, t2)

	WriteData(t, clusterSession, ks2, 1, t1)
	WriteData(t, clusterSession, ks2, 2, t2)
	WriteData(t, clusterSession, ks2, 3, t3)

	WriteData(t, clusterSession, ks3, 20, t1)

	// Create views
	CreateMaterializedView(t, clusterSession, ks1, t1, mv1)

	CreateSecondaryIndex(t, clusterSession, ks2, t1, si1)
	CreateMaterializedView(t, clusterSession, ks2, t2, mv1)
	CreateMaterializedView(t, clusterSession, ks2, t3, mv2)

	// Flush tables for correct memory calculations
	FlushTable(t, c, ManagedClusterHosts(), ks1, t1)
	FlushTable(t, c, ManagedClusterHosts(), ks1, t2)
	FlushTable(t, c, ManagedClusterHosts(), ks1, mv1)

	FlushTable(t, c, ManagedClusterHosts(), ks2, t1)
	FlushTable(t, c, ManagedClusterHosts(), ks2, t2)
	FlushTable(t, c, ManagedClusterHosts(), ks2, t3)
	FlushTable(t, c, ManagedClusterHosts(), ks2, si1+"_index")
	FlushTable(t, c, ManagedClusterHosts(), ks2, mv1)
	FlushTable(t, c, ManagedClusterHosts(), ks2, mv2)

	FlushTable(t, c, ManagedClusterHosts(), ks3, t1)

	expectedRepairOrder := []string{
		"system_auth.*",
		"system_replicated_keys.*",
		"system_distributed.*",
		"system_distributed_everywhere.*",
		"system_traces.*",

		ks1 + "." + t1,
		ks1 + "." + t2,
		ks1 + "." + mv1,

		ks2 + "." + t1,
		ks2 + "." + t2,
		ks2 + "." + t3,
		ks2 + "." + si1 + "_index",
		ks2 + "." + mv1,
		ks2 + "." + mv2,

		ks3 + "." + t1,
	}

	props := map[string]any{
		"fail_fast": true,
	}

	// Shows the actual order in which tables are repaired
	var actualRepairOrder []string
	muARO := sync.Mutex{}

	// Maps job ID to corresponding table
	jobTable := make(map[string]string)
	muJT := sync.Mutex{}

	// Repair request
	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if repairEndpointRegexp.MatchString(req.URL.Path) && req.Method == http.MethodPost {
			pathParts := strings.Split(req.URL.Path, "/")
			keyspace := pathParts[len(pathParts)-1]
			fullTable := keyspace + "." + req.URL.Query().Get("columnFamilies")

			// Update actual repair order on both repair start and end
			muARO.Lock()
			if len(actualRepairOrder) == 0 || actualRepairOrder[len(actualRepairOrder)-1] != fullTable {
				actualRepairOrder = append(actualRepairOrder, fullTable)
			}
			muARO.Unlock()
		}

		return nil, nil
	}))

	h.Hrt.SetRespNotifier(func(resp *http.Response, err error) {
		if resp == nil {
			return
		}

		var copiedBody bytes.Buffer
		tee := io.TeeReader(resp.Body, &copiedBody)
		body, _ := io.ReadAll(tee)
		resp.Body = io.NopCloser(&copiedBody)

		// Response to repair schedule
		if repairEndpointRegexp.MatchString(resp.Request.URL.Path) && resp.Request.Method == http.MethodPost {
			pathParts := strings.Split(resp.Request.URL.Path, "/")
			keyspace := pathParts[len(pathParts)-1]
			fullTable := keyspace + "." + resp.Request.URL.Query().Get("columnFamilies")

			// Register what table is being repaired
			muJT.Lock()
			jobTable[resp.Request.Host+string(body)] = fullTable
			muJT.Unlock()
		}

		// Response to repair status
		if repairEndpointRegexp.MatchString(resp.Request.URL.Path) && resp.Request.Method == http.MethodGet {
			status := string(body)
			if status == "\"SUCCESSFUL\"" || status == "\"FAILED\"" {
				// Add host prefix as IDs are unique only for a given host
				jobID := resp.Request.Host + resp.Request.URL.Query()["id"][0]

				muJT.Lock()
				fullTable := jobTable[jobID]
				muJT.Unlock()

				if fullTable == "" {
					t.Logf("This is strange %s", jobID)
					return
				}

				// Update actual repair order on both repair start and end
				muARO.Lock()
				if len(actualRepairOrder) == 0 || actualRepairOrder[len(actualRepairOrder)-1] != fullTable {
					actualRepairOrder = append(actualRepairOrder, fullTable)
				}
				muARO.Unlock()
			}
		}
	})

	Print("When: run repair")
	if err := h.runRegularRepair(ctx, props); err != nil {
		t.Errorf("Repair failed: %s", err)
	}

	Print("When: validate repair order")
	t.Logf("Expected repair order: %v", expectedRepairOrder)
	t.Logf("Recorded repair order: %v", actualRepairOrder)

	// If there are duplicates in actual repair order, then it means that different tables had overlapping repairs.
	if strset.New(actualRepairOrder...).Size() != len(actualRepairOrder) {
		t.Fatal("Table by table requirement wasn't preserved")
	}

	expIdx := 0
	actIdx := 0
	for actIdx < len(actualRepairOrder) {
		expTable := expectedRepairOrder[expIdx]
		actTable := actualRepairOrder[actIdx]

		if expTable == actTable {
			t.Logf("Exact match: %s %s", expTable, actTable)
			expIdx++
			actIdx++
			continue
		}

		expParts := strings.Split(expTable, ".")
		actParts := strings.Split(actTable, ".")
		if expParts[1] == "*" {
			if expParts[0] == actParts[0] {
				t.Logf("Star match: %s %s", expTable, actTable)
				actIdx++
				continue
			}

			t.Logf("Leave star behind: %s %s", expTable, actTable)
			expIdx++
			continue
		}

		t.Fatalf("No match: %s %s", expTable, actTable)
	}

	if expIdx != len(expectedRepairOrder) || actIdx != len(actualRepairOrder) {
		t.Fatalf("Expected repair order and actual repair order didn't match, expIdx: %d, actIdx: %d", expIdx, actIdx)
	}
}

func TestServiceRepairResumeAllRangesIntegration(t *testing.T) {
	// This test checks if repair targets the full token range even when it is paused and resumed multiple times.
	// It also checks if too many token ranges were redundantly repaired multiple times.

	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
	c := newTestClient(t, hrt, log.NopLogger)
	ctx := context.Background()

	session := CreateScyllaManagerDBSession(t)
	clusterSession := CreateSessionAndDropAllKeyspaces(t, c)
	cfg := repair.DefaultConfig()
	cfg.GracefulStopTimeout = time.Second
	h := newRepairWithClusterSessionTestHelper(t, session, hrt, c, cfg)

	const (
		ks1 = "test_repair_1"
		ks2 = "test_repair_2"
		ks3 = "test_repair_3"
		t1  = "test_table_1"
		t2  = "test_table_2"
		t3  = "test_table_3"
		si1 = "test_si_1"
		mv1 = "test_mv_1"
		mv2 = "test_mv_2"
		// This value has been chosen without much reasoning, but it seems fine.
		// Each table consists of 6 * 256 token ranges (total number of vnodes in keyspace),
		// so if stopping repair with short graceful stop timeout 4 times makes us
		// redo only that many ranges, everything should be fine.
		redundantLimit = 20
	)

	// Create keyspaces. Low RF increases repair parallelism.
	createKeyspace(t, clusterSession, ks1, 2, 1)
	createKeyspace(t, clusterSession, ks2, 1, 1)
	createKeyspace(t, clusterSession, ks3, 1, 1)

	// Create and fill tables
	WriteData(t, clusterSession, ks1, 1, t1)
	WriteData(t, clusterSession, ks1, 2, t2)

	WriteData(t, clusterSession, ks2, 1, t1)
	WriteData(t, clusterSession, ks2, 2, t2)
	WriteData(t, clusterSession, ks2, 3, t3)

	WriteData(t, clusterSession, ks3, 5, t1)

	// Create views
	CreateMaterializedView(t, clusterSession, ks1, t1, mv1)

	CreateSecondaryIndex(t, clusterSession, ks2, t1, si1)
	CreateMaterializedView(t, clusterSession, ks2, t2, mv1)
	CreateMaterializedView(t, clusterSession, ks2, t3, mv2)

	props := map[string]any{
		"fail_fast":             false,
		"continue":              true,
		"small_table_threshold": repairAllSmallTableThreshold,
	}

	type TableRange struct {
		FullTable string
		Ranges    []scyllaclient.TokenRange
	}

	// Maps job ID to corresponding table and ranges
	jobSpec := make(map[string]TableRange)
	muJS := sync.Mutex{}

	// Maps table to repaired ranges
	doneRanges := make(map[string][]scyllaclient.TokenRange)
	muDR := sync.Mutex{}

	parseRanges := func(dumpedRanges string) []scyllaclient.TokenRange {
		var out []scyllaclient.TokenRange
		for _, r := range strings.Split(dumpedRanges, ",") {
			tokens := strings.Split(r, ":")
			s, err := strconv.ParseInt(tokens[0], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			e, err := strconv.ParseInt(tokens[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, scyllaclient.TokenRange{
				StartToken: s,
				EndToken:   e,
			})
		}
		return out
	}

	// Tools for performing a repair with 4 pauses
	var (
		cnt             = atomic.Int64{}
		stop1Ctx, stop1 = context.WithCancel(ctx)
		stop2Ctx, stop2 = context.WithCancel(ctx)
		stop3Ctx, stop3 = context.WithCancel(ctx)
		stop4Ctx, stop4 = context.WithCancel(ctx)
		stopCnt1        = 150
		stopCnt2        = 250
		stopCnt3        = 400
		stopCnt4        = 500
	)

	// Repair request
	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if repairEndpointRegexp.MatchString(req.URL.Path) && req.Method == http.MethodPost {
			switch int(cnt.Add(1)) {
			case stopCnt1:
				stop1()
				t.Log("First repair pause")
			case stopCnt2:
				stop2()
				t.Log("Second repair pause")
			case stopCnt3:
				stop3()
				t.Log("Third repair pause")
			case stopCnt4:
				stop4()
				t.Log("Fourth repair pause")
			}
		}

		return nil, nil
	}))

	h.Hrt.SetRespNotifier(func(resp *http.Response, err error) {
		if resp == nil {
			return
		}

		var copiedBody bytes.Buffer
		tee := io.TeeReader(resp.Body, &copiedBody)
		body, _ := io.ReadAll(tee)
		resp.Body = io.NopCloser(&copiedBody)

		// Response to repair schedule
		if repairEndpointRegexp.MatchString(resp.Request.URL.Path) && resp.Request.Method == http.MethodPost {
			pathParts := strings.Split(resp.Request.URL.Path, "/")
			keyspace := pathParts[len(pathParts)-1]
			fullTable := keyspace + "." + resp.Request.URL.Query().Get("columnFamilies")
			ranges := parseRanges(resp.Request.URL.Query().Get("ranges"))

			// Register what table is being repaired
			muJS.Lock()
			jobSpec[resp.Request.Host+string(body)] = TableRange{
				FullTable: fullTable,
				Ranges:    ranges,
			}
			muJS.Unlock()
		}

		// Response to repair status
		if repairEndpointRegexp.MatchString(resp.Request.URL.Path) && resp.Request.Method == http.MethodGet {
			// Inject 5% errors on all runs except the last one.
			// This helps to test repair error resilience.
			if i := cnt.Load(); i < int64(stopCnt4) && i%20 == 0 {
				resp.Body = io.NopCloser(bytes.NewBufferString(fmt.Sprintf("%q", scyllaclient.CommandFailed)))
				return
			}

			status := string(body)
			if status == "\"SUCCESSFUL\"" {
				muJS.Lock()
				tr := jobSpec[resp.Request.Host+resp.Request.URL.Query()["id"][0]]
				muJS.Unlock()

				if tr.FullTable == "" {
					t.Logf("This is strange %s", resp.Request.Host+resp.Request.URL.Query()["id"][0])
					return
				}

				// Register done ranges
				muDR.Lock()
				dr := doneRanges[tr.FullTable]
				dr = append(dr, tr.Ranges...)
				doneRanges[tr.FullTable] = dr
				muDR.Unlock()
			}
		}
	})

	Print("When: run first repair with context cancel")
	if err := h.runRegularRepair(stop1Ctx, props); err == nil {
		t.Fatal("Repair failed without error")
	}

	Print("When: run second repair with context cancel")
	h.RunID = uuid.NewTime()
	if err := h.runRegularRepair(stop2Ctx, props); err == nil {
		t.Fatal("Repair failed without error")
	}

	Print("When: run third repair with context cancel")
	h.RunID = uuid.NewTime()
	if err := h.runRegularRepair(stop3Ctx, props); err == nil {
		t.Fatal("Repair failed without error")
	}

	Print("When: run fourth repair with context cancel")
	h.RunID = uuid.NewTime()
	if err := h.runRegularRepair(stop4Ctx, props); err == nil {
		t.Fatal("Repair failed without error")
	}

	validate := func(tab string, tr []scyllaclient.TokenRange) (redundant int, err error) {
		sort.Slice(tr, func(i, j int) bool {
			return tr[i].StartToken < tr[j].StartToken
		})

		// Check that full token range is repaired
		if tr[0].StartToken != dht.Murmur3MinToken {
			return 0, errors.Errorf("expected min token %d, got %d", dht.Murmur3MinToken, tr[0].StartToken)
		}
		if tr[len(tr)-1].EndToken != dht.Murmur3MaxToken {
			return 0, errors.Errorf("expected max token %d, got %d", dht.Murmur3MaxToken, tr[len(tr)-1].EndToken)
		}

		// Check if repaired token ranges are continuous
		for i := 1; i < len(tr); i++ {
			// Redundant ranges might occur due to pausing repair,
			// but there shouldn't be much of them.
			if tr[i-1] == tr[i] {
				redundant++
				continue
			}
			if tr[i-1].EndToken != tr[i].StartToken {
				return 0, errors.Errorf("non continuous token ranges %v and %v", tr[i-1], tr[i])
			}
		}

		return redundant, nil
	}

	// Tablet tables don't support resuming repair, so in order to check if repair
	// started from scratch, we need to remove all repaired ranges up to this point.
	ringDescriber := scyllaclient.NewRingDescriber(ctx, h.Client)
	for tab, _ := range doneRanges {
		_, err := validate(tab, doneRanges[tab])
		if err != nil && ringDescriber.IsTabletKeyspace(strings.Split(tab, ".")[0]) {
			doneRanges[tab] = nil
		}
	}

	Print("When: run fifth repair till it finishes")
	h.RunID = uuid.NewTime()
	if err := h.runRegularRepair(ctx, props); err != nil {
		t.Fatalf("Repair failed: %s", err)
	}

	Print("When: validate all, continuous ranges")
	redundant := 0
	for tab, tr := range doneRanges {
		t.Logf("Checking table %s", tab)

		r, err := validate(tab, tr)
		if err != nil {
			t.Fatal(err)
		}
		redundant += r
	}

	t.Logf("Overall redundant ranges %d", redundant)
	if redundant > redundantLimit {
		t.Fatalf("Expected less redundant token ranges than %d", redundant)
	}
}

func TestServiceRepairIntegration(t *testing.T) {
	defaultConfig := func() repair.Config {
		c := repair.DefaultConfig()
		c.PollInterval = 10 * time.Millisecond
		return c
	}
	session := CreateScyllaManagerDBSession(t)
	h := newRepairTestHelper(t, session, defaultConfig())
	clusterSession := CreateSessionAndDropAllKeyspaces(t, h.Client)

	createKeyspace(t, clusterSession, "test_repair", 2, 2)
	WriteData(t, clusterSession, "test_repair", 1, "test_table_0", "test_table_1")
	defer dropKeyspace(t, clusterSession, "test_repair")

	t.Run("repair simple", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.runRepair(ctx, multipleUnits(nil))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair is done")
		h.assertDone(longWait)

		Print("And: nodes are 100% repaired")
		h.assertProgress(100, now)
	})

	t.Run("repair dc", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.runRepair(ctx, multipleUnits(map[string]any{
			"dc": []string{"dc2"},
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: repair is done")
		h.assertDone(longWait)

		Print("Then: dc2 is used for repair")
		prog, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			h.T.Fatal(err)
		}
		if diff := cmp.Diff(prog.DC, []string{"dc2"}); diff != "" {
			h.T.Fatal(diff)
		}
		for _, h := range prog.Hosts {
			if !IPBelongsToDC("dc2", h.Host) {
				t.Errorf("%s does not belong to DC2", h.Host)
			}
		}
	})

	t.Run("repair ignore hosts", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var ignored = IPFromTestNet("12")
		ni, err := h.Client.NodeInfo(ctx, ignored)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = ExecOnHost(ignored, "sudo supervisorctl stop scylla")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_, _, err := ExecOnHost(ignored, "sudo supervisorctl start scylla")
			if err != nil {
				t.Fatal(err)
			}

			cfg := cqlping.Config{
				Addr:    ni.CQLAddr(ignored),
				Timeout: time.Minute,
			}

			cond := func() bool {
				if _, err = cqlping.QueryPing(ctx, cfg, TestDBUsername(), TestDBPassword()); err != nil {
					return false
				}
				status, err := h.Client.Status(ctx)
				if err != nil {
					return false
				}
				return len(status.Live()) == len(ManagedClusterHosts())
			}

			WaitCond(t, cond, time.Second, shortWait)
		}()

		Print("When: run repair")
		h.runRepair(ctx, singleUnit(map[string]any{
			"ignore_down_hosts": true,
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: repair is done")
		h.assertDone(longWait)

		Print("Then: ignored node is not repaired")
		prog, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			h.T.Fatal(err)
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

		host := h.GetHostsFromDC("dc1")[0]
		h.Hrt.SetInterceptor(assertReplicasRepairInterceptor(t, host))

		Print("When: run repair")
		h.runRepair(ctx, singleUnit(map[string]any{
			"host": host,
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("When: repair is done")
		h.assertDone(longWait)
	})

	t.Run("repair dc local keyspace mismatch", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("Given: dc2 only keyspace")
		ExecStmt(t, clusterSession, "CREATE KEYSPACE IF NOT EXISTS test_repair_dc2 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc2': 3}")
		ExecStmt(t, clusterSession, "CREATE TABLE IF NOT EXISTS test_repair_dc2.test_table_0 (id int PRIMARY KEY)")

		Print("When: run repair with dc1")
		h.runRepair(ctx, singleUnit(map[string]any{
			"keyspace": []string{"test_repair_dc2"},
			"dc":       []string{"dc1"},
		}))

		Print("When: repair fails")
		h.assertErrorContains("no replicas to repair", shortWait)
	})

	t.Run("repair stop", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.Hrt.SetInterceptor(holdRepairInterceptor(ctx, 2))
		h.runRepair(ctx, multipleUnits(map[string]any{
			"small_table_threshold": repairAllSmallTableThreshold,
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

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

		Print("When: run repair")
		h.Hrt.SetInterceptor(holdRepairInterceptor(ctx, 2))
		h.runRepair(ctx, multipleUnits(map[string]any{
			"small_table_threshold": repairAllSmallTableThreshold,
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

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
		h.Hrt.SetInterceptor(holdRepairInterceptor(ctx, 1))
		h.runRepair(ctx, multipleUnits(nil))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(shortWait)

		Print("When: create a new run")
		h.RunID = uuid.NewTime()

		Print("And: run repair")
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		h.Hrt.SetInterceptor(holdRepairInterceptor(ctx, 1))
		h.runRepair(ctx, multipleUnits(nil))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(shortWait)

		Print("When: create a new run")
		h.RunID = uuid.NewTime()

		Print("And: run repair")
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		h.Hrt.SetInterceptor(nil)
		h.runRepair(ctx, multipleUnits(nil))

		Print("And: repair of all nodes continues")
		h.assertProgress(100, longWait)
	})

	t.Run("repair restart respect repair control", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const (
			propParallel               = 0
			propIntensity              = 0
			controlParallel            = 1
			controlIntensity           = 1
			deprecatedControlIntensity = 0.5
		)

		Print("When: run repair")
		props := singleUnit(map[string]any{
			"parallel":              propParallel,
			"intensity":             propIntensity,
			"small_table_threshold": -1,
		})
		h.Hrt.SetInterceptor(holdRepairInterceptor(ctx, 1))
		h.runRepair(ctx, props)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("Then: assert parallel/intensity from properties")
		h.assertParallelIntensity(propParallel, propIntensity)

		Print("And: control parallel and intensity")
		if err := h.service.SetParallel(ctx, h.ClusterID, controlParallel); err != nil {
			t.Fatal(err)
		}
		if err := h.service.SetIntensity(ctx, h.ClusterID, deprecatedControlIntensity); err != nil {
			t.Fatal(err)
		}

		Print("Then: assert parallel/intensity from control")
		h.assertParallelIntensity(controlParallel, controlIntensity)

		Print("And: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(shortWait)

		Print("When: create a new run")
		h.RunID = uuid.NewTime()

		Print("And: resume repair")
		ctx = context.Background()
		holdCtx, holdCancel := context.WithCancel(ctx)
		h.Hrt.SetInterceptor(holdRepairInterceptor(holdCtx, 1))
		h.runRepair(ctx, props)

		Print("Then: resumed repair is running")
		h.assertRunning(shortWait)

		Print("Then: assert resumed, running parallel/intensity from control")
		h.assertParallelIntensity(controlParallel, controlIntensity)

		Print("Then: repair is done")
		holdCancel()
		h.assertDone(longWait)

		Print("Then: assert resumed, finished  parallel/intensity from control")
		h.assertParallelIntensity(controlParallel, controlIntensity)

		Print("And: run fresh repair")
		h.RunID = uuid.NewTime()
		holdCtx, holdCancel = context.WithCancel(ctx)
		h.Hrt.SetInterceptor(holdRepairInterceptor(holdCtx, 1))
		h.runRepair(ctx, props)

		Print("Then: fresh repair is running")
		h.assertRunning(shortWait)

		Print("Then: assert fresh, running parallel/intensity from properties")
		h.assertParallelIntensity(propParallel, propIntensity)

		Print("Then: repair is done")
		holdCancel()
		h.assertDone(longWait)

		Print("Then: assert fresh, finished repair parallel/intensity from control")
		h.assertParallelIntensity(propParallel, propIntensity)
	})

	t.Run("repair restart no continue", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		props := singleUnit(map[string]any{
			"continue":              false,
			"small_table_threshold": repairAllSmallTableThreshold,
		})

		Print("When: run repair")
		h.Hrt.SetInterceptor(holdRepairInterceptor(ctx, 2))
		h.runRepair(ctx, props)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(longWait)

		Print("When: create a new run")
		h.RunID = uuid.NewTime()

		Print("And: run repair")
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		h.Hrt.SetInterceptor(holdRepairInterceptor(ctx, 0))
		h.runRepair(ctx, props)

		WaitCond(h.T, func() bool {
			_, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
			if err != nil {
				if errors.Is(err, service.ErrNotFound) {
					return false
				}
				h.T.Fatal(err)
			}
			return true
		}, _interval, shortWait)

		Print("And: repair starts from scratch")
		if p, _ := h.progress(); p > 0 {
			t.Fatal("repair should start from scratch")
		}
	})

	t.Run("repair restart task properties changed", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.Hrt.SetInterceptor(holdRepairInterceptor(ctx, 1))
		h.runRepair(ctx, multipleUnits(nil))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair is stopped")
		cancel()

		Print("Then: status is StatusStopped")
		h.assertStopped(shortWait)

		Print("When: create a new run")
		h.RunID = uuid.NewTime()

		Print("And: run repair with modified units")
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		h.runRepair(ctx, multipleUnits(map[string]any{
			"keyspace": []string{"test_repair.test_table_1"},
			"dc":       []string{"dc2"},
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: repair continues")
		h.assertDone(shortWait)

		Print("And: dc2 is used for repair")
		prog, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			h.T.Fatal(err)
		}
		if diff := cmp.Diff(prog.DC, []string{"dc2"}); diff != "" {
			h.T.Fatal(diff, prog)
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
		holdCtx, holdCancel := context.WithCancel(ctx)
		h.Hrt.SetInterceptor(holdRepairInterceptor(holdCtx, 1))
		h.runRepair(ctx, allUnits(map[string]any{
			"fail_fast":             true,
			"small_table_threshold": repairAllSmallTableThreshold,
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: errors occur")
		h.Hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandFailed))
		holdCancel()

		Print("Then: repair completes with error")
		h.assertError(shortWait)

		Print("When: create a new run")
		h.RunID = uuid.NewTime()

		h.Hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))

		Print("And: run repair")
		h.runRepair(ctx, allUnits(nil))

		Print("And: repair is done")
		h.assertDone(longWait)
		h.assertProgress(100, shortWait)
	})

	t.Run("repair temporary network outage", func(t *testing.T) {
		c := defaultConfig()

		h := newRepairTestHelper(t, session, c)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		holdCtx, holdCancel := context.WithCancel(context.Background())
		h.Hrt.SetInterceptor(holdRepairInterceptor(holdCtx, 2))
		h.runRepair(ctx, singleUnit(map[string]any{
			"small_table_threshold": repairAllSmallTableThreshold,
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: no network for 5s with 1s backoff")
		h.Hrt.SetInterceptor(dialErrorInterceptor())
		holdCancel()
		time.AfterFunc(2*h.Client.Config().Timeout, func() {
			h.Hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
		})

		Print("And: repair contains error")
		h.assertErrorContains("logs", longWait)

		p, err := h.service.GetProgress(ctx, h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			h.T.Fatal(err)
		}
		if p.Success == 0 || p.Error == 0 || p.SuccessPercentage+p.ErrorPercentage < 98 { // Leave some wiggle room for rounding errors
			h.T.Fatal("Expected to get ~100% progress with some successes and errors")
		}
	})

	t.Run("repair error fail fast", func(t *testing.T) {
		c := defaultConfig()

		h := newRepairTestHelper(t, session, c)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		holdCtx, holdCancel := context.WithCancel(context.Background())
		h.Hrt.SetInterceptor(holdRepairInterceptor(holdCtx, 1))
		h.runRepair(ctx, allUnits(map[string]any{
			"fail_fast":             true,
			"small_table_threshold": repairAllSmallTableThreshold,
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: no network for 5s with 1s backoff")
		h.Hrt.SetInterceptor(dialErrorInterceptor())
		holdCancel()
		time.AfterFunc(3*h.Client.Config().Timeout, func() {
			h.Hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))
		})

		Print("Then: repair completes with error")
		h.assertError(longWait)
	})

	t.Run("repair non existing keyspace", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair with non-existing keyspace")
		h.runRepair(ctx, singleUnit(map[string]any{
			"keyspace": []string{"non_existing_keyspace"},
		}))

		Print("Then: repair fails")
		h.assertError(shortWait)
	})

	t.Run("drop table during repair", func(t *testing.T) {
		const (
			testKeyspace = "test_repair_drop_table"
			testTable    = "test_table_0"
		)

		createKeyspace(t, clusterSession, testKeyspace, 3, 3)
		WriteData(t, clusterSession, testKeyspace, 1, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		holdCtx, holdCancel := context.WithCancel(context.Background())
		h.Hrt.SetInterceptor(holdRepairInterceptor(holdCtx, 2))
		h.runRepair(ctx, map[string]any{
			"keyspace":              []string{testKeyspace + "." + testTable},
			"dc":                    []string{"dc1", "dc2"},
			"intensity":             0,
			"parallel":              1,
			"small_table_threshold": repairAllSmallTableThreshold,
		})

		Print("When: 10% progress")
		h.assertRunning(shortWait)

		ExecStmt(t, clusterSession, fmt.Sprintf("DROP TABLE %s.%s", testKeyspace, testTable))
		h.Hrt.SetInterceptor(nil)
		holdCancel()

		Print("Then: repair is done")
		h.assertDone(shortWait)
	})

	t.Run("drop keyspace during repair", func(t *testing.T) {
		const (
			testKeyspace = "test_repair_drop_table"
			testTable    = "test_table_0"
		)

		createKeyspace(t, clusterSession, testKeyspace, 3, 3)
		WriteData(t, clusterSession, testKeyspace, 1, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		props := map[string]any{
			"keyspace":              []string{testKeyspace + "." + testTable},
			"dc":                    []string{"dc1", "dc2"},
			"intensity":             1,
			"small_table_threshold": repairAllSmallTableThreshold,
		}

		Print("When: run repair")
		holdCtx, holdCancel := context.WithCancel(context.Background())
		h.Hrt.SetInterceptor(holdRepairInterceptor(holdCtx, 2))
		h.runRepair(ctx, props)

		Print("When: 10% progress")
		h.assertRunning(longWait)

		Print("And: keyspace is dropped during repair")
		dropKeyspace(t, clusterSession, testKeyspace)
		h.Hrt.SetInterceptor(nil)
		holdCancel()

		Print("Then: repair is done")
		h.assertDone(longWait)
	})

	t.Run("kill repairs on task failure", func(t *testing.T) {
		const killPath = "/storage_service/force_terminate_repair"

		props := multipleUnits(map[string]any{
			"small_table_threshold": repairAllSmallTableThreshold,
		})

		t.Run("when task is cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			h := newRepairTestHelper(t, session, defaultConfig())
			Print("When: run repair")
			var killRepairCalled int32
			h.Hrt.SetInterceptor(countInterceptor(&killRepairCalled, killPath, "", holdRepairInterceptor(ctx, 2)))
			h.runRepair(ctx, props)

			Print("When: repair is running")
			h.assertRunning(longWait)

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
			props["fail_fast"] = true

			Print("When: run repair")
			holdCtx, holdCancel := context.WithCancel(context.Background())
			h.Hrt.SetInterceptor(holdRepairInterceptor(holdCtx, 2))
			h.runRepair(ctx, props)

			Print("When: repair is running")
			h.assertRunning(longWait)

			Print("When: Scylla returns failures")
			var killRepairCalled int32
			h.Hrt.SetInterceptor(countInterceptor(&killRepairCalled, killPath, "", repairInterceptor(scyllaclient.CommandFailed)))
			holdCancel()

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
			testKeyspace = "test_repair_optimize_table"
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

		// Check small_table_optimization support
		ni, err := h.Client.AnyNodeInfo(ctx)
		if err != nil {
			t.Fatal(err)
		}
		support, err := ni.SupportsRepairSmallTableOptimization()
		if err != nil {
			t.Fatal(err)
		}
		rd := scyllaclient.NewRingDescriber(ctx, h.Client)
		if rd.IsTabletKeyspace(testKeyspace) {
			support = false
		}

		var (
			repairCalled int32
			optUsed      = atomic.Bool{}
			inner        = countInterceptor(&repairCalled, repairPath, http.MethodPost, repairInterceptor(scyllaclient.CommandSuccessful))
		)
		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if repairEndpointRegexp.MatchString(req.URL.Path) && req.Method == http.MethodPost {
				opt, ok := req.URL.Query()["small_table_optimization"]
				if ok && opt[0] == "true" {
					optUsed.Store(true)
				}
			}
			return inner.RoundTrip(req)
		}))

		Print("When: run repair")
		h.runRepair(ctx, map[string]any{
			"keyspace":              []string{testKeyspace + "." + testTable},
			"dc":                    []string{"dc1", "dc2"},
			"intensity":             1,
			"parallel":              1,
			"small_table_threshold": 1 * 1024 * 1024 * 1024,
		})

		Print("Then: repair is done")
		h.assertDone(longWait)

		// small_table_optimization should be used when supported
		if support != optUsed.Load() {
			t.Fatalf("small_table_optimization support: %v, used in API call: %v", support, optUsed.Load())
		}

		Print("And: one repair task was issued")
		if repairCalled != 1 {
			t.Fatalf("Expected repair in one shot got %d", repairCalled)
		}

		p, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			h.T.Fatal(err)
		}

		if p.TokenRanges != p.Success {
			t.Fatalf("Expected full success, got %d/%d", p.Success, p.TokenRanges)
		}
	})

	t.Run("small table optimisation not on big table", func(t *testing.T) {
		const (
			testKeyspace = "test_repair_big_table"
			testTable    = "test_table_0"
			tableMBSize  = 5

			repairPath = "/storage_service/repair_async/"
		)

		Print("Given: big and fully replicated table")
		ExecStmt(t, clusterSession, "CREATE KEYSPACE "+testKeyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}")
		WriteData(t, clusterSession, testKeyspace, tableMBSize, testTable)
		FlushTable(t, h.Client, ManagedClusterHosts(), testKeyspace, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var (
			repairCalled int32
			optUsed      = atomic.Bool{}
			inner        = countInterceptor(&repairCalled, repairPath, http.MethodPost, repairInterceptor(scyllaclient.CommandSuccessful))
		)
		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if repairEndpointRegexp.MatchString(req.URL.Path) && req.Method == http.MethodPost {
				opt, ok := req.URL.Query()["small_table_optimization"]
				if ok && opt[0] == "true" {
					optUsed.Store(true)
				}
			}
			return inner.RoundTrip(req)
		}))

		Print("When: run repair")
		h.runRepair(ctx, map[string]any{
			"keyspace":              []string{testKeyspace + "." + testTable},
			"small_table_threshold": tableMBSize * 1024 * 1024, // Actual table size is always greater because of replication
		})

		Print("Then: repair is done")
		h.assertDone(longWait)

		// small_table_optimization shouldn't be used on big tables
		if optUsed.Load() {
			t.Fatal("small_table_optimisation was used")
		}

		Print("And: more than one repair jobs were scheduled")
		if repairCalled <= 1 {
			t.Fatalf("Expected more than 1 repair jobs, got %d", repairCalled)
		}

		p, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			h.T.Fatal(err)
		}

		if p.TokenRanges != p.Success {
			t.Fatalf("Expected full success, got %d/%d", p.Success, p.TokenRanges)
		}
	})

	t.Run("deprecated float intensity", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		h.runRepair(ctx, multipleUnits(map[string]any{
			"intensity": 0.5,
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		p, err := h.service.GetProgress(ctx, h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			t.Fatal(err)
		}
		if p.Intensity != 1 {
			t.Fatalf("Expected default intensity (1) when using float value, got: %v", p.Intensity)
		}

		Print("And: repair is done")
		h.assertDone(longWait)
	})

	t.Run("repair status context timeout", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: repair status is not responding in time")
		holdCtx, holdCancel := context.WithCancel(ctx)
		h.Hrt.SetInterceptor(holdRepairInterceptor(holdCtx, 0))

		Print("And: run repair")
		h.runRepair(ctx, allUnits(map[string]any{
			"fail_fast": true,
		}))

		time.AfterFunc(time.Second, holdCancel)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		h.Hrt.SetInterceptor(repairInterceptor(scyllaclient.CommandSuccessful))

		Print("Then: repair is done")
		h.assertDone(longWait)
	})

	t.Run("repair alternator table", func(t *testing.T) {
		const (
			testTable      = "Tab_le-With1.da_sh2-aNd.d33ot.-"
			testKeyspace   = "alternator_" + testTable
			alternatorPort = 8000
			repairPath     = "/storage_service/repair_async/"
		)

		Print("When: create alternator table with 1 row")
		CreateAlternatorTable(t, ManagedClusterHost(), alternatorPort, testTable)
		FillAlternatorTableWithOneRow(t, ManagedClusterHost(), alternatorPort, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		var repairCalled int32
		h.Hrt.SetInterceptor(countInterceptor(&repairCalled, repairPath, http.MethodPost, repairInterceptor(scyllaclient.CommandSuccessful)))
		h.runRepair(ctx, map[string]any{
			"keyspace": []string{testKeyspace + "." + testTable},
		})

		Print("Then: repair is done")
		h.assertDone(longWait)

		Print("And: jobs were scheduled")
		if repairCalled < 1 {
			t.Fatalf("Expected at least 1 repair job, got %d", repairCalled)
		}

		p, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			h.T.Fatal(err)
		}
		if p.TokenRanges != p.Success {
			t.Fatalf("Expected full success, got %d/%d", p.Success, p.TokenRanges)
		}
		if len(p.Tables) != 1 {
			t.Fatalf("Expected only 1 table to be repaired, got %d", len(p.Tables))
		}
		if p.Tables[0].Keyspace != testKeyspace || p.Tables[0].Table != testTable {
			t.Fatalf("Expected %q.%q to be repaired, got %q.%q", testKeyspace, testTable, p.Tables[0].Keyspace, p.Tables[0].Table)
		}
		if p.Tables[0].TokenRanges != p.Tables[0].Success {
			t.Fatalf("Expected table to be fully repaired, got %d/%d", p.Tables[0].TokenRanges, p.Tables[0].Success)
		}
	})
}

func TestServiceRepairErrorNodetoolRepairRunningIntegration(t *testing.T) {
	t.Skip("nodetool repair is executing too fast skipping until solution is found")
	session := CreateScyllaManagerDBSession(t)
	h := newRepairTestHelper(t, session, repair.DefaultConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clusterSession := CreateSessionAndDropAllKeyspaces(t, h.Client)
	const ks = "test_repair"

	createKeyspace(t, clusterSession, ks, 3, 3)
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table_0 (id int PRIMARY KEY)")
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair.test_table_1 (id int PRIMARY KEY)")
	defer dropKeyspace(t, clusterSession, ks)

	// Repair can be very fast with newer versions of Scylla.
	// This fills keyspace with data to prolong nodetool repair execution.
	WriteData(t, clusterSession, ks, 10, generateTableNames(100)...)

	Print("Given: repair is running on a host")
	done := make(chan struct{})
	go func() {
		time.AfterFunc(1*time.Second, func() {
			close(done)
		})
		ExecOnHost(ManagedClusterHost(), "nodetool repair -ful -seq")
	}()
	defer func() {
		if err := h.Client.KillAllRepairs(context.Background(), ManagedClusterHost()); err != nil {
			t.Fatal(err)
		}
	}()

	<-done
	Print("When: repair starts")
	h.runRepair(ctx, singleUnit(nil))

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
	session := CreateScyllaManagerDBSession(t)
	h := newRepairTestHelper(t, session, repair.DefaultConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clusterSession := CreateSessionAndDropAllKeyspaces(t, h.Client)

	ExecStmt(t, clusterSession, "CREATE KEYSPACE test_repair_0 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair_0.test_table_0 (id int PRIMARY KEY)")
	defer dropKeyspace(t, clusterSession, "test_repair_0")

	ExecStmt(t, clusterSession, "CREATE KEYSPACE test_repair_1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 0}")
	ExecStmt(t, clusterSession, "CREATE TABLE test_repair_1.test_table_0 (id int PRIMARY KEY)")
	defer dropKeyspace(t, clusterSession, "test_repair_1")

	props, err := json.Marshal(map[string]any{
		"keyspace": []string{"test_repair_0", "test_repair_1"},
		"dc":       []string{"dc2"},
	})
	if err != nil {
		t.Fatal(err)
	}

	target, err := h.service.GetTarget(ctx, h.ClusterID, props)
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
