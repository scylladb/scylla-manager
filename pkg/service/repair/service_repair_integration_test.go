// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package repair_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"slices"
	"sort"
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
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/dht"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testhelper"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
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

	clusterID := uuid.MustRandom()
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)

	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	c := newTestClient(t, hrt, log.NopLogger)
	s := newTestService(t, session, c, config, logger, clusterID)

	return &repairTestHelper{
		CommonTestHelper: &CommonTestHelper{
			Logger:    logger,
			Session:   session,
			Hrt:       hrt,
			Client:    c,
			ClusterID: clusterID,
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

	clusterID := uuid.MustRandom()
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	s := newTestServiceWithClusterSession(t, session, c, config, logger, clusterID)

	return &repairTestHelper{
		CommonTestHelper: &CommonTestHelper{
			Logger:    logger,
			Session:   session,
			Hrt:       hrt,
			Client:    c,
			ClusterID: clusterID,
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
			if errors.Is(err, util.ErrNotFound) {
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

func (h *repairTestHelper) stopNode(host string) {
	h.T.Helper()

	_, _, err := ExecOnHost(host, "supervisorctl stop scylla")
	if err != nil {
		h.T.Fatal(err)
	}
}

func (h *repairTestHelper) startNode(host string, ni *scyllaclient.NodeInfo) {
	h.T.Helper()

	_, _, err := ExecOnHost(host, "supervisorctl start scylla")
	if err != nil {
		h.T.Fatal(err)
	}

	cfg := cqlping.Config{
		Addr:    ni.CQLAddr(host, false),
		Timeout: time.Minute,
	}
	if testconfig.IsSSLEnabled() {
		sslOpts := testconfig.CQLSSLOptions()
		tlsConfig, err := testconfig.TLSConfig(sslOpts)
		if err != nil {
			h.T.Fatalf("setup tls config: %v", err)
		}
		cfg.TLSConfig = tlsConfig
	}

	cond := func() bool {
		if _, err = cqlping.QueryPing(context.Background(), cfg, TestDBUsername(), TestDBPassword()); err != nil {
			return false
		}
		status, err := h.Client.Status(context.Background())
		if err != nil {
			return false
		}
		return len(status.Live()) == len(ManagedClusterHosts())
	}

	WaitCond(h.T, cond, time.Second, shortWait)
	time.Sleep(time.Second)
}

func percentComplete(p repair.Progress) (int, int) {
	if p.TokenRanges == 0 {
		return 0, 0
	}
	return int(p.Success * 100 / p.TokenRanges), int(p.Error * 100 / p.TokenRanges)
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

func newTestService(t *testing.T, session gocqlx.Session, client *scyllaclient.Client, c repair.Config, logger log.Logger, clusterID uuid.UUID) *repair.Service {
	t.Helper()

	s, err := repair.NewService(
		session,
		c,
		metrics.NewRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return gocqlx.Session{}, errors.New("not implemented")
		},
		NewTestConfigCacheSvc(t, clusterID, client.Config().Hosts),
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s
}

func newTestServiceWithClusterSession(t *testing.T, session gocqlx.Session, client *scyllaclient.Client, c repair.Config, logger log.Logger, clusterID uuid.UUID) *repair.Service {
	t.Helper()

	s, err := repair.NewService(
		session,
		c,
		metrics.NewRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return CreateSession(t, client), nil
		},
		NewTestConfigCacheSvc(t, clusterID, client.Config().Hosts),
		logger.Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	return s
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
				cmpopts.IgnoreSliceElements(func(t string) bool { return t == "dicts" }),
				cmpopts.IgnoreFields(repair.Target{}, "Host")); diff != "" {
				t.Fatal(diff)
			}
			if golden.Host != v.Host {
				t.Fatalf("Expected host: %s, got: %s", golden.Host, v.Host)
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
	createVnodeKeyspace(t, clusterSession, ks1, 1, 1)
	createDefaultKeyspace(t, clusterSession, ks2, 2, 2)
	createDefaultKeyspace(t, clusterSession, ks3, 3, 3)
	WriteData(t, clusterSession, ks1, 5, t1, t2)
	WriteData(t, clusterSession, ks2, 5, t1, t2)
	WriteData(t, clusterSession, ks3, 5, t1, t2)

	t.Run("repair schedules only one job per host at any given time", func(t *testing.T) {
		ctx := context.Background()

		props := map[string]any{
			"fail_fast": true,
		}

		// The amount of currently executed repair jobs on host
		jobsPerHost := make(map[netip.Addr]int)
		muJPH := sync.Mutex{}

		// Set of hosts used for given repair job
		hostsInJob := make(map[string][]netip.Addr)
		muHIJ := sync.Mutex{}

		cnt := atomic.Int64{}

		// Repair request
		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if r, ok := parseRepairReq(t, req); ok {
				muJPH.Lock()
				defer muJPH.Unlock()

				for _, host := range r.replicaSet {
					jobsPerHost[host]++
					if jobsPerHost[host] > maxJobsOnHost {
						cnt.Add(1)
						return nil, nil
					}
				}
			}
			return nil, nil
		}))

		h.Hrt.SetRespInterceptor(func(resp *http.Response, err error) (*http.Response, error) {
			if resp == nil {
				return nil, nil
			}

			if r, ok := parseRepairResp(t, resp); ok {
				muHIJ.Lock()
				hostsInJob[r.host.String()+r.id] = r.replicaSet
				muHIJ.Unlock()
			}

			if r, ok := parseRepairStatusResp(t, resp); ok {
				if r.status == repairStatusDone || r.status == repairStatusFailed {
					muHIJ.Lock()
					hosts := hostsInJob[r.host.String()+r.id]
					muHIJ.Unlock()

					muJPH.Lock()
					defer muJPH.Unlock()

					for _, host := range hosts {
						jobsPerHost[host]--
					}
				}
			}
			return nil, nil
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
	hrt.SetInterceptor(repairMockInterceptor(t, repairStatusDone))
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
	)

	// Create keyspaces. Low RF improves repair parallelism.
	createVnodeKeyspace(t, clusterSession, ks1, 1, 1)
	createDefaultKeyspace(t, clusterSession, ks2, 1, 1)
	createDefaultKeyspace(t, clusterSession, ks3, 2, 1)

	// Create and fill tables
	WriteData(t, clusterSession, ks1, 1, t1)
	WriteData(t, clusterSession, ks1, 2, t2)

	WriteData(t, clusterSession, ks2, 1, t1)
	WriteData(t, clusterSession, ks2, 2, t2)
	WriteData(t, clusterSession, ks2, 3, t3)

	WriteData(t, clusterSession, ks3, 20, t1)

	// Create views
	rd := scyllaclient.NewRingDescriber(context.Background(), h.Client)
	var ks1Views []string
	if !rd.IsTabletKeyspace(ks1) {
		mv1 := "zz_test_mv_1"
		CreateMaterializedView(t, clusterSession, ks1, t1, mv1)
		ks1Views = append(ks1Views, mv1)
	}
	var ks2Views []string
	if !rd.IsTabletKeyspace(ks2) {
		si1 := "aa_test_si_1"
		mv1 := "zz_test_mv_1"
		mv2 := "hh_test_mv_2"
		CreateSecondaryIndex(t, clusterSession, ks2, t1, si1)
		CreateMaterializedView(t, clusterSession, ks2, t2, mv1)
		CreateMaterializedView(t, clusterSession, ks2, t3, mv2)
		ks2Views = append(ks2Views, si1+"_index", mv1, mv2)
	}

	// Flush tables for correct memory calculations
	FlushTable(t, c, ManagedClusterHosts(), ks1, t1)
	FlushTable(t, c, ManagedClusterHosts(), ks1, t2)
	for _, v := range ks1Views {
		FlushTable(t, c, ManagedClusterHosts(), ks1, v)
	}

	FlushTable(t, c, ManagedClusterHosts(), ks2, t1)
	FlushTable(t, c, ManagedClusterHosts(), ks2, t2)
	FlushTable(t, c, ManagedClusterHosts(), ks2, t3)
	for _, v := range ks2Views {
		FlushTable(t, c, ManagedClusterHosts(), ks2, v)
	}

	FlushTable(t, c, ManagedClusterHosts(), ks3, t1)

	expectedRepairOrder := []string{
		"system_auth.*",
		"system_replicated_keys.*",
		"system_distributed.*",
		"system_distributed_everywhere.*",
		"system_traces.*",
	}

	expectedRepairOrder = append(expectedRepairOrder, ks1+"."+t1, ks1+"."+t2)
	for _, v := range ks1Views {
		expectedRepairOrder = append(expectedRepairOrder, ks1+"."+v)
	}

	expectedRepairOrder = append(expectedRepairOrder, ks2+"."+t1, ks2+"."+t2, ks2+"."+t3)
	for _, v := range ks2Views {
		expectedRepairOrder = append(expectedRepairOrder, ks2+"."+v)
	}

	expectedRepairOrder = append(expectedRepairOrder, ks3+"."+t1)

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
		if r, ok := parseRepairReq(t, req); ok {
			// Update actual repair order on both repair start and end
			muARO.Lock()
			if len(actualRepairOrder) == 0 || actualRepairOrder[len(actualRepairOrder)-1] != r.fullTable() {
				actualRepairOrder = append(actualRepairOrder, r.fullTable())
			}
			muARO.Unlock()
		}
		return nil, nil
	}))

	h.Hrt.SetRespInterceptor(func(resp *http.Response, err error) (*http.Response, error) {
		if resp == nil {
			return nil, nil
		}

		if r, ok := parseRepairResp(t, resp); ok {
			// Register what table is being repaired
			muJT.Lock()
			jobTable[r.host.String()+r.id] = r.fullTable()
			muJT.Unlock()
		}

		if r, ok := parseRepairStatusResp(t, resp); ok {
			if r.status == repairStatusDone || r.status == repairStatusFailed {
				// Add host prefix as IDs are unique only for a given host
				jobID := r.host.String() + r.id
				muJT.Lock()
				fullTable := jobTable[jobID]
				muJT.Unlock()

				if fullTable == "" {
					t.Logf("This is strange %s", jobID)
					return nil, nil
				}

				// Update actual repair order on both repair start and end
				muARO.Lock()
				if len(actualRepairOrder) == 0 || actualRepairOrder[len(actualRepairOrder)-1] != fullTable {
					actualRepairOrder = append(actualRepairOrder, fullTable)
				}
				muARO.Unlock()
			}
		}
		return nil, nil
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
	hrt.SetInterceptor(repairMockInterceptor(t, repairStatusDone))
	c := newTestClient(t, hrt, log.NopLogger)
	ctx := context.Background()

	session := CreateScyllaManagerDBSession(t)
	clusterSession := CreateSessionAndDropAllKeyspaces(t, c)
	cfg := repair.DefaultConfig()
	// Make sure that all repairs finish within the graceful stop timeout,
	// so that we avoid situations that interceptor marked range as done,
	// but SM won't save it in its DB.
	cfg.GracefulStopTimeout = time.Minute
	h := newRepairWithClusterSessionTestHelper(t, session, hrt, c, cfg)

	const (
		ks1 = "test_repair_1"
		ks2 = "test_repair_2"
		ks3 = "test_repair_3"
		t1  = "test_table_1"
		t2  = "test_table_2"
		t3  = "test_table_3"
	)

	// Create keyspaces. Low RF increases repair parallelism.
	createVnodeKeyspace(t, clusterSession, ks1, 2, 1)
	createVnodeKeyspace(t, clusterSession, ks2, 1, 1)

	if tabletRepairSupport(t) {
		createVnodeKeyspace(t, clusterSession, ks3, 1, 1)
	} else {
		createDefaultKeyspace(t, clusterSession, ks3, 1, 1)
	}

	// Create and fill tables
	WriteData(t, clusterSession, ks1, 1, t1)
	WriteData(t, clusterSession, ks1, 2, t2)

	WriteData(t, clusterSession, ks2, 1, t1)
	WriteData(t, clusterSession, ks2, 2, t2)
	WriteData(t, clusterSession, ks2, 3, t3)

	WriteData(t, clusterSession, ks3, 5, t1)

	// It's not possible to create views on tablet keyspaces
	rd := scyllaclient.NewRingDescriber(context.Background(), h.Client)
	if !rd.IsTabletKeyspace(ks1) {
		CreateMaterializedView(t, clusterSession, ks1, t1, "test_mv_1")
	}
	if !rd.IsTabletKeyspace(ks2) {
		CreateSecondaryIndex(t, clusterSession, ks2, t1, "test_si_1")
		CreateMaterializedView(t, clusterSession, ks2, t2, "test_mv_1")
		CreateMaterializedView(t, clusterSession, ks2, t3, "test_mv_2")
	}

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

	// Tools for performing a repair with 4 pauses
	var (
		reqCnt          = atomic.Int64{}
		rspCnt          = atomic.Int64{}
		stopErrInject   = atomic.Bool{}
		stop1Ctx, stop1 = context.WithCancel(ctx)
		stop2Ctx, stop2 = context.WithCancel(ctx)
		stop3Ctx, stop3 = context.WithCancel(ctx)
		stop4Ctx, stop4 = context.WithCancel(ctx)
		stopCnt1        = 50
		stopCnt2        = 75
		stopCnt3        = 100
		stopCnt4        = 125
	)

	// Repair request
	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if isRepairReq(req) {
			switch int(reqCnt.Add(1)) {
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

	h.Hrt.SetRespInterceptor(func(resp *http.Response, err error) (*http.Response, error) {
		if resp == nil {
			return nil, nil
		}

		if r, ok := parseRepairResp(t, resp); ok {
			// Register what table is being repaired
			muJS.Lock()
			jobSpec[r.host.String()+r.id] = TableRange{
				FullTable: r.fullTable(),
				Ranges:    r.ranges,
			}
			muJS.Unlock()
		}

		if r, ok := parseRepairStatusResp(t, resp); ok {
			// Inject errors on all runs except the last one.
			// This helps to test repair error resilience.
			if !stopErrInject.Load() && rspCnt.Add(1)%20 == 0 {
				resp.Body, _ = mockRepairStatusRespBody(t, resp.Request, repairStatusFailed)
				return resp, nil
			}

			if r.status == repairStatusDone {
				muJS.Lock()
				defer muJS.Unlock()

				k := r.host.String() + r.id
				if tr, ok := jobSpec[k]; ok {
					// Make sure that retries don't result in counting redundant ranges
					delete(jobSpec, k)
					// Register done ranges
					muDR.Lock()
					dr := doneRanges[tr.FullTable]
					dr = append(dr, tr.Ranges...)
					doneRanges[tr.FullTable] = dr
					muDR.Unlock()
				}
			}
		}
		return nil, nil
	})

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

	clearTabletRanges := func(doneRanges map[string][]scyllaclient.TokenRange, ringDescriber scyllaclient.RingDescriber) {
		var clearKeys []string
		for tab, dr := range doneRanges {
			_, err := validate(tab, dr)
			if err != nil && ringDescriber.IsTabletKeyspace(strings.Split(tab, ".")[0]) {
				clearKeys = append(clearKeys, tab)
			}
		}
		for _, k := range clearKeys {
			delete(doneRanges, k)
		}
	}

	Print("When: run first repair with context cancel")
	if err := h.runRegularRepair(stop1Ctx, props); err == nil {
		t.Fatal("Repair failed without error")
	}

	// Tablet tables don't support resuming repair, so in order to check if repair
	// started from scratch, we need to remove all repaired ranges up to this point.
	ringDescriber := scyllaclient.NewRingDescriber(ctx, h.Client)

	Print("When: run second repair with context cancel")
	h.RunID = uuid.NewTime()
	clearTabletRanges(doneRanges, ringDescriber)
	if err := h.runRegularRepair(stop2Ctx, props); err == nil {
		t.Fatal("Repair failed without error")
	}

	Print("When: run third repair with context cancel")
	clearTabletRanges(doneRanges, ringDescriber)
	h.RunID = uuid.NewTime()
	if err := h.runRegularRepair(stop3Ctx, props); err == nil {
		t.Fatal("Repair failed without error")
	}

	Print("When: run fourth repair with context cancel")
	clearTabletRanges(doneRanges, ringDescriber)
	h.RunID = uuid.NewTime()
	if err := h.runRegularRepair(stop4Ctx, props); err == nil {
		t.Fatal("Repair failed without error")
	}

	Print("When: run fifth repair till it finishes")
	clearTabletRanges(doneRanges, ringDescriber)
	h.RunID = uuid.NewTime()
	stopErrInject.Store(true)
	if err := h.runRegularRepair(ctx, props); err != nil {
		t.Fatalf("Repair failed: %s", err)
	}

	Print("When: validate all, continuous ranges")
	for tab, tr := range doneRanges {
		r, err := validate(tab, tr)
		if err != nil {
			t.Fatal(err)
		}
		if r > 0 {
			t.Fatalf("Expected no redundant ranges in %s, got %d (out of total %d)", tab, r, len(tr))
		}
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

	const testKs = "test_repair"
	createDefaultKeyspace(t, clusterSession, testKs, 2, 2)
	WriteData(t, clusterSession, testKs, 1, "test_table_0", "test_table_1")
	defer dropKeyspace(t, clusterSession, testKs)

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

		var ignored = ManagedClusterHost()
		h.stopNode(ignored)
		defer h.startNode(ignored, globalNodeInfo)

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

		if tabletRepairSupport(t) {
			t.Skip("This behavior is tested by test 'repair tablet API filtering'")
		}

		host := netip.MustParseAddr(h.GetHostsFromDC("dc1")[0])
		h.Hrt.SetInterceptor(repairReqAssertHostInterceptor(t, host))

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
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, ctx, 1))
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
		i, running := repairRunningInterceptor()
		h.Hrt.SetInterceptor(i)
		h.runRepair(ctx, multipleUnits(map[string]any{
			"small_table_threshold": repairAllSmallTableThreshold,
		}))

		Print("Then: repair is running")
		chanClosedWithin(t, running, shortWait)

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
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, ctx, 1))
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
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, ctx, 1))
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
		props := multipleUnits(map[string]any{
			"parallel":              propParallel,
			"intensity":             propIntensity,
			"small_table_threshold": -1,
		})
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, ctx, 1))
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
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, holdCtx, 1))
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
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, holdCtx, 1))
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

		props := multipleUnits(map[string]any{
			"continue":              false,
			"small_table_threshold": repairAllSmallTableThreshold,
		})

		Print("When: run repair")
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, ctx, 1))
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
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, ctx, 0))
		h.runRepair(ctx, props)

		WaitCond(h.T, func() bool {
			_, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, h.RunID)
			if err != nil {
				if errors.Is(err, util.ErrNotFound) {
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
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, ctx, 1))
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

		rd := scyllaclient.NewRingDescriber(ctx, h.Client)
		if rd.IsTabletKeyspace(testKs) {
			t.Skip("Test expects vnode keyspace: tablet tables are repaired with a single API call or they don't support resume")
		}

		Print("When: run repair")
		holdCtx, holdCancel := context.WithCancel(ctx)
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, holdCtx, 1))
		h.runRepair(ctx, allUnits(map[string]any{
			"fail_fast":             true,
			"small_table_threshold": repairAllSmallTableThreshold,
		}))

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		Print("And: errors occur")
		h.Hrt.SetInterceptor(repairMockInterceptor(t, repairStatusFailed))
		holdCancel()

		Print("Then: repair completes with error")
		h.assertError(shortWait)

		Print("When: create a new run")
		h.RunID = uuid.NewTime()

		h.Hrt.SetInterceptor(repairMockInterceptor(t, repairStatusDone))

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
		i, running := repairRunningInterceptor()
		h.Hrt.SetInterceptor(i)
		h.runRepair(ctx, multipleUnits(map[string]any{
			"small_table_threshold": repairAllSmallTableThreshold,
		}))

		Print("Then: repair is running")
		chanClosedWithin(t, running, shortWait)

		Print("And: no network for 5s with 1s backoff")
		h.Hrt.SetInterceptor(dialErrorInterceptor())
		time.AfterFunc(2*h.Client.Config().Timeout, func() {
			h.Hrt.SetInterceptor(repairMockInterceptor(t, repairStatusDone))
		})

		Print("And: repair contains error")
		h.assertError(longWait)

		p, err := h.service.GetProgress(ctx, h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			h.T.Fatal(err)
		}
		if p.Success == 0 || p.Error == 0 || p.Success+p.Error != p.TokenRanges { // Leave some wiggle room for rounding errors
			h.T.Fatalf("Expected to get full progress with some successes and errors, got %v", p)
		}
	})

	t.Run("repair error fail fast", func(t *testing.T) {
		c := defaultConfig()

		h := newRepairTestHelper(t, session, c)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		// Since RF={'dc1': 2, 'dc2': 2}, max parallel = 1,
		// so the repair with --fail-fast should finish right
		// after the first error.
		var callCnt int32
		h.Hrt.SetInterceptor(combineInterceptors(
			countInterceptor(&callCnt, isRepairReq),
			repairMockInterceptor(t, repairStatusFailed),
		))
		h.runRepair(ctx, multipleUnits(map[string]any{
			"fail_fast":             true,
			"small_table_threshold": repairAllSmallTableThreshold,
		}))

		Print("Then: repair completes with error")
		h.assertError(longWait)

		Print("Then: repair finished after the first error")
		if callCnt != 1 {
			t.Fatalf("Expected repair to finish after the first error, got: %d", callCnt)
		}
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

		createDefaultKeyspace(t, clusterSession, testKeyspace, 3, 3)
		WriteData(t, clusterSession, testKeyspace, 1, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		// It's difficult to ensure that table deletion happened while
		// the table was repaired, so we just make the best effort to hit it.
		i, running := repairRunningInterceptor()
		h.Hrt.SetInterceptor(i)
		h.runRepair(ctx, map[string]any{
			"keyspace":              []string{testKeyspace + "." + testTable},
			"dc":                    []string{"dc1", "dc2"},
			"intensity":             1,
			"parallel":              1,
			"small_table_threshold": repairAllSmallTableThreshold,
		})

		Print("When: repair is running")
		chanClosedWithin(t, running, shortWait)

		ExecStmt(t, clusterSession, fmt.Sprintf("DROP TABLE %s.%s", testKeyspace, testTable))

		Print("Then: repair is done")
		h.assertDone(shortWait)
	})

	t.Run("drop keyspace during repair", func(t *testing.T) {
		const (
			testKeyspace = "test_repair_drop_table"
			testTable    = "test_table_0"
		)

		createDefaultKeyspace(t, clusterSession, testKeyspace, 3, 3)
		WriteData(t, clusterSession, testKeyspace, 1, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		props := map[string]any{
			"keyspace":              []string{testKeyspace + "." + testTable},
			"dc":                    []string{"dc1", "dc2"},
			"intensity":             1,
			"parallel":              1,
			"small_table_threshold": repairAllSmallTableThreshold,
		}

		Print("When: run repair")
		// It's difficult to ensure that table deletion happened while
		// the table was repaired, so we just make the best effort to hit it.
		i, running := repairRunningInterceptor()
		h.Hrt.SetInterceptor(i)
		h.runRepair(ctx, props)

		Print("When: repair is running")
		chanClosedWithin(t, running, shortWait)

		Print("And: keyspace is dropped during repair")
		dropKeyspace(t, clusterSession, testKeyspace)

		Print("Then: repair is done")
		h.assertDone(longWait)
	})

	t.Run("kill repairs on task failure", func(t *testing.T) {
		props := multipleUnits(map[string]any{
			"small_table_threshold": repairAllSmallTableThreshold,
		})

		t.Run("when task is cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			h := newRepairTestHelper(t, session, defaultConfig())
			Print("When: run repair")
			var killRepairCalled int32
			i, running := repairRunningInterceptor()
			h.Hrt.SetInterceptor(combineInterceptors(
				countInterceptor(&killRepairCalled, isForceTerminateRepairReq),
				i,
			))
			h.runRepair(ctx, props)

			Print("When: repair is running")
			chanClosedWithin(t, running, shortWait)

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
			var killRepairCalled int32
			h.Hrt.SetInterceptor(combineInterceptors(
				countInterceptor(&killRepairCalled, isForceTerminateRepairReq),
				repairMockInterceptor(t, repairStatusFailed),
			))
			h.runRepair(ctx, props)

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
		)

		Print("Given: small and fully replicated table")
		// Small table optimisation is not supported for tablet keyspaces
		createVnodeKeyspace(t, clusterSession, testKeyspace, 3, 0)
		WriteData(t, clusterSession, testKeyspace, 1, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Check small_table_optimization support
		support, err := globalNodeInfo.SupportsRepairSmallTableOptimization()
		if err != nil {
			t.Fatal(err)
		}

		var (
			repairCalled int32
			optUsed      = atomic.Bool{}
		)
		h.Hrt.SetInterceptor(combineInterceptors(
			httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if r, ok := parseRepairReq(t, req); ok {
					if r.smallTableOptimization {
						optUsed.Store(true)
					}
				}
				return nil, nil
			}),
			countInterceptor(&repairCalled, isRepairReq),
			repairMockInterceptor(t, repairStatusDone),
		))

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
		)

		Print("Given: big and fully replicated table")
		// Small table optimisation is not supported for tablet keyspaces
		createVnodeKeyspace(t, clusterSession, testKeyspace, 3, 0)
		WriteData(t, clusterSession, testKeyspace, tableMBSize, testTable)
		FlushTable(t, h.Client, ManagedClusterHosts(), testKeyspace, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var (
			optUsed         = atomic.Bool{}
			mergedRangeUsed = atomic.Bool{}
		)
		h.Hrt.SetInterceptor(combineInterceptors(
			httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if r, ok := parseRepairReq(t, req); ok {
					if r.smallTableOptimization {
						optUsed.Store(true)
					}
					if slices.Equal(r.ranges, []scyllaclient.TokenRange{{StartToken: dht.Murmur3MinToken, EndToken: dht.Murmur3MaxToken}}) {
						mergedRangeUsed.Store(true)
					}
				}
				return nil, nil
			}),
			repairMockInterceptor(t, repairStatusDone),
		))

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
		// merged ranges optimization shouldn't be used on big tables
		if mergedRangeUsed.Load() {
			t.Fatal("merged ranges optimization was used")
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
		h.Hrt.SetInterceptor(repairMockAndBlockInterceptor(t, holdCtx, 0))

		Print("And: run repair")
		h.runRepair(ctx, allUnits(map[string]any{
			"fail_fast": true,
		}))

		time.AfterFunc(time.Second, holdCancel)

		Print("Then: repair is running")
		h.assertRunning(shortWait)

		h.Hrt.SetInterceptor(repairMockInterceptor(t, repairStatusDone))

		Print("Then: repair is done")
		h.assertDone(longWait)
	})

	t.Run("repair alternator table", func(t *testing.T) {
		const (
			testTable      = "Tab_le-With1.da_sh2-aNd.d33ot.-"
			testKeyspace   = "alternator_" + testTable
			alternatorPort = 8000
		)

		Print("When: create alternator table with 1 row")

		accessKeyID, secretAccessKey := CreateAlternatorUser(t, clusterSession, "")
		svc := CreateDynamoDBService(t, ManagedClusterHost(), alternatorPort, accessKeyID, secretAccessKey)
		CreateAlternatorTable(t, svc, testTable)
		FillAlternatorTableWithOneRow(t, svc, testTable)
		defer dropKeyspace(t, clusterSession, testKeyspace)

		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		Print("When: run repair")
		var repairCalled int32
		h.Hrt.SetInterceptor(combineInterceptors(
			countInterceptor(&repairCalled, isRepairReq),
			repairMockInterceptor(t, repairStatusDone),
		))
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

	t.Run("ranges batching", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const (
			ks               = "test_repair_ranges_batching"
			desiredIntensity = 1
		)

		Print("When: prepare keyspace with 9 replica sets")
		// Ranges batching is mainly used for speeding up vnode keyspace repair
		createVnodeKeyspace(t, clusterSession, ks, 2, 2)
		WriteData(t, clusterSession, ks, 1, "test_table_0")
		defer dropKeyspace(t, clusterSession, ks)

		cnt := atomic.Int64{}
		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if r, ok := parseRepairReq(t, req); ok {
				cnt.Add(1)
				if r.rangesParallelism != desiredIntensity {
					t.Errorf("Expected ranges_parallelism=%d, got %d", desiredIntensity, r.rangesParallelism)
				}
				if r.rangesParallelism == len(r.ranges) {
					t.Error("Ranges should be batched")
				}
			}
			return nil, nil
		}))

		Print("When: run repair")
		h.runRepair(ctx, map[string]any{
			"keyspace":              []string{ks},
			"fail_fast":             true,
			"small_table_threshold": repairAllSmallTableThreshold,
		})

		Print("Then: repair is done")
		h.assertDone(longWait)

		Print("Then: assuming that batching=100% and table with 9 replica sets, validate that 9 jobs were sent")
		if v := cnt.Load(); v != 9 {
			t.Errorf("Expected 9 jobs to be sent (one per replica set), got %d", v)
		}
	})

	t.Run("ranges batching fallback", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.GracefulStopTimeout = 1
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const (
			ks               = "test_repair_ranges_batching_fallback"
			desiredIntensity = 1
		)

		Print("When: prepare keyspace with 9 replica sets")
		// Ranges batching is mainly used for speeding up vnode keyspace repair
		createVnodeKeyspace(t, clusterSession, ks, 2, 2)
		WriteData(t, clusterSession, ks, 1, "test_table_0")
		defer dropKeyspace(t, clusterSession, ks)

		batching := atomic.Bool{}
		fail := atomic.Bool{}
		stop := atomic.Bool{}
		pauseCtx, pauseCancel := context.WithCancel(ctx)
		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if r, ok := parseRepairReq(t, req); ok {
				if r.rangesParallelism != desiredIntensity {
					t.Errorf("Expected ranges_parallelism=%d, got %d", desiredIntensity, r.rangesParallelism)
				}

				rangesCnt := len(r.ranges)
				// Watch out for split range
				if slices.ContainsFunc(r.ranges, func(tr scyllaclient.TokenRange) bool { return tr.StartToken == dht.Murmur3MinToken }) &&
					slices.ContainsFunc(r.ranges, func(tr scyllaclient.TokenRange) bool { return tr.EndToken == dht.Murmur3MaxToken }) {
					rangesCnt--
				}

				if batching.Load() {
					if r.rangesParallelism == rangesCnt {
						t.Error("Ranges should be batched")
					}
				} else {
					if r.rangesParallelism != rangesCnt {
						t.Error("Ranges shouldn't be batched")
					}
				}

				resp := httpx.MakeResponse(req, 200)
				resp.Body, _ = mockRepairRespBody(t, req)
				return resp, nil
			}

			if isRepairStatusReq(req) {
				resp := httpx.MakeResponse(req, 200)
				if stop.CompareAndSwap(true, false) {
					pauseCancel()
				}
				if fail.Load() {
					resp.Body, _ = mockRepairStatusRespBody(t, req, repairStatusFailed)
					return resp, nil
				}
				resp.Body, _ = mockRepairStatusRespBody(t, req, repairStatusDone)
				return resp, nil
			}

			return nil, nil
		}))

		props := map[string]any{
			"keyspace":              []string{ks},
			"intensity":             desiredIntensity,
			"small_table_threshold": repairAllSmallTableThreshold,
		}

		batching.Store(true)
		fail.Store(true)
		stop.Store(false)

		Print("When: run batched/failing repair")
		h.runRepair(ctx, props)

		Print("Then: batched/failing repair finished with error")
		h.assertError(longWait)

		// As repair service tests don't populate scheduler_task_run,
		// it has to be done manually.
		q := qb.Insert(table.SchedulerTaskRun.Name()).Columns(
			"cluster_id",
			"type",
			"task_id",
			"id",
			"status",
		).Query(session)
		defer q.Release()

		err := q.Bind(
			h.ClusterID,
			"repair",
			h.TaskID,
			h.RunID,
			scheduler.StatusError,
		).Exec()
		if err != nil {
			t.Fatal(err)
		}

		batching.Store(false)
		fail.Store(false)
		stop.Store(true)

		h.RunID = uuid.NewTime()
		Print("When: run not-batched/paused repair")
		h.runRepair(pauseCtx, props)

		Print("Then: not-batched/paused repair finished with error")
		h.assertError(longWait)
		if !errors.Is(h.result, context.Canceled) {
			t.Fatalf("Expected context cancel error, got %s", h.result)
		}

		err = q.Bind(
			h.ClusterID,
			"repair",
			h.TaskID,
			h.RunID,
			scheduler.StatusStopped,
		).Exec()
		if err != nil {
			t.Fatal(err)
		}

		stop.Store(false)

		h.RunID = uuid.NewTime()
		Print("When: run non-batched repair")
		h.runRepair(ctx, props)

		Print("Then: not-batched repair is done")
		h.assertDone(longWait)
	})

	t.Run("repair tablet API filtering", func(t *testing.T) {
		if !tabletRepairSupport(t) {
			t.Skip("Test expects tablet repair API to be exposed")
		}

		const (
			tabletMultiDCKs  = "tablet_api_filtering_multi_dc_tablet_ks"
			tabletSingleDCKs = "tablet_api_filtering_single_dc_tablet_ks"
			vnodeKs          = "tablet_api_filtering_vnode_ks"
		)
		createTabletKeyspace(t, clusterSession, tabletMultiDCKs, 2, 2)
		createTabletKeyspace(t, clusterSession, tabletSingleDCKs, 2, 0)
		createVnodeKeyspace(t, clusterSession, vnodeKs, 2, 2)
		WriteData(t, clusterSession, tabletMultiDCKs, 1, "tab")
		WriteData(t, clusterSession, tabletSingleDCKs, 1, "tab")
		WriteData(t, clusterSession, vnodeKs, 1, "tab")

		t.Run("Repairing tablet table with --host should fail at generating target", func(t *testing.T) {
			h := newRepairTestHelper(t, session, defaultConfig())
			_, err := h.generateTarget(map[string]any{
				"keyspace": []string{tabletMultiDCKs, tabletSingleDCKs, vnodeKs},
				"host":     ManagedClusterHost(),
			})
			if err == nil {
				t.Fatal("Expected err, got nil")
			}
		})

		t.Run("Repairing filtered out tablet table with --host should succeed", func(t *testing.T) {
			h := newRepairTestHelper(t, session, defaultConfig())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			host := netip.MustParseAddr(h.GetHostsFromDC("dc2")[0])
			h.Hrt.SetInterceptor(repairReqAssertHostInterceptor(t, host))

			h.runRepair(ctx, map[string]any{
				"dc":       []string{"dc2"},
				"keyspace": []string{tabletSingleDCKs, vnodeKs},
				"host":     host.String(),
			})
			h.assertDone(shortWait)
		})

		t.Run("Repairing table with node down should fail at generating target", func(t *testing.T) {
			h := newRepairTestHelper(t, session, defaultConfig())

			down := ManagedClusterHost()
			h.stopNode(down)
			defer h.startNode(down, globalNodeInfo)

			_, err := h.generateTarget(map[string]any{
				"keyspace": []string{tabletMultiDCKs},
			})
			if err == nil {
				t.Fatal("Expected err, got nil")
			}
			_, err = h.generateTarget(map[string]any{
				"keyspace": []string{vnodeKs},
			})
			if err == nil {
				t.Fatal("Expected err, got nil")
			}
		})

		t.Run("Repairing table with node down from filtered out DC should succeed", func(t *testing.T) {
			h := newRepairTestHelper(t, session, defaultConfig())

			down := h.GetHostsFromDC("dc2")[0]
			h.stopNode(down)
			defer h.startNode(down, globalNodeInfo)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h.runRepair(ctx, map[string]any{
				"keyspace": []string{tabletMultiDCKs, vnodeKs},
				"dc":       []string{"dc1"},
			})
			h.assertDone(2 * longWait)
		})
	})

	t.Run("repair API", func(t *testing.T) {
		h := newRepairTestHelper(t, session, defaultConfig())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const (
			tabletKS = "test_repair_single_dc_tablet_ks"
			vnodeKs  = "test_repair_vnode_ks"
			t1       = "test_table_1"
			t2       = "test_table_2"
		)
		testCases := []struct {
			ks            string
			tab           []string
			api           string
			singleCall    bool
			loadBalancing bool
		}{
			{
				ks:            tabletKS,
				tab:           []string{t1, t2},
				api:           tabletRepairEndpoint,
				singleCall:    true,
				loadBalancing: true,
			},
			{
				ks:            vnodeKs,
				tab:           []string{t1, t2},
				api:           repairAsyncEndpoint,
				singleCall:    false,
				loadBalancing: true,
			},
		}

		if !tabletRepairSupport(t) {
			// For this Scylla version tablet tables
			// are still repaired with the old repair API.
			testCases[0].api = repairAsyncEndpoint
			testCases[0].singleCall = false
			testCases[0].loadBalancing = false
		}

		Print("When: prepare keyspaces")
		createTabletKeyspace(t, clusterSession, tabletKS, 2, 2)
		WriteData(t, clusterSession, tabletKS, 1, t1, t2)
		defer dropKeyspace(t, clusterSession, tabletKS)

		createVnodeKeyspace(t, clusterSession, vnodeKs, 2, 2)
		WriteData(t, clusterSession, vnodeKs, 1, t1, t2)
		defer dropKeyspace(t, clusterSession, vnodeKs)

		tabRepairEndpoint := make(map[string]string) // The endpoint used for repairing the table
		tabCallCnt := make(map[string]int)           // The amount of API calls needed for repairing the table
		tabLoadBalancing := make(map[string]bool)    // Was load balancing enabled when table was repaired
		loadBalancing := true                        // Keeps track of current load balancing setting
		mu := sync.Mutex{}
		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if enabled, ok := newTabletLoadBalancingReq(t, req); ok {
				mu.Lock()
				loadBalancing = enabled
				mu.Unlock()
			}
			if r, ok := parseRepairReq(t, req); ok {
				mu.Lock()
				tabCallCnt[r.fullTable()]++
				// Ensure single repair endpoint per table
				if api, ok := tabRepairEndpoint[r.fullTable()]; ok {
					if api != req.URL.Path {
						t.Error("Mixing repair API for the same table")
					}
				} else {
					tabRepairEndpoint[r.fullTable()] = req.URL.Path
				}
				// Ensure single tablet load balancing setting per table
				if enabled, ok := tabLoadBalancing[r.fullTable()]; ok {
					if enabled != loadBalancing {
						t.Error("Mixing load balancing for the same table")
					}
				} else {
					tabLoadBalancing[r.fullTable()] = loadBalancing
				}
				mu.Unlock()
			}
			return nil, nil
		}))

		Print("When: run repair")
		h.runRepair(ctx, map[string]any{
			"keyspace":              []string{tabletKS, vnodeKs},
			"small_table_threshold": repairAllSmallTableThreshold,
		})

		Print("Then: repair is done")
		h.assertDone(longWait)

		Print("Then: validate used repair API")
		for _, tc := range testCases {
			for _, tab := range tc.tab {
				fn := tc.ks + "." + tab
				if api := tabRepairEndpoint[fn]; !strings.HasPrefix(api, tc.api) {
					t.Errorf("Table %q: expected API %q, got %q", fn, tc.api, api)
				}
				if cnt := tabCallCnt[fn]; cnt <= 0 || ((cnt == 1) != tc.singleCall) {
					t.Errorf("Table %q: expected single_call=%v, got %d", fn, tc.singleCall, cnt)
				}
				if enabled := tabLoadBalancing[fn]; enabled != tc.loadBalancing {
					t.Errorf("Table %q: expected tablet load balancing: %v, got %v", fn, tc.loadBalancing, enabled)
				}
			}
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

	createDefaultKeyspace(t, clusterSession, ks, 3, 3)
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
