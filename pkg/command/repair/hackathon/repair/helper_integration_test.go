// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package repair_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	slices2 "github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/client/operations"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// Read only, should be used for checking testing environment
// like Scylla version or tablets.
var globalNodeInfo *scyllaclient.NodeInfo

func tabletRepairSupport(t *testing.T) bool {
	t.Helper()

	ok, err := globalNodeInfo.SupportsTabletRepair()
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// Used to fill globalNodeInfo before running the tests.
func TestMain(m *testing.M) {
	if !flag.Parsed() {
		flag.Parse()
	}

	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())

	logger := log.NewDevelopment().Named("Setup")
	c, err := scyllaclient.NewClient(config, logger)
	if err != nil {
		logger.Fatal(context.Background(), "Failed to create client", "error", err)
	}

	globalNodeInfo, err = c.AnyNodeInfo(context.Background())
	if err != nil {
		logger.Fatal(context.Background(), "Failed to get global node info", "error", err)
	}

	os.Exit(m.Run())
}

// Creates vnode keyspace.
func createVnodeKeyspace(t *testing.T, session gocqlx.Session, keyspace string, rf1, rf2 int) {
	stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d}"
	if globalNodeInfo.EnableTablets {
		stmt += " AND tablets = {'enabled': false}"
	}
	ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2))
}

// Creates tablet keyspace or skips the test if that's not possible.
func createTabletKeyspace(t *testing.T, session gocqlx.Session, keyspace string, rf1, rf2 int) {
	if !globalNodeInfo.EnableTablets {
		t.Skip("Test requires tablets enabled in order to create tablet keyspace")
	}
	stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d} AND tablets = {'enabled': true}"
	ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2))
}

// Creates keyspace with default replication type (vnode or tablets).
func createDefaultKeyspace(t *testing.T, session gocqlx.Session, keyspace string, rf1, rf2 int) {
	if globalNodeInfo.EnableTablets {
		stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d} AND tablets = {'enabled': true}"
		ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2))
		return
	}
	stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d}"
	ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2))
}

func dropKeyspace(t *testing.T, session gocqlx.Session, keyspace string) {
	ExecStmt(t, session, fmt.Sprintf("DROP KEYSPACE IF EXISTS %q", keyspace))
}

type repairReq struct {
	// always set
	host     netip.Addr
	keyspace string
	table    string
	// always set for vnode
	replicaSet []netip.Addr
	ranges     []scyllaclient.TokenRange
	// optional for vnode
	smallTableOptimization bool
	rangesParallelism      int
	// optional for tablet
	dcFilter   []string
	hostFilter []netip.Addr
}

func (r repairReq) fullTable() string {
	return r.keyspace + "." + r.table
}

type repairStatusReq struct {
	host netip.Addr
	id   string
}

type repairResp struct {
	repairReq
	id string
}
type repairStatusResp struct {
	repairStatusReq
	status repairStatus
}

type repairStatus int

const (
	repairStatusDone repairStatus = iota
	repairStatusFailed
	repairStatusRunning
)

const (
	repairAsyncEndpoint          = "/storage_service/repair_async"
	repairStatusEndpoint         = "/storage_service/repair_status"
	tabletRepairEndpoint         = "/storage_service/tablets/repair"
	waitTaskEndpoint             = "/task_manager/wait_task"
	forceTerminateRepairEndpoint = "/storage_service/force_terminate_repair"
	tabletBalancingEndpoint      = "/storage_service/tablets/balancing"
)

func isRepairAsyncReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, repairAsyncEndpoint) && req.Method == http.MethodPost
}

func isTabletRepairReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, tabletRepairEndpoint) && req.Method == http.MethodPost
}

func isRepairReq(req *http.Request) bool {
	return isRepairAsyncReq(req) || isTabletRepairReq(req)
}

func isRepairAsyncStatusReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, repairStatusEndpoint) && req.Method == http.MethodGet
}

func isTabletRepairStatusReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, waitTaskEndpoint) && req.Method == http.MethodGet
}

func isRepairStatusReq(req *http.Request) bool {
	return isRepairAsyncStatusReq(req) || isTabletRepairStatusReq(req)
}

func isForceTerminateRepairReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, forceTerminateRepairEndpoint) && req.Method == http.MethodPost
}

func isTabletBalancingReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, tabletBalancingEndpoint) && req.Method == http.MethodPost
}

// Returns parsed repair request and true if provided with repair request.
// If provided with any other request, returns an empty struct and false.
func parseRepairReq(t *testing.T, req *http.Request) (repairReq, bool) {
	if isRepairAsyncReq(req) {
		return parseRepairAsyncReq(t, req), true
	}
	if isTabletRepairReq(req) {
		return parseTabletRepairReq(t, req), true
	}
	return repairReq{}, false
}

func parseRepairAsyncReq(t *testing.T, req *http.Request) repairReq {
	if !isRepairAsyncReq(req) {
		t.Error("Not old repair sched req")
		return repairReq{}
	}

	sched := repairReq{
		host:                   netip.MustParseAddr(req.URL.Hostname()),
		keyspace:               strings.TrimPrefix(req.URL.Path, repairAsyncEndpoint+"/"),
		table:                  req.URL.Query().Get("columnFamilies"),
		ranges:                 parseRanges(t, req.URL.Query().Get("ranges")),
		smallTableOptimization: req.URL.Query().Get("small_table_optimization") == "true",
	}
	if replicaSet := req.URL.Query().Get("hosts"); replicaSet != "" {
		sched.replicaSet = slices2.Map(strings.Split(replicaSet, ","), netip.MustParseAddr)
	}
	if rawRangesParallelism := req.URL.Query().Get("ranges_parallelism"); rawRangesParallelism != "" {
		rangesParallelism, err := strconv.Atoi(rawRangesParallelism)
		if err != nil {
			t.Error(err)
			return repairReq{}
		}
		sched.rangesParallelism = rangesParallelism
	}
	if sched.keyspace == "" || sched.table == "" || len(sched.replicaSet) == 0 {
		t.Error("Not fully initialized old repair sched req")
		return repairReq{}
	}

	return sched
}

func parseTabletRepairReq(t *testing.T, req *http.Request) repairReq {
	if !isTabletRepairReq(req) {
		t.Error("Not tablet repair sched req")
		return repairReq{}
	}

	sched := repairReq{
		host:     netip.MustParseAddr(req.URL.Hostname()),
		keyspace: req.URL.Query().Get("ks"),
		table:    req.URL.Query().Get("table"),
	}
	if dcFilter := req.URL.Query().Get("dcs_filter"); dcFilter != "" {
		sched.dcFilter = strings.Split(dcFilter, ",")
	}
	if hostsFilter := req.URL.Query().Get("hosts_filter"); hostsFilter != "" {
		sched.hostFilter = slices2.Map(strings.Split(hostsFilter, ","), netip.MustParseAddr)
	}
	if sched.keyspace == "" || sched.table == "" {
		t.Error("Not fully initialized tablet repair sched req")
		return repairReq{}
	}

	return sched
}

// Returns parsed repair status request and true if provided with repair status request.
// If provided with any other request, returns an empty struct and false.
func parseRepairStatusReq(t *testing.T, req *http.Request) (repairStatusReq, bool) {
	if isRepairAsyncStatusReq(req) {
		return parseRepairAsyncStatusReq(t, req), true
	}
	if isTabletRepairStatusReq(req) {
		return parseTabletRepairStatusReq(t, req), true
	}
	return repairStatusReq{}, false
}

func parseRepairAsyncStatusReq(t *testing.T, req *http.Request) repairStatusReq {
	if !isRepairAsyncStatusReq(req) {
		t.Error("Not old repair status req")
		return repairStatusReq{}
	}

	status := repairStatusReq{
		host: netip.MustParseAddr(req.URL.Hostname()),
		id:   req.URL.Query().Get("id"),
	}
	if status.id == "" {
		t.Error("Not fully initialized old repair status req")
		return repairStatusReq{}
	}

	return status
}

func parseTabletRepairStatusReq(t *testing.T, req *http.Request) repairStatusReq {
	if !isTabletRepairStatusReq(req) {
		t.Error("Not tablet repair status req")
		return repairStatusReq{}
	}

	status := repairStatusReq{
		host: netip.MustParseAddr(req.URL.Hostname()),
		id:   strings.TrimPrefix(req.URL.Path, waitTaskEndpoint+"/"),
	}
	if status.id == "" {
		t.Error("Not fully initialized tablet repair status req")
		return repairStatusReq{}
	}

	return status
}

// Returns parsed repair response and true if provided with repair response.
// If provided with any other response, returns an empty struct and false.
func parseRepairResp(t *testing.T, resp *http.Response) (repairResp, bool) {
	if resp.StatusCode != http.StatusOK {
		return repairResp{}, false
	}
	if isRepairAsyncReq(resp.Request) {
		return parseRepairAsyncResp(t, resp), true
	}
	if isTabletRepairReq(resp.Request) {
		return parseTabletRepairResp(t, resp), true
	}
	return repairResp{}, false
}

func parseRepairAsyncResp(t *testing.T, resp *http.Response) repairResp {
	req, ok := parseRepairReq(t, resp.Request)
	if !ok {
		t.Error("Not repair sched resp")
		return repairResp{}
	}

	sched := repairResp{
		repairReq: req,
		id:        string(copyRespBody(t, resp)),
	}
	if sched.id == "" {
		t.Error("Not fully initialized repair sched resp")
		return repairResp{}
	}

	return sched
}

func parseTabletRepairResp(t *testing.T, resp *http.Response) repairResp {
	req, ok := parseRepairReq(t, resp.Request)
	if !ok {
		t.Error("Not repair sched resp")
		return repairResp{}
	}

	var b operations.StorageServiceTabletsRepairPostOKBody
	if err := json.Unmarshal(copyRespBody(t, resp), &b); err != nil {
		t.Error(err)
		return repairResp{}
	}
	sched := repairResp{
		repairReq: req,
		id:        b.TabletTaskID,
	}
	if sched.id == "" {
		t.Error("Not fully initialized repair sched resp")
		return repairResp{}
	}

	return sched
}

// Returns parsed repair status response and true if provided with repair status response.
// If provided with any other response, returns an empty struct and false.
func parseRepairStatusResp(t *testing.T, resp *http.Response) (repairStatusResp, bool) {
	if resp.StatusCode != http.StatusOK {
		return repairStatusResp{}, false
	}
	if isRepairAsyncStatusReq(resp.Request) {
		return parseRepairAsyncStatusResp(t, resp), true
	}
	if isTabletRepairStatusReq(resp.Request) {
		return parseTabletRepairStatusResp(t, resp), true
	}
	return repairStatusResp{}, false
}

func parseRepairAsyncStatusResp(t *testing.T, resp *http.Response) repairStatusResp {
	req, ok := parseRepairStatusReq(t, resp.Request)
	if !ok {
		t.Error("Not repair status resp")
		return repairStatusResp{}
	}

	body := string(copyRespBody(t, resp))
	var status repairStatus
	switch scyllaclient.CommandStatus(strings.Trim(body, "\"")) {
	case scyllaclient.CommandSuccessful:
		status = repairStatusDone
	case scyllaclient.CommandFailed:
		status = repairStatusFailed
	case scyllaclient.CommandRunning:
		status = repairStatusRunning
	default:
		t.Error("Unknown old repair status: " + body)
		return repairStatusResp{}
	}

	return repairStatusResp{
		repairStatusReq: req,
		status:          status,
	}
}

func parseTabletRepairStatusResp(t *testing.T, resp *http.Response) repairStatusResp {
	req, ok := parseRepairStatusReq(t, resp.Request)
	if !ok {
		t.Error("Not repair status resp")
		return repairStatusResp{}
	}

	var taskStatus models.TaskStatus
	body := copyRespBody(t, resp)
	if err := json.Unmarshal(body, &taskStatus); err != nil {
		t.Error(err)
		return repairStatusResp{}
	}
	var status repairStatus
	switch scyllaclient.ScyllaTaskState(taskStatus.State) {
	case scyllaclient.ScyllaTaskStateDone:
		status = repairStatusDone
	case scyllaclient.ScyllaTaskStateFailed:
		status = repairStatusFailed
	case scyllaclient.ScyllaTaskStateCreated, scyllaclient.ScyllaTaskStateRunning:
		status = repairStatusRunning
	default:
		t.Error("Unknown tablet repair status: " + string(body))
		return repairStatusResp{}
	}

	return repairStatusResp{
		repairStatusReq: req,
		status:          status,
	}
}

// Returns whether tablet load balancing is enabled and true if provided with tablet balancing request.
// If provided with any other request, returns false and false.
func newTabletLoadBalancingReq(t *testing.T, req *http.Request) (enabled bool, ok bool) {
	if !isTabletBalancingReq(req) {
		return false, false
	}

	rawEnabled := req.URL.Query().Get("enabled")
	switch rawEnabled {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		t.Error("Unexpected 'enabled' query param")
		return false, false
	}
}

// Returns mocked body of repair response and true if provided with repair request.
// If provided with any other request, returns nil and false.
func mockRepairRespBody(t *testing.T, req *http.Request) (io.ReadCloser, bool) {
	if isRepairAsyncReq(req) {
		return mockRepairAsyncRespBody(t, req), true
	}
	if isTabletRepairReq(req) {
		return mockTabletRepairSchedRespBody(t, req), true
	}
	return nil, false
}

var repairTaskCounter int32

func mockRepairAsyncRespBody(t *testing.T, req *http.Request) io.ReadCloser {
	if !isRepairAsyncReq(req) {
		t.Error("Not old repair sched req")
	}

	return io.NopCloser(bytes.NewBufferString(fmt.Sprint(atomic.AddInt32(&repairTaskCounter, 1))))
}

func mockTabletRepairSchedRespBody(t *testing.T, req *http.Request) io.ReadCloser {
	if !isTabletRepairReq(req) {
		t.Error("Not tablet repair sched req")
	}

	b, err := json.Marshal(operations.StorageServiceTabletsRepairPostOKBody{
		TabletTaskID: uuid.NewTime().String(),
	})
	if err != nil {
		t.Error(err)
		return nil
	}

	return io.NopCloser(bytes.NewBuffer(b))
}

// Returns mocked body of repair status response and true if provided with repair status request.
// If provided with any other request, returns nil and false.
func mockRepairStatusRespBody(t *testing.T, req *http.Request, status repairStatus) (io.ReadCloser, bool) {
	if isRepairAsyncStatusReq(req) {
		return mockRepairAsyncStatusRespBody(t, req, status), true
	}
	if isTabletRepairStatusReq(req) {
		return mockTabletRepairStatusRespBody(t, req, status), true
	}
	return nil, false
}

func mockRepairAsyncStatusRespBody(t *testing.T, req *http.Request, status repairStatus) io.ReadCloser {
	if !isRepairAsyncStatusReq(req) {
		t.Error("Not old repair status req")
		return nil
	}

	var s scyllaclient.CommandStatus
	switch status {
	case repairStatusDone:
		s = scyllaclient.CommandSuccessful
	case repairStatusFailed:
		s = scyllaclient.CommandFailed
	case repairStatusRunning:
		s = scyllaclient.CommandRunning
	default:
		t.Errorf("Unknown old repair status: %d", status)
		return nil
	}

	return io.NopCloser(bytes.NewBufferString(fmt.Sprintf("%q", s)))
}

func mockTabletRepairStatusRespBody(t *testing.T, req *http.Request, status repairStatus) io.ReadCloser {
	if !isTabletRepairStatusReq(req) {
		t.Error("Not tablet repair status req")
		return nil
	}

	var s models.TaskStatus
	switch status {
	case repairStatusDone:
		s = models.TaskStatus{
			ProgressCompleted: 1,
			ProgressTotal:     1,
			State:             string(scyllaclient.ScyllaTaskStateDone),
		}
	case repairStatusFailed:
		s = models.TaskStatus{
			ProgressTotal: 1,
			State:         string(scyllaclient.ScyllaTaskStateFailed),
		}
	case repairStatusRunning:
		s = models.TaskStatus{
			ProgressTotal: 1,
			State:         string(scyllaclient.ScyllaTaskStateRunning),
		}
	default:
		t.Errorf("Unknown tablet repair status: %d", status)
		return nil
	}

	b := &bytes.Buffer{}
	if err := json.NewEncoder(b).Encode(s); err != nil {
		t.Error(err)
		return nil
	}
	return io.NopCloser(b)
}

// It allows for mocking responses to both repair and repair status requests.
// For repair requests, it mocks response with random repair ID.
// For repair status requests, it mocks response with provided status.
// When mocking, it's usually important to mock both responses to repair
// and repair status requests. If we mock only one of those, we might end up
// in a situation when SM schedules repair jobs in a manner which breaks
// its contract (e.g. by breaking the one job per one host rule), but not because
// of incorrect SM behavior, but rather not appropriate mocking.
func repairMockInterceptor(t *testing.T, status repairStatus) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if body, ok := mockRepairRespBody(t, req); ok {
			resp := httpx.MakeResponse(req, http.StatusOK)
			resp.Body = body
			return resp, nil
		}
		if body, ok := mockRepairStatusRespBody(t, req, status); ok {
			resp := httpx.MakeResponse(req, http.StatusOK)
			resp.Body = body
			return resp, nil
		}
		return nil, nil
	})
}

// It allows for mocking responses to both repair and repair status requests
// (similar to repairMockInterceptor with repair statusStatusDone).
// Apart from that, it also starts blocking on repair status requests
// after mocking a response to blockAfter of them.
func repairMockAndBlockInterceptor(t *testing.T, ctx context.Context, blockAfter int64) http.RoundTripper {
	i := repairMockInterceptor(t, repairStatusDone)
	cnt := &atomic.Int64{}
	cnt.Add(blockAfter)
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := i.RoundTrip(req)
		if err != nil {
			return resp, err
		}
		if isRepairStatusReq(req) {
			if v := cnt.Add(-1); v < 0 {
				<-ctx.Done()
			}
		}
		return resp, nil
	})
}

func repairRunningInterceptor() (http.RoundTripper, chan struct{}) {
	done := make(chan struct{})
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if isRepairReq(req) {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		return nil, nil
	}), done
}

func repairReqAssertHostInterceptor(t *testing.T, host netip.Addr) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if r, ok := parseRepairReq(t, req); ok {
			if !slices.Contains(r.replicaSet, host) {
				err := fmt.Errorf("hosts query param (%v) are missing host (%s)", r.replicaSet, host)
				t.Error(err)
				return nil, err
			}
		}
		return nil, nil
	})
}

func countInterceptor(counter *int32, reqMatcher func(*http.Request) bool) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if reqMatcher(req) {
			atomic.AddInt32(counter, 1)
		}
		return nil, nil
	})
}

func dialErrorInterceptor() http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("mock dial error")
	})
}

func combineInterceptors(interceptors ...http.RoundTripper) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		for _, i := range interceptors {
			resp, err := i.RoundTrip(req)
			if resp != nil || err != nil {
				return resp, err
			}
		}
		return nil, nil
	})
}

func parseRanges(t *testing.T, dumpedRanges string) []scyllaclient.TokenRange {
	if dumpedRanges == "" {
		return nil
	}
	var out []scyllaclient.TokenRange
	for _, r := range strings.Split(dumpedRanges, ",") {
		tokens := strings.Split(r, ":")
		s, err := strconv.ParseInt(tokens[0], 10, 64)
		if err != nil {
			t.Error(err)
			return nil
		}
		e, err := strconv.ParseInt(tokens[1], 10, 64)
		if err != nil {
			t.Error(err)
			return nil
		}
		out = append(out, scyllaclient.TokenRange{
			StartToken: s,
			EndToken:   e,
		})
	}
	return out
}

func copyRespBody(t *testing.T, resp *http.Response) []byte {
	var copiedBody bytes.Buffer
	tee := io.TeeReader(resp.Body, &copiedBody)
	body, err := io.ReadAll(tee)
	if err != nil {
		t.Error(err)
		return nil
	}
	resp.Body = io.NopCloser(&copiedBody)
	return body
}

func chanClosedWithin(t *testing.T, c chan struct{}, d time.Duration) {
	select {
	case <-c:
	case <-time.After(d):
		t.Fatal("timeout after ", d)
	}
}
