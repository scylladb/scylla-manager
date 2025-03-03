// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package repair_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
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
)

// Read only, should be used for checking testing environment
// like Scylla version or tablets.
var globalNodeInfo *scyllaclient.NodeInfo

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
	host       string
	keyspace   string
	table      string
	replicaSet []string
	ranges     []scyllaclient.TokenRange

	// optional
	SmallTableOptimization bool
	RangesParallelism      int
}

func (r repairReq) fullTable() string {
	return r.keyspace + "." + r.table
}

type repairStatusReq struct {
	host string
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
	forceTerminateRepairEndpoint = "/storage_service/force_terminate_repair"
)

func isRepairAsyncReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, repairAsyncEndpoint) && req.Method == http.MethodPost
}

func isRepairReq(req *http.Request) bool {
	return isRepairAsyncReq(req)
}

func isRepairAsyncStatusReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, repairStatusEndpoint) && req.Method == http.MethodGet
}

func isRepairStatusReq(req *http.Request) bool {
	return isRepairAsyncStatusReq(req)
}

func isForceTerminateRepairReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, forceTerminateRepairEndpoint) && req.Method == http.MethodPost
}

func parseRepairReq(t *testing.T, req *http.Request) (repairReq, bool) {
	if isRepairAsyncReq(req) {
		return parseRepairAsyncReq(t, req), true
	}
	return repairReq{}, false
}

func parseRepairAsyncReq(t *testing.T, req *http.Request) repairReq {
	if !isRepairAsyncReq(req) {
		t.Error("Not old repair sched req")
		return repairReq{}
	}

	sched := repairReq{
		host:                   req.Host,
		keyspace:               strings.TrimPrefix(req.URL.Path, repairAsyncEndpoint+"/"),
		table:                  req.URL.Query().Get("columnFamilies"),
		replicaSet:             strings.Split(req.URL.Query().Get("hosts"), ","),
		ranges:                 parseRanges(t, req.URL.Query().Get("ranges")),
		SmallTableOptimization: req.URL.Query().Get("small_table_optimization") == "true",
	}
	if rawRangesParallelism := req.URL.Query().Get("ranges_parallelism"); rawRangesParallelism != "" {
		rangesParallelism, err := strconv.Atoi(rawRangesParallelism)
		if err != nil {
			t.Error(err)
			return repairReq{}
		}
		sched.RangesParallelism = rangesParallelism
	}
	if sched.keyspace == "" || sched.table == "" || len(sched.replicaSet) == 0 {
		t.Error("Not fully initialized old repair sched req")
		return repairReq{}
	}

	return sched
}

func parseRepairStatusReq(t *testing.T, req *http.Request) (repairStatusReq, bool) {
	if isRepairAsyncStatusReq(req) {
		return parseRepairAsyncStatusReq(t, req), true
	}
	return repairStatusReq{}, false
}

func parseRepairAsyncStatusReq(t *testing.T, req *http.Request) repairStatusReq {
	if !isRepairAsyncStatusReq(req) {
		t.Error("Not old repair status req")
		return repairStatusReq{}
	}

	status := repairStatusReq{
		host: req.Host,
		id:   req.URL.Query().Get("id"),
	}
	if status.id == "" {
		t.Error("Not fully initialized old repair status req")
		return repairStatusReq{}
	}

	return status
}

func parseRepairResp(t *testing.T, resp *http.Response) (repairResp, bool) {
	if resp.StatusCode != http.StatusOK {
		return repairResp{}, false
	}
	if isRepairAsyncReq(resp.Request) {
		return parseRepairAsyncResp(t, resp), true
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

func parseRepairStatusResp(t *testing.T, resp *http.Response) (repairStatusResp, bool) {
	if resp.StatusCode != http.StatusOK {
		return repairStatusResp{}, false
	}
	if isRepairAsyncStatusReq(resp.Request) {
		return parseRepairAsyncStatusResp(t, resp), true
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

func mockRepairRespBody(t *testing.T, req *http.Request) (io.ReadCloser, bool) {
	if isRepairAsyncReq(req) {
		return mockRepairAsyncRespBody(t, req), true
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

func mockRepairStatusRespBody(t *testing.T, req *http.Request, status repairStatus) (io.ReadCloser, bool) {
	if isRepairAsyncStatusReq(req) {
		return mockRepairAsyncStatusRespBody(t, req, status), true
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

func repairStatusInterceptor(t *testing.T, status repairStatus) http.RoundTripper {
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

func repairHoldInterceptor(t *testing.T, ctx context.Context, after int64) http.RoundTripper {
	cnt := &atomic.Int64{}
	cnt.Add(after)
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if body, ok := mockRepairRespBody(t, req); ok {
			resp := httpx.MakeResponse(req, http.StatusOK)
			resp.Body = body
			return resp, nil
		}
		if body, ok := mockRepairStatusRespBody(t, req, repairStatusDone); ok {
			resp := httpx.MakeResponse(req, 200)
			resp.Body = body
			if v := cnt.Add(-1); v < 0 {
				<-ctx.Done()
				return resp, nil
			}
			return resp, nil
		}
		return nil, nil
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

func repairReqAssertHostInterceptor(t *testing.T, host string) http.RoundTripper {
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
