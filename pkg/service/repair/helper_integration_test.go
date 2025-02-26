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
func createTabletKeyspace(t *testing.T, session gocqlx.Session, keyspace string, rf1, rf2, tablets int) {
	if !globalNodeInfo.EnableTablets {
		t.Skip("Test requires tablets enabled in order to create tablet keyspace")
	}
	stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d} AND tablets = {'enabled': true, 'initial': %d}"
	ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2, tablets))
}

// Creates keyspace with default replication type (vnode or tablets).
func createDefaultKeyspace(t *testing.T, session gocqlx.Session, keyspace string, rf1, rf2, tablets int) {
	if globalNodeInfo.EnableTablets {
		stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d} AND tablets = {'enabled': true, 'initial': %d}"
		ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2, tablets))
		return
	}
	stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d}"
	ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2))
}

func dropKeyspace(t *testing.T, session gocqlx.Session, keyspace string) {
	ExecStmt(t, session, fmt.Sprintf("DROP KEYSPACE IF EXISTS %q", keyspace))
}

type repairSchedReq struct {
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

func (r repairSchedReq) fullTable() string {
	return r.keyspace + "." + r.table
}

type repairStatusReq struct {
	host string
	id   string
}

type repairSchedResp struct {
	repairSchedReq
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
	oldRepairSchedPathPrefix  = "/storage_service/repair_async"
	oldRepairStatusPathPrefix = "/storage_service/repair_status"
	killRepairPathPrefix      = "/storage_service/force_terminate_repair"
)

func isOldRepairSchedReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, oldRepairSchedPathPrefix) && req.Method == http.MethodPost
}

func isRepairSchedReq(req *http.Request) bool {
	return isOldRepairSchedReq(req)
}

func isOldRepairStatusReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, oldRepairStatusPathPrefix) && req.Method == http.MethodGet
}

func isRepairStatusReq(req *http.Request) bool {
	return isOldRepairStatusReq(req)
}

func isKillRepairReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, killRepairPathPrefix) && req.Method == http.MethodPost
}

func newRepairSchedReq(t *testing.T, req *http.Request) (repairSchedReq, bool) {
	if isOldRepairSchedReq(req) {
		return newOldRepairSchedReq(t, req), true
	}
	return repairSchedReq{}, false
}

func newOldRepairSchedReq(t *testing.T, req *http.Request) repairSchedReq {
	if !isOldRepairSchedReq(req) {
		t.Error("Not old repair sched req")
		return repairSchedReq{}
	}

	sched := repairSchedReq{
		host:                   req.Host,
		keyspace:               strings.TrimPrefix(req.URL.Path, oldRepairSchedPathPrefix+"/"),
		table:                  req.URL.Query().Get("columnFamilies"),
		replicaSet:             strings.Split(req.URL.Query().Get("hosts"), ","),
		ranges:                 parseRanges(t, req.URL.Query().Get("ranges")),
		SmallTableOptimization: req.URL.Query().Get("small_table_optimization") == "true",
	}
	if rawRangesParallelism := req.URL.Query().Get("ranges_parallelism"); rawRangesParallelism != "" {
		rangesParallelism, err := strconv.Atoi(rawRangesParallelism)
		if err != nil {
			t.Error(err)
			return repairSchedReq{}
		}
		sched.RangesParallelism = rangesParallelism
	}
	if sched.keyspace == "" || sched.table == "" || len(sched.replicaSet) == 0 {
		t.Error("Not fully initialized old repair sched req")
		return repairSchedReq{}
	}

	return sched
}

func newRepairStatusReq(t *testing.T, req *http.Request) (repairStatusReq, bool) {
	if isOldRepairStatusReq(req) {
		return newOldRepairStatusReq(t, req), true
	}
	return repairStatusReq{}, false
}

func newOldRepairStatusReq(t *testing.T, req *http.Request) repairStatusReq {
	if !isOldRepairStatusReq(req) {
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

func newRepairSchedResp(t *testing.T, resp *http.Response) (repairSchedResp, bool) {
	if resp.StatusCode != http.StatusOK {
		return repairSchedResp{}, false
	}
	if isOldRepairSchedReq(resp.Request) {
		return newOldRepairSchedResp(t, resp), true
	}
	return repairSchedResp{}, false

}

func newOldRepairSchedResp(t *testing.T, resp *http.Response) repairSchedResp {
	req, ok := newRepairSchedReq(t, resp.Request)
	if !ok {
		t.Error("Not repair sched resp")
		return repairSchedResp{}
	}

	sched := repairSchedResp{
		repairSchedReq: req,
		id:             string(copyRespBody(t, resp)),
	}
	if sched.id == "" {
		t.Error("Not fully initialized repair sched resp")
		return repairSchedResp{}
	}

	return sched
}

func newRepairStatusResp(t *testing.T, resp *http.Response) (repairStatusResp, bool) {
	if resp.StatusCode != http.StatusOK {
		return repairStatusResp{}, false
	}
	if isOldRepairStatusReq(resp.Request) {
		return newOldRepairStatusResp(t, resp), true
	}
	return repairStatusResp{}, false
}

func newOldRepairStatusResp(t *testing.T, resp *http.Response) repairStatusResp {
	req, ok := newRepairStatusReq(t, resp.Request)
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

func mockRepairSchedRespBody(t *testing.T, req *http.Request) (io.ReadCloser, bool) {
	if isOldRepairSchedReq(req) {
		return mockOldRepairSchedRespBody(t, req), true
	}
	return nil, false
}

var repairTaskCounter int32

func mockOldRepairSchedRespBody(t *testing.T, req *http.Request) io.ReadCloser {
	if !isOldRepairSchedReq(req) {
		t.Error("Not old repair sched req")
	}

	return io.NopCloser(bytes.NewBufferString(fmt.Sprint(atomic.AddInt32(&repairTaskCounter, 1))))
}

func mockRepairStatusRespBody(t *testing.T, req *http.Request, status repairStatus) (io.ReadCloser, bool) {
	if isOldRepairStatusReq(req) {
		return mockOldRepairStatusRespBody(t, req, status), true
	}
	return nil, false
}

func mockOldRepairStatusRespBody(t *testing.T, req *http.Request, status repairStatus) io.ReadCloser {
	if !isOldRepairStatusReq(req) {
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
		if body, ok := mockRepairSchedRespBody(t, req); ok {
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
		if body, ok := mockRepairSchedRespBody(t, req); ok {
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

func repairReqAssertHostInterceptor(t *testing.T, host string) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if r, ok := newRepairSchedReq(t, req); ok {
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
