// Copyright (C) 2025 ScyllaDB

//go:build all || integration

package tablet_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair/tablet"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testhelper"
)

type testHelper struct {
	*CommonTestHelper

	service          *tablet.Service
	clusterSession   gocqlx.Session
	alternatorClient *dynamodb.Client
}

func newTestHelper(t *testing.T) *testHelper {
	t.Helper()

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	client := newTestClient(t, hrt, log.NopLogger)
	smSession := CreateScyllaManagerDBSession(t)

	clusterSession := CreateManagedClusterSession(t, true, client, "", "")
	accessKeyID, secretAccessKey := GetAlternatorCreds(t, clusterSession, "")

	return &testHelper{
		CommonTestHelper: &CommonTestHelper{
			Logger:    logger,
			Session:   smSession,
			Hrt:       hrt,
			Client:    client,
			ClusterID: uuid.MustRandom(),
			TaskID:    uuid.MustRandom(),
			RunID:     uuid.NewTime(),
			T:         t,
		},
		service:          newTestService(t, smSession, client, logger),
		clusterSession:   clusterSession,
		alternatorClient: CreateAlternatorClient(t, client, ManagedClusterHost(), accessKeyID, secretAccessKey),
	}
}

func newTestClient(t *testing.T, hrt *HackableRoundTripper, logger log.Logger) *scyllaclient.Client {
	t.Helper()

	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	config.Timeout = 15 * time.Second
	config.Transport = hrt
	config.Backoff.MaxRetries = 5

	c, err := scyllaclient.NewClient(config, logger.Named("scylla"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newTestService(t *testing.T, session gocqlx.Session, client *scyllaclient.Client, logger log.Logger) *tablet.Service {
	t.Helper()

	return tablet.NewService(
		session,
		metrics.NewTabletRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		logger,
	)
}

type tabletRepairReq struct {
	keyspace string
	table    string

	incrementalMode string
}

func (r tabletRepairReq) fullTab() fullTab {
	return fullTab{
		ks:  r.keyspace,
		tab: r.table,
	}
}

func parseTabletRepairReq(t *testing.T, req *http.Request) tabletRepairReq {
	if !isTabletRepairReq(req) {
		t.Error("Not tablet repair sched req")
		return tabletRepairReq{}
	}

	sched := tabletRepairReq{
		keyspace:        req.URL.Query().Get("ks"),
		table:           req.URL.Query().Get("table"),
		incrementalMode: req.URL.Query().Get("incremental_mode"),
	}
	if sched.keyspace == "" || sched.table == "" {
		t.Error("Not fully initialized tablet repair sched req")
		return tabletRepairReq{}
	}

	return sched
}

func isTabletRepairReq(req *http.Request) bool {
	return strings.HasPrefix(req.URL.Path, "/storage_service/tablets/repair") && req.Method == http.MethodPost
}

type fullTab struct {
	ks  string
	tab string
}

func TestTabletRepairIntegration(t *testing.T) {
	h := newTestHelper(t)

	ni, err := h.Client.AnyNodeInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ni.EnableTablets {
		t.Skip("Tablet repair requires tablets to be enabled")
	}

	if ok, err := ni.SupportsTabletRepair(); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Skip("Tablet repair requires tablet repair API support")
	}

	const (
		cqlKs   = "cql_ks"
		cqlTab1 = "tab_1"
		cqlTab2 = "tab_2"
		cqlTab3 = "tab_3"
		altTab1 = "tab_1" + AlternatorProblematicTableChars
		altTab2 = "tab_2" + AlternatorProblematicTableChars
		altTab3 = "tab_3" + AlternatorProblematicTableChars
	)

	// We should enhance table setup with special tables (mv, si, gsi, lsi),
	// but this requires #4660 to be fixed first.

	// CQL setup
	WriteData(t, h.clusterSession, cqlKs, 1, cqlTab1, cqlTab2, cqlTab3)
	// Create CDC table (if supported on tablets)
	if CheckConstraint(t, ni.ScyllaVersion, ">= 2025.4") {
		ExecStmt(t, h.clusterSession, fmt.Sprintf("UPDATE %q.%q SET data = null WHERE id = 0 IF data != null", cqlKs, cqlTab3))
	}

	// Alternator setup.
	// Note that alternator tables on tablets are not supported in all scylla versions
	// which support tablet repair. Because of that, they might or might not be included
	// in the tablet repair task.
	CreateAlternatorTable(t, h.alternatorClient, ni, false, 0, 0, altTab1, altTab2, altTab3)

	// Get all tables from scylla so that we don't need to handle alternator and cdc table names manually
	allTables := make(map[fullTab]struct{})
	kss, err := h.Client.FilteredKeyspaces(context.Background(), scyllaclient.KeyspaceTypeNonLocal, scyllaclient.ReplicationTablet)
	if err != nil {
		t.Fatal(err)
	}
	for _, ks := range kss {
		tabs, err := h.Client.Tables(context.Background(), ks)
		if err != nil {
			t.Fatal(err)
		}
		for _, tab := range tabs {
			allTables[fullTab{
				ks:  ks,
				tab: tab,
			}] = struct{}{}
		}
	}

	// helper function which runs tablet repair and verifies that:
	// - it targeted all tables
	// - it made a single repair call per table
	// - it used incremental mode if supported
	smokeTest := func(t *testing.T, testCtx context.Context) {
		repairedTables := map[fullTab]struct{}{}
		mu := sync.Mutex{}

		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if !isTabletRepairReq(req) {
				return nil, nil
			}
			mu.Lock()
			defer mu.Unlock()
			// Check single call per single table
			trr := parseTabletRepairReq(t, req)
			ft := trr.fullTab()
			if _, ok := repairedTables[ft]; ok {
				t.Errorf("Multiple tablet repair calls for a single table %s", ft)
			}
			repairedTables[ft] = struct{}{}
			// Check incremental mode
			if trr.incrementalMode != "" {
				t.Errorf("Expected default incremental mode, got %q", trr.incrementalMode)
			}
			return nil, nil
		}))

		h.RunID = uuid.NewTime()
		err := h.service.Run(testCtx, h.ClusterID, h.TaskID, h.RunID, json.RawMessage{})
		if err != nil {
			t.Fatal(err)
		}
		// Check that all tables were repaired
		if !maps.Equal(allTables, repairedTables) {
			t.Fatalf("Expected repaired tables to be:\n%v\n, got:\n%v\n", allTables, repairedTables)
		}
	}

	t.Run("Smoke", func(t *testing.T) {
		testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer testCancel()
		smokeTest(t, testCtx)
	})

	// helper function which runs tablet repair and hangs it after N tables were repaired.
	// It returns:
	// - repairCtxCancel - cancels ctx of hanged repair
	// - freeRepair - unhangs hanged repair
	// - waitRepair - waits for the unhanged repair to finish
	hangAfter := func(t *testing.T, testCtx context.Context, after int) (repairCtxCancel, freeRepair func(), waitRepair func() error) {
		// Since we intercept tablet repair sched, we need to start counting from -1
		doneTabCnt := -1
		testWait := make(chan struct{})
		hangRepair := make(chan struct{})
		mu := sync.Mutex{}

		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if !isTabletRepairReq(req) {
				return nil, nil
			}
			mu.Lock()
			doneTabCnt++
			curr := doneTabCnt
			mu.Unlock()
			// Hang after N table is repaired
			if curr == after {
				close(testWait)
				select {
				case <-hangRepair:
				case <-testCtx.Done():
					return nil, testCtx.Err()
				}
			}
			return nil, nil
		}))
		// Start repair
		wg, _ := errgroup.WithContext(testCtx)
		repairCtx, repairCtxCancel := context.WithCancel(testCtx)
		wg.Go(func() error {
			h.RunID = uuid.NewTime()
			return h.service.Run(repairCtx, h.ClusterID, h.TaskID, h.RunID, json.RawMessage{})
		})
		// Wait for repair to hang
		select {
		case <-testWait:
		case <-testCtx.Done():
			t.Error(testCtx.Err())
		}
		return repairCtxCancel, func() { close(hangRepair) }, wg.Wait
	}

	t.Run("Pause and resume", func(t *testing.T) {
		testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer testCancel()
		// Hang running repair
		const interruptAfter = 2
		repairCtxCancel, freeRepair, waitRepair := hangAfter(t, testCtx, interruptAfter)
		// Verify progress of running repair
		pr, err := h.service.GetProgress(testCtx, h.ClusterID, h.TaskID, h.RunID)
		if err != nil {
			t.Fatal(err)
		}
		var completedTabCnt int
		for _, tab := range pr.Tables {
			if tab.CompletedAt != nil && !tab.CompletedAt.IsZero() && tab.Error == "" {
				completedTabCnt++
			}
		}
		if completedTabCnt != interruptAfter {
			t.Fatalf("Expected %d completed tables, got %d from progress %+v", interruptAfter, completedTabCnt, pr)
		}
		// Pause repair and let it finish
		repairCtxCancel()
		freeRepair()
		if err := waitRepair(); !errors.Is(err, context.Canceled) {
			t.Fatalf("Expected context canceled error, got %v", err)
		}
		// Resume repair and verify that it ran from scratch
		smokeTest(t, testCtx)
	})

	t.Run("Transient node down", func(t *testing.T) {
		testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer testCancel()
		// Hang running repair
		const interruptAfter = 1
		_, freeRepair, waitRepair := hangAfter(t, testCtx, interruptAfter)
		// Take node down when repair is running
		downNode := ManagedClusterHost()
		ni, err := h.Client.NodeInfo(testCtx, downNode)
		if err != nil {
			t.Fatal(err)
		}
		h.StopNode(downNode)
		// Unhang repair
		freeRepair()
		// Let repair run with node down for a while
		time.Sleep(15 * time.Second)
		// Bring node up
		h.StartNode(downNode, ni)
		// Verify that repair finished successfully
		if err := waitRepair(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Ongoing scylla tablet repair tasks", func(t *testing.T) {
		testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer testCancel()

		h.Hrt.SetInterceptor(nil)
		// Start lots of scylla tablet repair tasks
		for ft := range allTables {
			_ = h.Client.TabletRepair(testCtx, ft.ks, ft.tab, scyllaclient.TabletRepairParams{})
		}
		// Make sure that they are visible on all nodes
		for _, host := range ManagedClusterHosts() {
			if err := h.Client.RaftReadBarrier(testCtx, host, ""); err != nil {
				t.Fatal(err)
			}
		}
		// Verify that those scylla tablet repair tasks won't break SM tablet repair task
		smokeTest(t, testCtx)
	})
}
