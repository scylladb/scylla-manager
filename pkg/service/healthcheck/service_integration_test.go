// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package healthcheck

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	scyllaModels "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// This test creates a contract saying that the status checks of CQL and alternator,
// must be independent from agent's API responsiveness.
// CQL and Alternator status checks are expected to simulate user environment, where it
// creates the session and executes simple query to get the result.
// It must be independent from any side effects.
//
// It's the answer to https://github.com/scylladb/scylla-manager/issues/3796,
// which is a part of https://github.com/scylladb/scylla-manager/issues/3767.
func TestStatus_Ping_Independent_From_REST_Integration(t *testing.T) {
	if IsIPV6Network() {
		t.Skip("DB node do not have ip6tables and related modules to make it work properly")
	}

	// Given
	tryUnblockCQL(t, ManagedClusterHosts())
	tryUnblockREST(t, ManagedClusterHosts())
	tryUnblockAlternator(t, ManagedClusterHosts())
	tryStartAgent(t, ManagedClusterHosts())

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("healthcheck")

	session := CreateScyllaManagerDBSession(t)
	defer session.Close()

	s := store.NewTableStore(session, table.Secrets)
	clusterSvc, err := cluster.NewService(session, metrics.NewClusterMetrics(), s, scyllaclient.DefaultTimeoutConfig(),
		0, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	scyllaClientProvider := func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
		sc := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
		sc.Timeout = time.Second
		sc.Transport = NewHackableRoundTripper(scyllaclient.DefaultTransport())
		return scyllaclient.NewClient(sc, logger.Named("scylla"))
	}

	hostWithUnresponsiveREST := ToCanonicalIP(IPFromTestNet("11"))
	testCluster := &cluster.Cluster{
		Host:      hostWithUnresponsiveREST,
		AuthToken: "token",
	}
	clusterWithSSL(t, testCluster, IsSSLEnabled())

	err = clusterSvc.PutCluster(context.Background(), testCluster)
	if err != nil {
		t.Fatal(err)
	}

	configCacheSvc := configcache.NewService(configcache.DefaultConfig(), clusterSvc, scyllaClientProvider, s, logger.Named("config-cache"))
	configCacheSvc.Init(context.Background())

	defaultConfigForHealthcheck := DefaultConfig()
	defaultConfigForHealthcheck.NodeInfoTTL = 0
	healthSvc, err := NewService(
		defaultConfigForHealthcheck,
		scyllaClientProvider,
		s,
		clusterSvc.GetClusterByID,
		configCacheSvc,
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}

	// When #1 -> default scenario where everything works fine
	status, err := healthSvc.Status(context.Background(), testCluster.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Then #1 -> all statuses UP
	for _, hostStatus := range status {
		if hostStatus.CQLStatus != "UP" || hostStatus.AlternatorStatus != "UP" || hostStatus.RESTStatus != "UP" {
			t.Fatalf("Expected CQL, Alternator and REST to be UP, but was {%s %s %s} on {%s}",
				hostStatus.CQLStatus, hostStatus.AlternatorStatus, hostStatus.RESTStatus, hostStatus.Host)
		}
	}

	// When #2 -> one of the hosts has unresponsive REST API
	defer unblockREST(t, hostWithUnresponsiveREST)
	blockREST(t, hostWithUnresponsiveREST)

	// Then #2 -> only REST ping fails, CQL and Alternator are fine
	status, err = healthSvc.Status(context.Background(), testCluster.ID)
	if err != nil {
		t.Fatal(err)
	}
	testedHostStatusExists := false
	for _, hostStatus := range status {
		if hostStatus.Host == hostWithUnresponsiveREST {
			testedHostStatusExists = true
			if hostStatus.CQLStatus != "UP" || hostStatus.AlternatorStatus != "UP" || hostStatus.RESTStatus == "UP" {
				t.Fatalf("Expected CQL = UP, Alternator = UP and REST != UP, but was {%s %s %s} on {%s}",
					hostStatus.CQLStatus, hostStatus.AlternatorStatus, hostStatus.RESTStatus, hostStatus.Host)
			}
			continue
		}
		if hostStatus.CQLStatus != "UP" || hostStatus.AlternatorStatus != "UP" || hostStatus.RESTStatus != "UP" {
			t.Fatalf("Expected CQL, Alternator and REST to be UP, but was {%s %s %s} on {%s}",
				hostStatus.CQLStatus, hostStatus.AlternatorStatus, hostStatus.RESTStatus, hostStatus.Host)
		}
	}
	if !testedHostStatusExists {
		t.Fatalf("Couldn't test status of {%s}", hostWithUnresponsiveREST)
	}
}

func TestStatusIntegration(t *testing.T) {
	if IsIPV6Network() {
		t.Skip("DB node do not have ip6tables and related modules to make it work properly")
	}

	session := CreateScyllaManagerDBSession(t)
	defer session.Close()

	s := store.NewTableStore(session, table.Secrets)
	clusterSvc, err := cluster.NewService(session, metrics.NewClusterMetrics(), s, scyllaclient.DefaultTimeoutConfig(),
		15*time.Minute, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	c := &cluster.Cluster{
		Host:      "192.168.200.11",
		AuthToken: "token",
	}
	clusterWithSSL(t, c, IsSSLEnabled())

	err = clusterSvc.PutCluster(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	testStatusIntegration(t, c.ID, clusterSvc, clusterSvc.GetClusterByID, s, IsSSLEnabled())
}

func TestStatusWithCQLCredentialsIntegration(t *testing.T) {
	if IsIPV6Network() {
		t.Skip("DB node do not have ip6tables and related modules to make it work properly")
	}
	username, password := ManagedClusterCredentials()

	session := CreateScyllaManagerDBSession(t)
	defer session.Close()

	s := store.NewTableStore(session, table.Secrets)
	clusterSvc, err := cluster.NewService(session, metrics.NewClusterMetrics(), s, scyllaclient.DefaultTimeoutConfig(),
		15*time.Minute, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster.Cluster{
		Host:      "192.168.200.11",
		AuthToken: "token",
		Username:  username,
		Password:  password,
	}
	clusterWithSSL(t, c, IsSSLEnabled())

	err = clusterSvc.PutCluster(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	testStatusIntegration(t, c.ID, clusterSvc, clusterSvc.GetClusterByID, s, IsSSLEnabled())
}

func testStatusIntegration(t *testing.T, clusterID uuid.UUID, clusterSvc cluster.Servicer, clusterProvider cluster.ProviderFunc, secretsStore store.Store, sslEnabled bool) {
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("healthcheck")

	// Tests here do not test the dynamic t/o functionality
	c := DefaultConfig()

	tryUnblockCQL(t, ManagedClusterHosts())
	tryUnblockREST(t, ManagedClusterHosts())
	tryUnblockAlternator(t, ManagedClusterHosts())
	tryStartAgent(t, ManagedClusterHosts())

	defer func() {
		tryUnblockCQL(t, ManagedClusterHosts())
		tryUnblockREST(t, ManagedClusterHosts())
		tryUnblockAlternator(t, ManagedClusterHosts())
		tryStartAgent(t, ManagedClusterHosts())
	}()

	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	scyllaClientProvider := func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
		sc := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
		sc.Timeout = time.Second
		sc.Transport = hrt
		return scyllaclient.NewClient(sc, logger.Named("scylla"))
	}
	configCacheSvc := configcache.NewService(configcache.DefaultConfig(), clusterSvc, scyllaClientProvider,
		secretsStore, logger.Named("config-cache"))
	configCacheSvc.Init(context.Background())

	s, err := NewService(
		c,
		scyllaClientProvider,
		secretsStore,
		clusterProvider,
		configCacheSvc,
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}

	assertEqual := func(t *testing.T, golden, status []NodeStatus) {
		t.Helper()

		opts := cmp.Options{
			UUIDComparer(),
			cmpopts.IgnoreFields(NodeStatus{}, "HostID", "Status", "CQLRtt", "RESTRtt", "AlternatorRtt",
				"TotalRAM", "Uptime", "CPUCount", "ScyllaVersion", "AgentVersion"),
		}
		if diff := cmp.Diff(golden, status, opts...); diff != "" {
			t.Errorf("Status() = %+v, diff %s", status, diff)
		}
	}

	t.Run("all nodes UP", func(t *testing.T) {
		status, err := s.Status(context.Background(), clusterID)
		if err != nil {
			t.Error(err)
			return
		}

		for id := range status {
			status[id].Host = ToCanonicalIP(status[id].Host)
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
		}
		assertEqual(t, golden, status)
	})

	t.Run("node REST TIMEOUT", func(t *testing.T) {
		host := IPFromTestNet("12")
		blockREST(t, host)
		defer unblockREST(t, host)

		status, err := s.Status(context.Background(), clusterID)
		if err != nil {
			t.Error(err)
			return
		}

		for id := range status {
			status[id].Host = ToCanonicalIP(status[id].Host)
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "TIMEOUT", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
		}
		assertEqual(t, golden, status)
	})

	t.Run("node CQL TIMEOUT", func(t *testing.T) {
		host := IPFromTestNet("12")
		blockCQL(t, host, sslEnabled)
		defer unblockCQL(t, host, sslEnabled)

		status, err := s.Status(context.Background(), clusterID)
		if err != nil {
			t.Error(err)
			return
		}

		for id := range status {
			status[id].Host = ToCanonicalIP(status[id].Host)
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "TIMEOUT", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
		}
		assertEqual(t, golden, status)
	})

	t.Run("node Alternator TIMEOUT", func(t *testing.T) {
		host := IPFromTestNet("12")
		blockAlternator(t, host)
		defer unblockAlternator(t, host)

		status, err := s.Status(context.Background(), clusterID)
		if err != nil {
			t.Error(err)
			return
		}

		for id := range status {
			status[id].Host = ToCanonicalIP(status[id].Host)
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "TIMEOUT", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
		}
		assertEqual(t, golden, status)
	})

	t.Run("node REST DOWN", func(t *testing.T) {
		host := IPFromTestNet("12")
		stopAgent(t, host)
		defer startAgent(t, host)

		status, err := s.Status(context.Background(), clusterID)
		if err != nil {
			t.Error(err)
			return
		}

		for id := range status {
			status[id].Host = ToCanonicalIP(status[id].Host)
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "DOWN", RESTCause: "dial tcp " + URLEncodeIP(ToCanonicalIP(IPFromTestNet("12"))) + ":10001: connect: connection refused", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
		}
		assertEqual(t, golden, status)
	})

	t.Run("node REST UNAUTHORIZED", func(t *testing.T) {
		hrt.SetInterceptor(fakeHealthCheckStatus(IPFromTestNet("12"), http.StatusUnauthorized))
		defer hrt.SetInterceptor(nil)

		status, err := s.Status(context.Background(), clusterID)
		if err != nil {
			t.Error(err)
			return
		}

		for id := range status {
			status[id].Host = ToCanonicalIP(status[id].Host)
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "UNAUTHORIZED", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
		}
		assertEqual(t, golden, status)
	})

	t.Run("node REST ERROR", func(t *testing.T) {
		hrt.SetInterceptor(fakeHealthCheckStatus(IPFromTestNet("12"), http.StatusBadGateway))
		defer hrt.SetInterceptor(nil)

		status, err := s.Status(context.Background(), clusterID)
		if err != nil {
			t.Error(err)
			return
		}

		for id := range status {
			status[id].Host = ToCanonicalIP(status[id].Host)
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "HTTP 502", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP", SSL: sslEnabled},
		}
		assertEqual(t, golden, status)
	})

	t.Run("all nodes REST DOWN", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for _, h := range ManagedClusterHosts() {
			blockREST(t, h)
		}
		defer func() {
			for _, h := range ManagedClusterHosts() {
				unblockREST(t, h)
			}
		}()

		msg := ""
		if _, err := s.Status(ctx, clusterID); err != nil {
			msg = err.Error()
		}
		if !strings.HasPrefix(msg, "status: giving up after") {
			t.Errorf("Status() error = %s, expected ~ status: giving up after", msg)
		}
	})

	t.Run("context timeout exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
		defer cancel()

		_, err := s.Status(ctx, clusterID)
		if err == nil {
			t.Error("Expected error got nil")
		}
	})
}

func blockREST(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdBlockScyllaREST); err != nil {
		t.Error(err)
	}
}

func unblockREST(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdUnblockScyllaREST); err != nil {
		t.Error(err)
	}
}

func tryUnblockREST(t *testing.T, hosts []string) {
	t.Helper()
	for _, host := range hosts {
		_ = RunIptablesCommand(host, CmdUnblockScyllaREST)
	}
}

func blockCQL(t *testing.T, h string, sslEnabled bool) {
	t.Helper()
	cmd := CmdBlockScyllaCQL
	if sslEnabled {
		cmd = CmdBlockScyllaCQLSSL
	}
	if err := RunIptablesCommand(h, cmd); err != nil {
		t.Error(err)
	}
}

func unblockCQL(t *testing.T, h string, sslEnabled bool) {
	t.Helper()
	cmd := CmdUnblockScyllaCQL
	if sslEnabled {
		cmd = CmdUnblockScyllaCQLSSL
	}
	if err := RunIptablesCommand(h, cmd); err != nil {
		t.Error(err)
	}
}

func tryUnblockCQL(t *testing.T, hosts []string) {
	t.Helper()
	for _, host := range hosts {
		_ = RunIptablesCommand(host, CmdUnblockScyllaCQL)
	}
}

func blockAlternator(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdBlockScyllaAlternator); err != nil {
		t.Error(err)
	}
}

func unblockAlternator(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdUnblockScyllaAlternator); err != nil {
		t.Error(err)
	}
}

func tryUnblockAlternator(t *testing.T, hosts []string) {
	t.Helper()
	for _, host := range hosts {
		_ = RunIptablesCommand(host, CmdUnblockScyllaAlternator)
	}
}

const agentService = "scylla-manager-agent"

func stopAgent(t *testing.T, h string) {
	t.Helper()
	if err := StopService(h, agentService); err != nil {
		t.Error(err)
	}
}

func startAgent(t *testing.T, h string) {
	t.Helper()
	if err := StartService(h, agentService); err != nil {
		t.Error(err)
	}
}

func tryStartAgent(t *testing.T, hosts []string) {
	t.Helper()
	for _, host := range hosts {
		_ = StartService(host, agentService)
	}
}

const pingPath = "/storage_service/scylla_release_version"

func fakeHealthCheckStatus(host string, code int) http.RoundTripper {
	return httpx.RoundTripperFunc(func(r *http.Request) (*http.Response, error) {
		h, _, err := net.SplitHostPort(r.URL.Host)
		if err != nil {
			return nil, err
		}
		if net.ParseIP(h).String() == net.ParseIP(host).String() && r.Method == http.MethodGet && r.URL.Path == pingPath {
			body, _ := (&scyllaModels.ErrorModel{
				Message: "test",
				Code:    int64(code),
			}).MarshalBinary()

			resp := &http.Response{
				Status:        http.StatusText(code),
				StatusCode:    code,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				Request:       r,
				Header:        make(http.Header, 0),
				ContentLength: int64(len(body)),
				Body:          io.NopCloser(bytes.NewReader(body)),
			}
			return resp, nil
		}
		return nil, nil
	})
}

func clusterWithSSL(t *testing.T, cluster *cluster.Cluster, sslEnabled bool) {
	t.Helper()
	if !sslEnabled {
		return
	}
	sslOpts := CQLSSLOptions()
	userKey, err := os.ReadFile(sslOpts.KeyPath)
	if err != nil {
		t.Fatalf("read file (%s) err: %v", sslOpts.KeyPath, err)
	}
	userCrt, err := os.ReadFile(sslOpts.CertPath)
	if err != nil {
		t.Fatalf("read file (%s) err: %v", sslOpts.CertPath, err)
	}
	cluster.SSLUserKeyFile = userKey
	cluster.SSLUserCertFile = userCrt
}
