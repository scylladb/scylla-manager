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
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	scyllaModels "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

func TestStatusIntegration(t *testing.T) {
	if IsIPV6Network() {
		t.Skip("DB node do not have ip6tables and related modules to make it work properly")
	}

	session := CreateScyllaManagerDBSession(t)
	defer session.Close()

	clusterID := uuid.MustRandom()

	s := store.NewTableStore(session, table.Secrets)
	testStatusIntegration(t, clusterID, s)
}

func TestStatusWithCQLCredentialsIntegration(t *testing.T) {
	if IsIPV6Network() {
		t.Skip("DB node do not have ip6tables and related modules to make it work properly")
	}
	username, password := ManagedClusterCredentials()

	session := CreateScyllaManagerDBSession(t)
	defer session.Close()

	clusterID := uuid.MustRandom()

	s := store.NewTableStore(session, table.Secrets)
	if err := s.Put(&secrets.CQLCreds{
		ClusterID: clusterID,
		Username:  username,
		Password:  password,
	}); err != nil {
		t.Fatal(err)
	}

	testStatusIntegration(t, clusterID, s)
}

func testStatusIntegration(t *testing.T, clusterID uuid.UUID, secretsStore store.Store) {
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
	s, err := NewService(
		c,
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			sc := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
			sc.Timeout = time.Second
			sc.Transport = hrt
			return scyllaclient.NewClient(sc, logger.Named("scylla"))
		},
		secretsStore,
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
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
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
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "TIMEOUT", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
		}
		assertEqual(t, golden, status)
	})

	t.Run("node CQL TIMEOUT", func(t *testing.T) {
		host := IPFromTestNet("12")
		blockCQL(t, host)
		defer unblockCQL(t, host)

		status, err := s.Status(context.Background(), clusterID)
		if err != nil {
			t.Error(err)
			return
		}

		for id := range status {
			status[id].Host = ToCanonicalIP(status[id].Host)
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "TIMEOUT", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
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
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "TIMEOUT"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
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
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "DOWN", RESTCause: "dial tcp " + URLEncodeIP(ToCanonicalIP(IPFromTestNet("12"))) + ":10001: connect: connection refused", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
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
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "UNAUTHORIZED", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
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
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("11")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("12")), CQLStatus: "UP", RESTStatus: "HTTP 502", AlternatorStatus: "UP"},
			{Datacenter: "dc1", Host: ToCanonicalIP(IPFromTestNet("13")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("21")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("22")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
			{Datacenter: "dc2", Host: ToCanonicalIP(IPFromTestNet("23")), CQLStatus: "UP", RESTStatus: "UP", AlternatorStatus: "UP"},
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

func blockCQL(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdBlockScyllaCQL); err != nil {
		t.Error(err)
	}
}

func unblockCQL(t *testing.T, h string) {
	t.Helper()
	if err := RunIptablesCommand(h, CmdUnblockScyllaCQL); err != nil {
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
