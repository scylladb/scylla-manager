// Copyright (C) 2017 ScyllaDB

// +build all integration

package healthcheck

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/service/secrets"
	"github.com/scylladb/mermaid/pkg/service/secrets/dbsecrets"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/httpx"
	"github.com/scylladb/mermaid/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

func TestStatusIntegration(t *testing.T) {
	session := CreateSession(t)
	defer session.Close()

	secretsStore, err := dbsecrets.New(session)
	if err != nil {
		t.Fatal(err)
	}
	testStatusIntegration(t, secretsStore)
}

type testStore []byte

var _ secrets.Store = &testStore{}

func (t *testStore) Put(secret secrets.KeyValue) error {
	if _, ok := secret.(*secrets.CQLCreds); !ok {
		return nil
	}

	v, err := secret.MarshalBinary()
	if err != nil {
		return err
	}
	*t = v
	return nil
}

func (t *testStore) Get(secret secrets.KeyValue) error {
	if _, ok := secret.(*secrets.CQLCreds); !ok {
		return service.ErrNotFound
	}

	if len(*t) == 0 {
		return service.ErrNotFound
	}
	return secret.UnmarshalBinary(*t)
}

func (t *testStore) Delete(secret secrets.KeyValue) error {
	panic("not implemented")
}

func (t *testStore) DeleteAll(clusterID uuid.UUID) error {
	panic("not implemented")
}

func (t *testStore) version() byte {
	if len(*t) == 0 {
		return 0
	}
	return []byte(*t)[0]
}

func TestStatusWithCQLCredentialsIntegration(t *testing.T) {
	username, password := ManagedClusterCredentials()

	session := CreateSession(t)
	defer session.Close()

	secretsStore := &testStore{}
	secretsStore.Put(&secrets.CQLCreds{
		Username: username,
		Password: password,
	})

	testStatusIntegration(t, secretsStore)
}

func testStatusIntegration(t *testing.T, secretsStore secrets.Store) {
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("healthcheck")

	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	s, err := NewService(
		DefaultConfig(),
		func(ctx context.Context, id uuid.UUID) (string, error) {
			return "test_cluster", nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			conf := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
			conf.Transport = hrt
			return scyllaclient.NewClient(conf, logger.Named("scylla"))
		},
		secretsStore,
		logger,
	)
	if err != nil {
		t.Fatal(err)
	}

	assertEqual := func(t *testing.T, got, golden []NodeStatus) {
		t.Helper()

		opts := cmp.Options{
			UUIDComparer(),
			cmpopts.IgnoreFields(NodeStatus{}, "HostID", "Status", "CQLRtt", "RESTRtt"),
		}
		if diff := cmp.Diff(got, golden, opts...); diff != "" {
			t.Errorf("Status() = %+v, diff %s", got, diff)
		}
	}

	t.Run("resolve address via Agent NodeInfo endpoint", func(t *testing.T) {
		cid := uuid.MustRandom()

		s.nodeInfoCache[clusterIDHost{ClusterID: cid, Host: "192.168.100.11"}] = &scyllaclient.NodeInfo{
			BroadcastRPCAddress: "127.0.0.1",
			NativeTransportPort: DefaultPort,
		}
		s.nodeInfoCache[clusterIDHost{ClusterID: cid, Host: "192.168.100.12"}] = &scyllaclient.NodeInfo{
			BroadcastRPCAddress: "192.168.100.12",
			NativeTransportPort: "1",
		}
		s.nodeInfoCache[clusterIDHost{ClusterID: cid, Host: "192.168.100.13"}] = &scyllaclient.NodeInfo{
			BroadcastRPCAddress: "400.400.400.400",
			NativeTransportPort: DefaultPort,
		}

		status, err := s.Status(context.Background(), cid)
		if err != nil {
			t.Error(err)
			return
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.12", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		assertEqual(t, status, golden)
	})

	t.Run("all nodes UP", func(t *testing.T) {
		status, err := s.Status(context.Background(), uuid.MustRandom())
		if err != nil {
			t.Error(err)
			return
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.12", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		assertEqual(t, status, golden)
	})

	t.Run("node REST TIMEOUT", func(t *testing.T) {
		host := "192.168.100.12"
		blockREST(t, host)
		defer unblockREST(t, host)

		status, err := s.Status(context.Background(), uuid.MustRandom())
		if err != nil {
			t.Error(err)
			return
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.12", CQLStatus: "UP", RESTStatus: "TIMEOUT"},
			{Datacenter: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		assertEqual(t, status, golden)
	})

	t.Run("node CQL TIMEOUT", func(t *testing.T) {
		host := "192.168.100.12"
		blockCQL(t, host)
		defer unblockCQL(t, host)

		status, err := s.Status(context.Background(), uuid.MustRandom())
		if err != nil {
			t.Error(err)
			return
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.12", CQLStatus: "TIMEOUT", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		assertEqual(t, status, golden)
	})

	t.Run("node REST DOWN", func(t *testing.T) {
		host := "192.168.100.12"
		stopAgent(t, host)
		defer startAgent(t, host)

		status, err := s.Status(context.Background(), uuid.MustRandom())
		if err != nil {
			t.Error(err)
			return
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.12", CQLStatus: "UP", RESTStatus: "DOWN"},
			{Datacenter: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		assertEqual(t, status, golden)
	})

	t.Run("node REST UNAUTHORIZED", func(t *testing.T) {
		hrt.SetInterceptor(fakeHealthCheckStatus("192.168.100.12", http.StatusUnauthorized))
		defer hrt.SetInterceptor(nil)

		status, err := s.Status(context.Background(), uuid.MustRandom())
		if err != nil {
			t.Error(err)
			return
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.12", CQLStatus: "UP", RESTStatus: "UNAUTHORIZED"},
			{Datacenter: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		assertEqual(t, status, golden)
	})

	t.Run("node REST ERROR", func(t *testing.T) {
		hrt.SetInterceptor(fakeHealthCheckStatus("192.168.100.12", http.StatusBadGateway))
		defer hrt.SetInterceptor(nil)

		status, err := s.Status(context.Background(), uuid.MustRandom())
		if err != nil {
			t.Error(err)
			return
		}

		golden := []NodeStatus{
			{Datacenter: "dc1", Host: "192.168.100.11", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc1", Host: "192.168.100.12", CQLStatus: "UP", RESTStatus: "HTTP 502"},
			{Datacenter: "dc1", Host: "192.168.100.13", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.21", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.22", CQLStatus: "UP", RESTStatus: "UP"},
			{Datacenter: "dc2", Host: "192.168.100.23", CQLStatus: "UP", RESTStatus: "UP"},
		}
		assertEqual(t, status, golden)
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
		if _, err := s.Status(ctx, uuid.MustRandom()); err != nil {
			msg = err.Error()
		}
		if !strings.HasPrefix(msg, "status: giving up after") {
			t.Errorf("Status() error = %s, expected ~ status: giving up after", msg)
		}
	})

	t.Run("context timeout exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()

		_, err := s.Status(ctx, uuid.MustRandom())
		if err == nil {
			t.Error("Expected error got nil")
		}
	})
}

func blockREST(t *testing.T, h string) {
	t.Helper()
	if err := block(h, CmdBlockScyllaREST); err != nil {
		t.Error(err)
	}
}

func unblockREST(t *testing.T, h string) {
	t.Helper()
	if err := unblock(h, CmdUnblockScyllaREST); err != nil {
		t.Error(err)
	}
}

func blockCQL(t *testing.T, h string) {
	t.Helper()
	if err := block(h, CmdBlockScyllaCQL); err != nil {
		t.Error(err)
	}
}

func unblockCQL(t *testing.T, h string) {
	t.Helper()
	if err := unblock(h, CmdUnblockScyllaCQL); err != nil {
		t.Error(err)
	}
}

func block(h, cmd string) error {
	stdout, stderr, err := ExecOnHost(h, cmd)
	if err != nil {
		return errors.Wrapf(err, "block failed host: %s, stdout %s, stderr %s", h, stdout, stderr)
	}
	return nil
}

func unblock(h, cmd string) error {
	stdout, stderr, err := ExecOnHost(h, cmd)
	if err != nil {
		return errors.Wrapf(err, "unblock failed host: %s, stdout %s, stderr %s", h, stdout, stderr)
	}
	return nil
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

func fakeHealthCheckStatus(host string, code int) http.RoundTripper {
	return httpx.RoundTripperFunc(func(r *http.Request) (*http.Response, error) {
		h, _, err := net.SplitHostPort(r.URL.Host)
		if err != nil {
			return nil, err
		}
		if h == host && r.Method == http.MethodGet && r.URL.Path == "/" {
			resp := &http.Response{
				Status:     http.StatusText(code),
				StatusCode: code,
				Proto:      "HTTP/1.1",
				ProtoMajor: 1,
				ProtoMinor: 1,
				Request:    r,
				Header:     make(http.Header, 0),
				Body:       ioutil.NopCloser(bytes.NewReader([]byte{})),
			}
			return resp, nil
		}
		return nil, nil
	})
}
