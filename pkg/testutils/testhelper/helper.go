// Copyright (C) 2023 ScyllaDB

package testhelper

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/config/server"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// CommonTestHelper common tester object for backups and repairs.
type CommonTestHelper struct {
	Logger  log.Logger
	Session gocqlx.Session
	Hrt     *HackableRoundTripper
	Client  *scyllaclient.Client

	ClusterID uuid.UUID
	TaskID    uuid.UUID
	RunID     uuid.UUID
	T         *testing.T
}

// GetHostsFromDC returns list of hosts on the scylla cluster on the given DC.
func (h *CommonTestHelper) GetHostsFromDC(dcName string) []string {
	info, err := h.Client.Datacenters(context.Background())
	if err != nil {
		h.T.Fatal(err)
	}
	return info[dcName]
}

// GetAllHosts returns list of hosts on the scylla cluster across all DC available.
func (h *CommonTestHelper) GetAllHosts() []string {
	info, err := h.Client.Datacenters(context.Background())
	if err != nil {
		h.T.Fatal(err)
	}
	var out []string
	for _, dcList := range info {
		out = append(out, dcList...)
	}
	return out
}

// StopNode via supervisorctl.
func (h *CommonTestHelper) StopNode(host string) {
	h.T.Helper()

	_, _, err := ExecOnHost(host, "supervisorctl stop scylla")
	if err != nil {
		h.T.Fatal(err)
	}
}

// StartNode via supervisorctl and wait for it to become available.
func (h *CommonTestHelper) StartNode(host string, ni *scyllaclient.NodeInfo) {
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
		if _, err = cqlping.QueryPing(context.Background(), cfg, testconfig.TestDBUsername(), testconfig.TestDBPassword()); err != nil {
			return false
		}
		for _, other := range testconfig.ManagedClusterHosts() {
			status, err := h.Client.Status(scyllaclient.ClientContextWithSelectedHost(context.Background(), other))
			if err != nil || len(status.Live()) != len(testconfig.ManagedClusterHosts()) {
				return false
			}
		}
		return true
	}
	WaitCond(h.T, cond, time.Second, time.Minute)
}

// RestartAgents via supervisorctl.
func (h *CommonTestHelper) RestartAgents() {
	execOnAllHosts(h, "supervisorctl restart scylla-manager-agent")
}

// RestartScylla performs a rolling restart of a cluster.
func (h *CommonTestHelper) RestartScylla() {
	h.T.Helper()
	Print("When: restart cluster")

	ctx := context.Background()
	cfg := cqlping.Config{Timeout: 100 * time.Millisecond}
	const cmdRestart = "supervisorctl restart scylla"

	for _, host := range h.GetAllHosts() {
		Print("When: restart Scylla on host: " + host)
		stdout, stderr, err := ExecOnHost(host, cmdRestart)
		if err != nil {
			h.T.Log("stdout", stdout)
			h.T.Log("stderr", stderr)
			h.T.Fatal("Command failed on host", host, err)
		}

		var sessionHosts []string
		b := backoff.WithContext(backoff.WithMaxRetries(
			backoff.NewConstantBackOff(500*time.Millisecond), 10), ctx)
		if err := backoff.Retry(func() error {
			sessionHosts, err = cluster.GetRPCAddresses(ctx, h.Client, []string{host}, false)
			return err
		}, b); err != nil {
			h.T.Fatal(err)
		}

		cfg.Addr = sessionHosts[0]
		if testconfig.IsSSLEnabled() {
			sslOpts := testconfig.CQLSSLOptions()
			cfg.TLSConfig, err = testconfig.TLSConfig(sslOpts)
			if err != nil {
				h.T.Fatalf("tls config: %v", err)
			}
		}
		cond := func() bool {
			if _, err = cqlping.QueryPing(ctx, cfg, testconfig.TestDBUsername(), testconfig.TestDBPassword()); err != nil {
				return false
			}
			status, err := h.Client.Status(ctx)
			if err != nil {
				return false
			}
			return len(status.Live()) == 6
		}

		WaitCond(h.T, cond, time.Second, 60*time.Second)
		Print("Then: Scylla is restarted on host: " + host)
	}

	Print("Then: cluster is restarted")
}

func execOnAllHosts(h *CommonTestHelper, cmd string) {
	h.T.Helper()
	for _, host := range h.GetAllHosts() {
		stdout, stderr, err := ExecOnHost(host, cmd)
		if err != nil {
			h.T.Log("stdout", stdout)
			h.T.Log("stderr", stderr)
			h.T.Fatal("Command failed on host", host, err)
		}
	}
}

// NewTestConfigCacheSvc creates default config cache service which can be used
// for testing other services relaying on it.
func NewTestConfigCacheSvc(t *testing.T, clusterID uuid.UUID, hosts []string) configcache.ConfigCacher {
	t.Helper()

	session := CreateScyllaManagerDBSession(t)
	secretsStore := store.NewTableStore(session, table.Secrets)

	clusterSvc, err := cluster.NewService(session, metrics.NewClusterMetrics(), secretsStore,
		scyllaclient.DefaultTimeoutConfig(), server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	err = clusterSvc.PutCluster(t.Context(), ValidCluster(t, clusterID, hosts[0]))
	if err != nil {
		t.Fatal(err)
	}

	scyllaClientProvider := func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
		sc := scyllaclient.TestConfig(hosts, AgentAuthToken())
		sc.Timeout = time.Second
		sc.Transport = NewHackableRoundTripper(scyllaclient.DefaultTransport())
		return scyllaclient.NewClient(sc, log.NewDevelopment())
	}

	svc := configcache.NewService(configcache.DefaultConfig(), clusterSvc, scyllaClientProvider, secretsStore, log.NewDevelopment())
	svc.Init(t.Context())
	return svc
}

// ValidCluster return Cluster initialized according to test configuration.
func ValidCluster(t *testing.T, id uuid.UUID, host string) *cluster.Cluster {
	t.Helper()

	c := &cluster.Cluster{
		ID:        id,
		Name:      "name_" + id.String(),
		Host:      host,
		Port:      10001,
		AuthToken: AgentAuthToken(),
		Username:  testconfig.TestDBUsername(),
		Password:  testconfig.TestDBPassword(),
	}

	if testconfig.IsSSLEnabled() {
		sslOpts := testconfig.CQLSSLOptions()
		userKey, err := os.ReadFile(sslOpts.KeyPath)
		if err != nil {
			t.Fatalf("read file (%s) err: %v", sslOpts.KeyPath, err)
		}
		userCrt, err := os.ReadFile(sslOpts.CertPath)
		if err != nil {
			t.Fatalf("read file (%s) err: %v", sslOpts.CertPath, err)
		}
		c.SSLUserKeyFile = userKey
		c.SSLUserCertFile = userCrt
	}

	return c
}
