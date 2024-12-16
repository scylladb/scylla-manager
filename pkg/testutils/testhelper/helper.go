// Copyright (C) 2023 ScyllaDB

package testhelper

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/config/server"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
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

// RestartAgents via supervisorctl.
func (h *CommonTestHelper) RestartAgents() {
	execOnAllHosts(h, "supervisorctl restart scylla-manager-agent")
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
	err = clusterSvc.PutCluster(context.Background(), ValidCluster(t, clusterID, hosts[0]))
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
	svc.Init(context.Background())
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
