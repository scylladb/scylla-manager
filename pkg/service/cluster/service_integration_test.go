// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package cluster_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/config/server"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestValidateHostConnectivityIntegration(t *testing.T) {
	if IsIPV6Network() {
		t.Skip("DB node do not have ip6tables and related modules to make it work properly")
	}

	Print("given: the fresh cluster")
	var (
		ctx          = context.Background()
		session      = CreateScyllaManagerDBSession(t)
		secretsStore = store.NewTableStore(session, table.Secrets)
		c            = &cluster.Cluster{
			AuthToken: "token",
			Host:      ManagedClusterHost(),
		}
	)
	s, err := cluster.NewService(session, metrics.NewClusterMetrics(), secretsStore, scyllaclient.DefaultTimeoutConfig(),
		server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	err = s.PutCluster(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	allHosts := ManagedClusterHosts()
	for _, tc := range []struct {
		name      string
		hostsDown []string
		result    error
		timeout   time.Duration
	}{
		{
			name:      "coordinator host is DOWN",
			hostsDown: []string{ManagedClusterHost()},
			result:    nil,
			timeout:   6 * time.Second,
		},
		{
			name:      "only one is UP",
			hostsDown: allHosts[:len(allHosts)-1],
			result:    nil,
			timeout:   6 * time.Second,
		},
		{
			name:      "all hosts are DOWN",
			hostsDown: allHosts,
			result:    cluster.ErrNoValidKnownHost,
			timeout:   11 * time.Second, // the 5 seconds calls will timeout twice
		},
		{
			name:      "all hosts are UP",
			hostsDown: nil,
			result:    nil,
			timeout:   6 * time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				for _, host := range tc.hostsDown {
					if err := StartService(host, "scylla"); err != nil {
						t.Logf("error on starting stopped scylla service on host={%s}, err={%s}", host, err)
					}
					if err := RunIptablesCommand(t, host, CmdUnblockScyllaREST); err != nil {
						t.Logf("error trying to unblock REST API on host = {%s}, err={%s}", host, err)
					}
				}
			}()
			TryUnblockCQL(t, ManagedClusterHosts())
			TryUnblockREST(t, ManagedClusterHosts())
			TryUnblockAlternator(t, ManagedClusterHosts())
			TryStartAgent(t, ManagedClusterHosts())
			if err := EnsureNodesAreUP(t, ManagedClusterHosts(), time.Minute); err != nil {
				t.Fatalf("not all nodes are UP, err = {%v}", err)
			}

			Printf("then: validate that call to validate host connectivity takes less than %v seconds", tc.timeout.Seconds())
			testCluster, err := s.GetClusterByID(context.Background(), c.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := callValidateHostConnectivityWithTimeout(ctx, s, tc.timeout, testCluster); err != nil {
				t.Fatal(err)
			}
			Printf("when: the scylla service is stopped and the scylla API is timing out on some hosts")
			// It's needed to block Scylla REST API, so that the clients are just hanging when they call the API.
			// Scylla service must be stopped to make the node to report DOWN status. Blocking REST API is not
			// enough.
			for _, host := range tc.hostsDown {
				if err := StopService(host, "scylla"); err != nil {
					t.Fatal(err)
				}
				if err := RunIptablesCommand(t, host, CmdBlockScyllaREST); err != nil {
					t.Error(err)
				}
			}

			Printf("then: validate that call still takes less than %v seconds", tc.timeout.Seconds())
			if err := callValidateHostConnectivityWithTimeout(ctx, s, tc.timeout, testCluster); !errors.Is(err, tc.result) {
				t.Fatal(err)
			}
		})
	}
}

func callValidateHostConnectivityWithTimeout(ctx context.Context, s *cluster.Service, timeout time.Duration,
	c *cluster.Cluster) error {

	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error)
	go func() {
		done <- s.ValidateHostsConnectivity(callCtx, c)
	}()

	select {
	case <-time.After(timeout):
		cancel()
		return fmt.Errorf("expected s.ValidateHostsConnectivity to complete in less than %v seconds, time exceeded", timeout.Seconds())
	case err := <-done:
		return err
	}
}

func TestClientIntegration(t *testing.T) {
	expectedHosts := ManagedClusterHosts()

	session := CreateScyllaManagerDBSession(t)
	secretsStore := store.NewTableStore(session, table.Secrets)
	s, err := cluster.NewService(session, metrics.NewClusterMetrics(), secretsStore, scyllaclient.DefaultTimeoutConfig(),
		server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	c := &cluster.Cluster{
		AuthToken: "token",
		Host:      ManagedClusterHost(),
	}
	err = s.PutCluster(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.GetClusterByID(context.Background(), c.ID)
	if err != nil {
		t.Fatal(err)
	}

	// #1
	// update cluster.known_hosts to put non-existing IPs on top of existing one
	// the expectation is that client will finally manage to clean it up
	// and will update known_hosts to its proper values
	fakeHostWithOnlyOneCorrect := []string{"192.168.10.1", "192.168.10.2", c.KnownHosts[0]}
	c.KnownHosts = fakeHostWithOnlyOneCorrect
	q := table.Cluster.UpdateQuery(session, "known_hosts").BindStruct(c)
	if err := q.ExecRelease(); err != nil {
		t.Fatal(err)
	}

	_, err = s.Client(context.Background(), c.ID)
	if err != nil {
		t.Fatal("Cannot create Scylla API client", err)
	}
	c, err = s.GetClusterByID(context.Background(), c.ID)
	if err != nil {
		t.Fatal(err)
	}

	// assert that all nodes are available on cluster.known_hosts
	diff := ipsNotInSlice(c.KnownHosts, expectedHosts)
	if len(diff) > 0 {
		t.Fatalf("Not all expected elements are available on cluster knownHosts, current = {%v}, expected = {%v}",
			c.KnownHosts, expectedHosts)
	}
}

func ipsNotInSlice(a []string, b []string) []string {
	m := make(map[string]struct{})
	for _, elem := range a {
		m[net.ParseIP(elem).String()] = struct{}{}
	}

	var diff []string
	for _, elem := range b {
		_, ok := m[net.ParseIP(elem).String()]
		if !ok {
			diff = append(diff, elem)
		}
	}

	return diff
}

func TestAlternatorClientIntegration(t *testing.T) {
	smSession := CreateScyllaManagerDBSession(t)
	defer smSession.Close()

	secretsStore := store.NewTableStore(smSession, table.Secrets)
	s, err := cluster.NewService(smSession, metrics.NewClusterMetrics(), secretsStore, scyllaclient.DefaultTimeoutConfig(),
		server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	c := &cluster.Cluster{
		AuthToken: "token",
		Host:      ManagedClusterHost(),
	}
	if err = s.PutCluster(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	scClient, err := s.CreateClientNoCache(context.Background(), c.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer scClient.Close()

	clusterSession := CreateManagedClusterSession(t, false, scClient, "", "")
	defer clusterSession.Close()

	c.AlternatorAccessKeyID, c.AlternatorSecretAccessKey = GetAlternatorCreds(t, clusterSession, "")
	if err = s.PutCluster(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	client, err := s.GetAlternatorClient(context.Background(), c.ID, ManagedClusterHost())
	if err != nil {
		t.Fatal(err)
	}

	const tableName = ".scylla.alternator.system_schema.tables"
	_, err = client.Scan(context.Background(), &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		Limit:     aws.Int32(1),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestServiceStorageIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)

	secretsStore := store.NewTableStore(session, table.Secrets)

	cfg := scyllaclient.DefaultTimeoutConfig()
	cfg.Timeout = 2 * time.Second
	cfg.Backoff.WaitMax = 2 * time.Second
	cfg.Backoff.MaxRetries = 1
	s, err := cluster.NewService(session, metrics.NewClusterMetrics(), secretsStore, cfg,
		server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	var change cluster.Change
	s.SetOnChangeListener(func(ctx context.Context, c cluster.Change) error {
		change = c
		return nil
	})

	setup := func(t *testing.T) {
		t.Helper()
		ExecStmt(t, session, "TRUNCATE cluster")
	}

	ctx := context.Background()

	diffOpts := cmp.Options{
		UUIDComparer(),
		cmpopts.IgnoreFields(cluster.Cluster{}, "Host"),
		cmpopts.IgnoreFields(cluster.Cluster{}, "KnownHosts"),
	}

	t.Run("list empty", func(t *testing.T) {
		setup(t)

		clusters, err := s.ListClusters(ctx, &cluster.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(clusters) != 0 {
			t.Fatal("expected 0 len result")
		}
	})

	t.Run("list not empty", func(t *testing.T) {
		setup(t)

		expected := make([]*cluster.Cluster, 3)
		for i := range expected {
			c := &cluster.Cluster{
				ID:        uuid.NewTime(),
				Name:      "name" + strconv.Itoa(i),
				Host:      ManagedClusterHost(),
				AuthToken: AgentAuthToken(),
			}
			if err := s.PutCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			expected[i] = c
		}

		clusters, err := s.ListClusters(ctx, &cluster.Filter{})
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(clusters, expected, diffOpts...); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get missing cluster", func(t *testing.T) {
		setup(t)

		c, err := s.GetClusterByID(ctx, uuid.MustRandom())
		if !errors.Is(err, util.ErrNotFound) {
			t.Fatal("expected not found")
		}
		if c != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("get cluster", func(t *testing.T) {
		setup(t)

		c0 := validCluster()
		c0.ID = uuid.Nil

		if err := s.PutCluster(ctx, c0); err != nil {
			t.Fatal(err)
		}
		if c0.ID == uuid.Nil {
			t.Fatal("ID not updated")
		}
		c1, err := s.GetClusterByID(ctx, c0.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(c0, c1, diffOpts...); diff != "" {
			t.Fatal("read write mismatch", diff)
		}

		c2, err := s.GetClusterByName(ctx, c0.Name)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(c0, c2, diffOpts...); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("get cluster name", func(t *testing.T) {
		setup(t)

		c := validCluster()
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		clusterName, err := s.GetClusterName(ctx, c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if clusterName != c.Name {
			t.Fatal("expected", c.Name, "got", clusterName)
		}
	})

	t.Run("put nil cluster", func(t *testing.T) {
		setup(t)

		if err := s.PutCluster(ctx, nil); err == nil {
			t.Fatal("expected validation error")
		} else {
			t.Log(err)
		}
	})

	t.Run("put conflicting cluster name", func(t *testing.T) {
		setup(t)

		c0 := validCluster()

		if err := s.PutCluster(ctx, c0); err != nil {
			t.Fatal(err)
		}

		c1 := c0
		c1.ID = uuid.Nil

		if err := s.PutCluster(ctx, c0); err == nil {
			t.Fatal("expected validation error")
		} else {
			t.Log(err)
		}
	})

	t.Run("put cluster with wrong auth token", func(t *testing.T) {
		setup(t)

		c := validCluster()
		c.AuthToken = "foobar"

		if err := s.PutCluster(ctx, c); err == nil {
			t.Fatal("expected validation error")
		} else {
			t.Log(err)
		}
	})

	t.Run("put new cluster", func(t *testing.T) {
		setup(t)

		c := validCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if c.ID == uuid.Nil {
			t.Fatal("id not set")
		}
		if change.ID != c.ID {
			t.Fatal("id mismatch")
		}
		if change.Type != cluster.Create {
			t.Fatal("invalid type", change)
		}
	})

	assertSecrets := func(t *testing.T, c *cluster.Cluster) {
		t.Helper()

		cqlCreds := &secrets.CQLCreds{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(cqlCreds); err != nil {
			t.Fatal(err)
		}
		alternatorCreds := &secrets.AlternatorCreds{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(alternatorCreds); err != nil {
			t.Fatal(err)
		}
		tlsIdentity := &secrets.TLSIdentity{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(tlsIdentity); err != nil {
			t.Fatal(err)
		}

		goldenCqlCreds, goldenAlternatorCreds, goldenTlsIdentity := goldenSecrets(c)
		if diff := cmp.Diff(goldenCqlCreds, cqlCreds, diffOpts...); diff != "" {
			t.Error("Invalid CQL creds, diff", diff)
		}
		if diff := cmp.Diff(goldenAlternatorCreds, alternatorCreds, diffOpts...); diff != "" {
			t.Error("Invalid alternator creds, diff", diff)
		}
		if diff := cmp.Diff(goldenTlsIdentity, tlsIdentity, diffOpts...); diff != "" {
			t.Error("Invalid TLS identity, diff", diff)
		}
	}

	t.Run("put new cluster with secrets", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		assertSecrets(t, c)
	})

	t.Run("update cluster with secrets", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		c.SSLUserCertFile, err = os.ReadFile("testdata/cluster_update.crt")
		if err != nil {
			t.Fatal(err)
		}
		c.SSLUserKeyFile, err = os.ReadFile("testdata/cluster_update.key")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		assertSecrets(t, c)
	})

	t.Run("check existing CQL credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		ok, err := s.CheckCQLCredentials(c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected true")
		}
	})

	t.Run("check non-existing CQL credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil
		c.Username = ""
		c.Password = ""

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		ok, err := s.CheckCQLCredentials(c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("expected false")
		}
	})

	t.Run("check existing alternator credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		ok, err := s.CheckAlternatorCredentials(c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected true")
		}
	})

	t.Run("check non-existing alternator credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil
		c.AlternatorAccessKeyID = ""
		c.AlternatorSecretAccessKey = ""

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		ok, err := s.CheckAlternatorCredentials(c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("expected false")
		}
	})

	t.Run("delete cluster removes secrets", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteCluster(ctx, c.ID); err != nil {
			t.Fatal(err)
		}

		cqlCreds := &secrets.CQLCreds{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(cqlCreds); !errors.Is(err, util.ErrNotFound) {
			t.Fatal(err)
		}
		tlsIdentity := &secrets.TLSIdentity{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(tlsIdentity); !errors.Is(err, util.ErrNotFound) {
			t.Fatal(err)
		}
		alternatorCreds := &secrets.AlternatorCreds{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(alternatorCreds); !errors.Is(err, util.ErrNotFound) {
			t.Fatal(err)
		}
	})

	t.Run("delete CQL credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteCQLCredentials(ctx, c.ID); err != nil {
			t.Fatal(err)
		}

		cqlCreds := &secrets.CQLCreds{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(cqlCreds); !errors.Is(err, util.ErrNotFound) {
			t.Fatal(err)
		}
	})

	t.Run("delete alternator credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteAlternatorCredentials(ctx, c.ID); err != nil {
			t.Fatal(err)
		}

		alternatorCreds := &secrets.AlternatorCreds{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(alternatorCreds); !errors.Is(err, util.ErrNotFound) {
			t.Fatal(err)
		}
	})

	t.Run("delete SSL cert", func(t *testing.T) {
		setup(t)

		c := tlsCluster()
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteSSLUserCert(ctx, c.ID); err != nil {
			t.Fatal(err)
		}

		tlsIdentity := &secrets.TLSIdentity{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(tlsIdentity); !errors.Is(err, util.ErrNotFound) {
			t.Fatal(err)
		}
	})

	t.Run("put new cluster without automatic repair", func(t *testing.T) {
		setup(t)

		c := validCluster()
		c.ID = uuid.Nil
		c.WithoutRepair = true

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if !change.WithoutRepair {
			t.Fatal("automatic repair scheduling not skipped")
		}
	})

	t.Run("put existing cluster", func(t *testing.T) {
		setup(t)

		c := validCluster()
		// Given cluster
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if change.ID != c.ID {
			t.Fatal("id mismatch")
		}
		if change.Type != cluster.Create {
			t.Fatal("invalid type", change)
		}

		// Then PutCluster with same data results in Update
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if change.ID != c.ID {
			t.Fatal("id mismatch")
		}
		if change.Type != cluster.Update {
			t.Fatal("invalid type", change)
		}
	})

	t.Run("delete missing cluster", func(t *testing.T) {
		setup(t)

		id := uuid.MustRandom()

		err := s.DeleteCluster(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("delete cluster", func(t *testing.T) {
		setup(t)

		c := validCluster()
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteCluster(ctx, c.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetClusterByID(ctx, c.ID); !errors.Is(err, util.ErrNotFound) {
			t.Fatal(err)
		}
		if change.ID != c.ID {
			t.Fatal("id mismatch")
		}
		if change.Type != cluster.Delete {
			t.Fatal("invalid type", change)
		}
	})

	t.Run("failed connectivity check", func(t *testing.T) {
		if IsIPV6Network() {
			t.Skip("DB node do not have ip6tables and related modules to make it work properly")
		}

		setup(t)
		hosts := ManagedClusterHosts()
		if len(hosts) < 2 {
			t.Skip("not enough nodes in the cluster")
		}
		h1 := hosts[0]
		h2 := hosts[1]

		c := validCluster()
		c.Host = h1
		if err := RunIptablesCommand(t, h2, CmdBlockScyllaREST); err != nil {
			t.Fatal(err)
		}
		defer RunIptablesCommand(t, h2, CmdUnblockScyllaREST)

		if err := s.PutCluster(ctx, c); err == nil {
			t.Fatal("expected put cluster to fail because of connectivity issues")
		} else {
			t.Logf("put cluster ended with expected error: %s", err)
		}

		clusters, err := s.ListClusters(ctx, &cluster.Filter{})
		if err != nil {
			t.Fatalf("list clusters: %s", err)
		}
		if len(clusters) != 0 {
			t.Fatalf("expected no clusters to be listed, got: %v", clusters)
		}

		var cnt int
		if err := session.Query("SELECT COUNT(*) FROM cluster", nil).GetRelease(&cnt); err != nil {
			t.Fatalf("check SM DB cluster table entries: %s", err)
		}
		if cnt != 0 {
			t.Fatalf("expected no entries in SM DB cluster table, got: %d", cnt)
		}
	})

	t.Run("no --host in SM DB", func(t *testing.T) {
		setup(t)
		c := validCluster()
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		if err := session.Query(fmt.Sprintf("UPDATE cluster SET host = '' WHERE id = %s", c.ID), nil).ExecRelease(); err != nil {
			t.Fatalf("remove --host from SM DB: %s", err)
		}

		client, err := s.CreateClientNoCache(context.Background(), c.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range ManagedClusterHosts() {
			if _, err := client.HostRack(ctx, h); err != nil {
				t.Fatalf("test client by getting rack of host %s: %s", h, err)
			}
		}
	})

	t.Run("list nodes", func(t *testing.T) {
		setup(t)

		c := &cluster.Cluster{
			ID:        uuid.NewTime(),
			Name:      "clust1",
			Host:      ManagedClusterHost(),
			AuthToken: AgentAuthToken(),
		}
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		got, err := s.ListNodes(ctx, c.ID)
		if err != nil {
			t.Fatal(err)
		}

		for id := range got {
			got[id].Address = ToCanonicalIP(got[id].Address)
		}

		expected := []cluster.Node{
			{
				Datacenter:        "dc1",
				Address:           ToCanonicalIP(IPFromTestNet("11")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("11")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc1",
				Address:           ToCanonicalIP(IPFromTestNet("12")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("12")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc1",
				Address:           ToCanonicalIP(IPFromTestNet("13")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("13")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc2",
				Address:           ToCanonicalIP(IPFromTestNet("21")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("21")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc2",
				Address:           ToCanonicalIP(IPFromTestNet("22")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("22")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc2",
				Address:           ToCanonicalIP(IPFromTestNet("23")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("23")),
				PrometheusPort:    9180,
			},
		}

		opts := append(diffOpts, cmpopts.SortSlices(func(x, y cluster.Node) bool {
			if x.Datacenter > y.Datacenter {
				return false
			}
			if x.Address > y.Address {
				return false
			}
			return true
		}))

		if diff := cmp.Diff(expected, got, opts...); diff != "" {
			t.Fatal(diff)
		}
	})
}

func validCluster() *cluster.Cluster {
	return &cluster.Cluster{
		ID:        uuid.MustRandom(),
		Name:      "name_" + uuid.MustRandom().String(),
		Host:      ManagedClusterHost(),
		Port:      10001,
		AuthToken: AgentAuthToken(),
	}
}

var (
	tlsCert []byte
	tlsKey  []byte
)

func init() {
	var err error
	tlsCert, err = os.ReadFile("testdata/cluster.crt")
	if err != nil {
		panic(err)
	}
	tlsKey, err = os.ReadFile("testdata/cluster.key")
	if err != nil {
		panic(err)
	}
}

func tlsCluster() *cluster.Cluster {
	c := validCluster()
	c.Username = "user"
	c.Password = "password"
	c.AlternatorAccessKeyID = "id"
	c.AlternatorSecretAccessKey = "key"
	c.SSLUserCertFile = tlsCert
	c.SSLUserKeyFile = tlsKey
	return c
}

func goldenSecrets(c *cluster.Cluster) (*secrets.CQLCreds, *secrets.AlternatorCreds, *secrets.TLSIdentity) {
	return &secrets.CQLCreds{
			ClusterID: c.ID,
			Username:  c.Username,
			Password:  c.Password,
		}, &secrets.AlternatorCreds{
			ClusterID:       c.ID,
			AccessKeyID:     c.AlternatorAccessKeyID,
			SecretAccessKey: c.AlternatorSecretAccessKey,
		}, &secrets.TLSIdentity{
			ClusterID:  c.ID,
			Cert:       c.SSLUserCertFile,
			PrivateKey: c.SSLUserKeyFile,
		}
}
