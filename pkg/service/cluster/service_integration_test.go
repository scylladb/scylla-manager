// Copyright (C) 2017 ScyllaDB

// +build all integration

package cluster_test

import (
	"context"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	"github.com/scylladb/scylla-manager/pkg/secrets"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/pkg/store"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func TestServiceStorageIntegration(t *testing.T) {
	session := CreateSession(t)

	secretsStore := store.NewTableStore(session, table.Secrets)

	s, err := cluster.NewService(session, secretsStore, log.NewDevelopment())
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
		if !errors.Is(err, service.ErrNotFound) {
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
		tlsIdentity := &secrets.TLSIdentity{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(tlsIdentity); err != nil {
			t.Fatal(err)
		}

		goldenCqlCreds, goldenTlsIdentity := goldenSecrets(c)
		if diff := cmp.Diff(goldenCqlCreds, cqlCreds, diffOpts...); diff != "" {
			t.Error("Invalid CQL creds, diff", diff)
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

		c.SSLUserCertFile, err = ioutil.ReadFile("testdata/cluster_update.crt")
		if err != nil {
			t.Fatal(err)
		}
		c.SSLUserKeyFile, err = ioutil.ReadFile("testdata/cluster_update.key")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		assertSecrets(t, c)
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
		if err := secretsStore.Get(cqlCreds); !errors.Is(err, service.ErrNotFound) {
			t.Fatal(err)
		}
		tlsIdentity := &secrets.TLSIdentity{
			ClusterID: c.ID,
		}
		if err := secretsStore.Get(tlsIdentity); !errors.Is(err, service.ErrNotFound) {
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
		if err := secretsStore.Get(cqlCreds); !errors.Is(err, service.ErrNotFound) {
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
		if err := secretsStore.Get(tlsIdentity); !errors.Is(err, service.ErrNotFound) {
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
		if _, err := s.GetClusterByID(ctx, c.ID); !errors.Is(err, service.ErrNotFound) {
			t.Fatal(err)
		}
		if change.ID != c.ID {
			t.Fatal("id mismatch")
		}
		if change.Type != cluster.Delete {
			t.Fatal("invalid type", change)
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
		expected := []cluster.Node{
			{
				"dc1",
				"192.168.100.11",
				2,
			},
			{
				"dc1",
				"192.168.100.12",
				2,
			},
			{
				"dc1",
				"192.168.100.13",
				2,
			},
			{
				"dc2",
				"192.168.100.21",
				2,
			},
			{
				"dc2",
				"192.168.100.22",
				2,
			},
			{
				"dc2",
				"192.168.100.23",
				2,
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
		AuthToken: AgentAuthToken(),
	}
}

var (
	tlsCert []byte
	tlsKey  []byte
)

func init() {
	var err error
	tlsCert, err = ioutil.ReadFile("testdata/cluster.crt")
	if err != nil {
		panic(err)
	}
	tlsKey, err = ioutil.ReadFile("testdata/cluster.key")
	if err != nil {
		panic(err)
	}
}

func tlsCluster() *cluster.Cluster {
	c := validCluster()
	c.Username = "user"
	c.Password = "password"
	c.SSLUserCertFile = tlsCert
	c.SSLUserKeyFile = tlsKey
	return c
}

func goldenSecrets(c *cluster.Cluster) (*secrets.CQLCreds, *secrets.TLSIdentity) {
	return &secrets.CQLCreds{
			ClusterID: c.ID,
			Username:  c.Username,
			Password:  c.Password,
		}, &secrets.TLSIdentity{
			ClusterID:  c.ID,
			Cert:       c.SSLUserCertFile,
			PrivateKey: c.SSLUserKeyFile,
		}
}
