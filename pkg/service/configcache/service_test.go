// Copyright (C) 2024 ScyllaDB

package configcache

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Servicer.ForceUpdateCluster() and Servicer.Init(ctx context.Context) are expected to be covered with the
// integration tests.

func TestService_Read(t *testing.T) {
	emptyConfigHash, err := NodeConfig{}.sha256hash()
	if err != nil {
		t.Fatalf("unable to create sha256 hash out of empty NodeConfig, err = {%v}", err)
	}
	host1NodeConfig := NodeConfig{
		NodeInfo: &scyllaclient.NodeInfo{
			AgentVersion: "expectedVersion",
		},
	}
	host1ConfigHash, err := host1NodeConfig.sha256hash()
	if err != nil {
		t.Fatal(err)
	}

	cluster1UUID := uuid.MustRandom()
	host1ID := "host1"
	initialState := convertMapToSyncMap(
		map[any]any{
			cluster1UUID.String(): convertMapToSyncMap(
				map[any]any{
					host1ID: host1NodeConfig,
				},
			),
		},
	)

	for _, tc := range []struct {
		name             string
		cluster          uuid.UUID
		host             string
		state            *sync.Map
		resultErr        error
		resultConfigHash [32]byte
	}{
		{
			name:             "host configuration doesn't exist",
			host:             "host_that_does_not_exist",
			cluster:          cluster1UUID,
			state:            initialState,
			resultErr:        ErrNoHostConfig,
			resultConfigHash: emptyConfigHash,
		},
		{
			name:             "cluster configuration doesn't exist",
			host:             host1ID,
			cluster:          uuid.Nil,
			state:            initialState,
			resultErr:        ErrNoClusterConfig,
			resultConfigHash: emptyConfigHash,
		},
		{
			name:             "retrieves the config",
			host:             host1ID,
			cluster:          cluster1UUID,
			state:            initialState,
			resultErr:        nil,
			resultConfigHash: host1ConfigHash,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			svc := Service{
				svcConfig:    DefaultConfig(),
				clusterSvc:   &mockClusterServicer{},
				scyllaClient: mockProviderFunc,
				secretsStore: &mockStore{},
				configs:      tc.state,
			}

			// When
			conf, err := svc.Read(tc.cluster, tc.host)

			// Then
			if err != tc.resultErr {
				t.Fatalf("expected error = {%v}, but got {%v}", tc.resultErr, err)
			}
			confHash, err := conf.sha256hash()
			if err != nil {
				t.Fatalf("unable to create hash out of NodeConf, err = {%v}", err)
			}
			if confHash != tc.resultConfigHash {
				t.Fatalf("expected hash = {%s}, but got {%s}", tc.resultConfigHash, confHash)
			}
		})
	}
}

func TestService_Run(t *testing.T) {
	t.Run("validate context cancellation handling", func(t *testing.T) {
		svc := Service{
			svcConfig:    DefaultConfig(),
			clusterSvc:   &mockClusterServicer{},
			scyllaClient: mockProviderFunc,
			secretsStore: &mockStore{},
			configs:      &sync.Map{},
		}

		ctx, cancel := context.WithCancel(context.Background())
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			svc.Run(ctx)
		}()

		time.Sleep(3 * time.Second)
		cancel()

		wg.Wait()
	})
}

func (nc NodeConfig) sha256hash() (hash [32]byte, err error) {
	data, err := json.Marshal(nc)
	if err != nil {
		return hash, err
	}

	return sha256.Sum256(data), nil
}

// utility functions
func convertMapToSyncMap(m map[any]any) *sync.Map {
	syncM := sync.Map{}
	for k, v := range m {
		syncM.Store(k, v)
	}
	return &syncM
}

// internal mock implementation of interfaces

// mockClusterServicer implements the Servicer interface as a mock for testing purposes.
type mockClusterServicer struct{}

// ListClusters mocks the ListClusters method of Servicer.
func (s *mockClusterServicer) ListClusters(ctx context.Context, f *cluster.Filter) ([]*cluster.Cluster, error) {
	return nil, nil
}

// GetCluster mocks the GetCluster method of Servicer.
func (s *mockClusterServicer) GetCluster(ctx context.Context, idOrName string) (*cluster.Cluster, error) {
	return nil, nil
}

// PutCluster mocks the PutCluster method of Servicer.
func (s *mockClusterServicer) PutCluster(ctx context.Context, c *cluster.Cluster) error {
	return nil
}

// DeleteCluster mocks the DeleteCluster method of Servicer.
func (s *mockClusterServicer) DeleteCluster(ctx context.Context, id uuid.UUID) error {
	return nil
}

// CheckCQLCredentials mocks the CheckCQLCredentials method of Servicer.
func (s *mockClusterServicer) CheckCQLCredentials(id uuid.UUID) (bool, error) {
	return false, nil
}

// DeleteCQLCredentials mocks the DeleteCQLCredentials method of Servicer.
func (s *mockClusterServicer) DeleteCQLCredentials(ctx context.Context, id uuid.UUID) error {
	return nil
}

// DeleteSSLUserCert mocks the DeleteSSLUserCert method of Servicer.
func (s *mockClusterServicer) DeleteSSLUserCert(ctx context.Context, id uuid.UUID) error {
	return nil
}

// ListNodes mocks the ListNodes method of Servicer.
func (s *mockClusterServicer) ListNodes(ctx context.Context, id uuid.UUID) ([]cluster.Node, error) {
	return nil, nil
}

// mockStore implements the Store interface as a mock for testing purposes.
type mockStore struct{}

// Put mocks the Put method of Store.
func (s *mockStore) Put(v store.Entry) error {
	return nil
}

// Get mocks the Get method of Store.
func (s *mockStore) Get(v store.Entry) error {
	return nil
}

// Check mocks the Check method of Store.
func (s *mockStore) Check(v store.Entry) (bool, error) {
	return false, nil
}

// Delete mocks the Delete method of Store.
func (s *mockStore) Delete(v store.Entry) error {
	return nil
}

// DeleteAll mocks the DeleteAll method of Store.
func (s *mockStore) DeleteAll(clusterID uuid.UUID) error {
	return nil
}

var (
	mockProviderFunc = func(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
		return nil, nil
	}
)
