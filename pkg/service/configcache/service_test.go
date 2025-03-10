// Copyright (C) 2024 ScyllaDB

package configcache

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
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

func TestService_AvailableHosts(t *testing.T) {
	host1NodeConfig := NodeConfig{
		NodeInfo: &scyllaclient.NodeInfo{
			AgentVersion: "expectedVersion",
		},
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
		name           string
		cluster        uuid.UUID
		state          *sync.Map
		expectedError  error
		expectedResult []string
	}{
		{
			name:           "get all available hosts",
			cluster:        cluster1UUID,
			state:          initialState,
			expectedError:  nil,
			expectedResult: []string{host1ID},
		},
		{
			name:           "get all available hosts of non-existing cluster",
			cluster:        uuid.MustRandom(),
			state:          initialState,
			expectedError:  ErrNoClusterConfig,
			expectedResult: nil,
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
			hosts, err := svc.AvailableHosts(context.Background(), tc.cluster)
			if err != tc.expectedError {
				t.Fatalf("expected {%v}, but got {%v}", tc.expectedError, err)
			}

			// Then
			if len(hosts) != len(tc.expectedResult) {
				t.Fatalf("expected hosts size = {%v}, but got {%v}", len(tc.expectedResult), len(hosts))
			}
			for i := 0; i < len(tc.expectedResult); i++ {
				if hosts[i] != tc.expectedResult[i] {
					t.Fatalf("expected host {%v}, but got {%v}", host1ID, hosts[0])
				}
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

func TestServiceForceUpdateCluster(t *testing.T) {
	t.Run("validate no panic when updating non-existing cluster", func(t *testing.T) {
		svc := Service{
			svcConfig:    DefaultConfig(),
			clusterSvc:   &mockErrorClusterSvc{},
			scyllaClient: mockProviderFunc,
			secretsStore: &mockStore{},
			configs:      &sync.Map{},
			logger:       log.NewDevelopment(),
		}

		if svc.ForceUpdateCluster(context.Background(), uuid.MustRandom()) {
			t.Fatalf("Expected updating non-existing cluster config to fail")
		}
	})
}

func TestService_Read_IPv6Normalization(t *testing.T) {
	// Define a canonical IPv6 representation
	key := "2001:0db8:0000:0000:0000:0000:0000:0001"
	parsedKey, err := netip.ParseAddr(key)
	if err != nil {
		t.Fatal(err)
	}

	// Assume that our system normalizes IPv6 addresses to the canonical form.
	// Create the configuration for the canonical IPv6 address.
	nodeConfig := NodeConfig{
		NodeInfo: &scyllaclient.NodeInfo{
			AgentVersion: "expectedVersion",
		},
	}
	configHash, err := nodeConfig.sha256hash()
	if err != nil {
		t.Fatal(err)
	}

	clusterID := uuid.MustRandom()
	// Prepopulate the service config with the canonical representation as the key.
	initialState := convertMapToSyncMap(map[any]any{
		clusterID.String(): convertMapToSyncMap(map[any]any{
			parsedKey.String(): nodeConfig,
		}),
	})

	// Define several valid IPv6 representations for the same host.
	testCases := []struct {
		name    string
		hostKey string
	}{
		{"Fully Expanded", "2001:0db8:0000:0000:0000:0000:0000:0001"},
		{"Uncompressed", "2001:db8:0:0:0:0:0:1"},
		{"Compressed", "2001:db8::1"},
		{"Uppercase Variant", "2001:DB8::1"},
		{"Bracketed", "[2001:db8::1]"},
	}

	svc := Service{
		svcConfig:    DefaultConfig(),
		clusterSvc:   &mockClusterServicer{},
		scyllaClient: mockProviderFunc,
		secretsStore: &mockStore{},
		configs:      initialState,
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			conf, err := svc.Read(clusterID, tc.hostKey)
			// Then
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			h, err := conf.sha256hash()
			if err != nil {
				t.Fatalf("error creating hash: %v", err)
			}
			if h != configHash {
				t.Fatalf("expected hash %v, got %v", configHash, h)
			}
		})
	}
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

// mockErrorClusterSvc works like mockClusterServicer with custom overrides for error responses.
type mockErrorClusterSvc struct {
	mockClusterServicer
}

// GetCluster mocks the GetCluster method of Servicer with error response.
func (s *mockErrorClusterSvc) GetCluster(_ context.Context, _ string) (*cluster.Cluster, error) {
	return nil, errors.New("not found")
}
