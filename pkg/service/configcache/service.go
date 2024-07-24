// Copyright (C) 2024 ScyllaDB

package configcache

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// ConfigCacher is the interface defining the cache behavior.
type ConfigCacher interface {
	// Read returns either the host configuration that is currently stored in the cache,
	// ErrNoClusterConfig if config for the whole cluster doesn't exist,
	// or ErrNoHostConfig if config of the particular host doesn't exist.
	Read(clusterID uuid.UUID, host string) (NodeConfig, error)

	// AvailableHosts returns list of hosts of given cluster that keep their configuration in cache.
	AvailableHosts(ctx context.Context, clusterID uuid.UUID) ([]string, error)

	// ForceUpdateCluster updates single cluster config in cache and does it outside the background process.
	ForceUpdateCluster(ctx context.Context, clusterID uuid.UUID) bool

	// RemoveCluster removes cluster data of a given uuid from cache.
	RemoveCluster(clusterID uuid.UUID)

	// Init updates cache with config of all currently managed clusters.
	Init(ctx context.Context)

	// Run starts the infinity loop responsible for updating the clusters configuration periodically.
	Run(ctx context.Context)
}

// Service is responsible for handling all cluster configuration cache related operations.
// Use svc.Read(clusterID, host) to read the configuration of particular host in given cluster.
// Use svc.Run() to let the cache update itself periodically with the current configuration.
type Service struct {
	svcConfig Config

	clusterSvc   cluster.Servicer
	scyllaClient scyllaclient.ProviderFunc
	secretsStore store.Store

	configs *sync.Map
	logger  log.Logger
}

// NewService is the constructor for the cluster config cache service.
func NewService(config Config, clusterSvc cluster.Servicer, client scyllaclient.ProviderFunc, secretsStore store.Store, logger log.Logger) ConfigCacher {
	return &Service{
		svcConfig:    config,
		clusterSvc:   clusterSvc,
		scyllaClient: client,
		secretsStore: secretsStore,
		configs:      &sync.Map{},
		logger:       logger,
	}
}

func (svc *Service) Init(ctx context.Context) {
	svc.configs = &sync.Map{}
	svc.updateAll(ctx)
}

func (svc *Service) Read(clusterID uuid.UUID, host string) (NodeConfig, error) {
	emptyConfig := NodeConfig{}

	clusterConfig, err := svc.readClusterConfig(clusterID)
	if err != nil {
		return emptyConfig, err
	}

	rawHostConfig, ok := clusterConfig.Load(host)
	if !ok {
		return emptyConfig, ErrNoHostConfig
	}
	hostConfig, ok := rawHostConfig.(NodeConfig)
	if !ok {
		panic("cluster host emptyConfig cache stores unexpected type")
	}
	return hostConfig, nil
}

// Run starts the infinity loop responsible for updating the clusters configuration periodically.
func (svc *Service) Run(ctx context.Context) {
	freq := time.NewTicker(svc.svcConfig.UpdateFrequency)

	for {
		// make sure to shut down when the context is cancelled
		select {
		case <-ctx.Done():
			return
		default:
		}

		select {
		case <-freq.C:
			svc.updateAll(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// RemoveCluster removes cluster data of a given uuid from cache.
func (svc *Service) RemoveCluster(clusterID uuid.UUID) {
	svc.configs.Delete(clusterID.String())
}

// ForceUpdateCluster updates single cluster config in cache and does it outside the background process.
func (svc *Service) ForceUpdateCluster(ctx context.Context, clusterID uuid.UUID) bool {
	logger := svc.logger.Named("Force update cluster").With("cluster", clusterID)

	c, err := svc.clusterSvc.GetCluster(ctx, clusterID.String())
	if err != nil {
		logger.Error(ctx, "Update failed", "error", err)
	}

	return svc.updateSingle(ctx, c)
}

// AvailableHosts returns list of hosts of given cluster that keep their configuration in cache.
func (svc *Service) AvailableHosts(ctx context.Context, clusterID uuid.UUID) ([]string, error) {
	logger := svc.logger.Named("Listing available hosts").With("cluster", clusterID)

	clusterConfig, err := svc.readClusterConfig(clusterID)
	if err != nil {
		return nil, err
	}

	var availableHosts []string
	clusterConfig.Range(func(key, value any) bool {
		host, ok := key.(string)
		if !ok {
			logger.Error(ctx, "Cannot cast to string", "host", key, "error", err)
			return false
		}
		availableHosts = append(availableHosts, host)
		return true
	})

	return availableHosts, nil
}

func (svc *Service) updateSingle(ctx context.Context, c *cluster.Cluster) bool {
	logger := svc.logger.Named("Cluster config update").With("cluster", c.ID)

	clusterConfig := &sync.Map{}

	client, err := svc.scyllaClient(ctx, c.ID)
	if err != nil {
		logger.Error(ctx, "Couldn't create scylla client", "cluster", c.ID, "error", err)
		return false
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Error(ctx, "Couldn't close HTTP client", "error", err)
		}
	}()

	// Hosts that are going to be asked about the configuration are exactly the same as
	// the ones used by the scylla client.
	hostsWg := sync.WaitGroup{}

	for _, host := range client.Config().Hosts {
		hostsWg.Add(1)

		host := host
		perHostLogger := logger.Named("Cluster host config update").With("host", host)
		go func() {
			defer hostsWg.Done()

			config, err := svc.retrieveNodeConfig(ctx, host, client, c)
			if err != nil {
				perHostLogger.Error(ctx, "Couldn't read cluster host config", "error", err)
				return
			}
			clusterConfig.Store(host, config)
		}()
	}
	hostsWg.Wait()
	svc.configs.Store(c.ID.String(), clusterConfig)

	return true
}

func (svc *Service) updateAll(ctx context.Context) {
	clusters, err := svc.clusterSvc.ListClusters(ctx, &cluster.Filter{})
	if err != nil {
		svc.logger.Error(ctx, "Couldn't list clusters", "error", err)
		return
	}

	clustersWg := sync.WaitGroup{}
	for _, c := range clusters {
		c := c
		clustersWg.Add(1)
		go func() {
			defer clustersWg.Done()

			svc.updateSingle(ctx, c)
		}()
	}
	clustersWg.Wait()
}

func (svc *Service) readClusterConfig(clusterID uuid.UUID) (*sync.Map, error) {
	emptyConfig := &sync.Map{}

	rawClusterConfig, ok := svc.configs.Load(clusterID.String())
	if !ok {
		return emptyConfig, ErrNoClusterConfig
	}
	clusterConfig, ok := rawClusterConfig.(*sync.Map)
	if !ok {
		panic("cluster cache emptyConfig stores unexpected type")
	}
	return clusterConfig, nil
}

func (svc *Service) retrieveNodeConfig(ctx context.Context, host string, client *scyllaclient.Client,
	c *cluster.Cluster,
) (NodeConfig, error) {
	config := NodeConfig{}

	nodeInfoResp, err := client.NodeInfo(ctx, host)
	if err != nil {
		return config, errors.Wrap(err, "retrieve cluster host configuration")
	}
	dc, err := client.HostDatacenter(ctx, host)
	if err != nil {
		return config, errors.Wrap(err, "retrieve host Datacenter info")
	}
	rack, err := client.HostRack(ctx, host)
	if err != nil {
		return config, errors.Wrap(err, "retrieve host Rack info")
	}

	config, err = NewNodeConfig(c, nodeInfoResp, svc.secretsStore, host, dc, rack)
	if err != nil {
		return config, errors.Wrap(err, "retrieve cluster host configuration")
	}

	return config, nil
}
