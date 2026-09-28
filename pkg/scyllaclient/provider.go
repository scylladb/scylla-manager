// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
)

// ProviderFunc is a function that returns a Client for a given cluster.
type ProviderFunc func(ctx context.Context, clusterID uuid.UUID) (*Client, error)

type clientTTL struct {
	// Protects clientTTL during initialization and
	// validation of underlying client and its hostsTTL.
	// Serves as a waiting mechanism ensuring that parallel
	// calls to Client won't initialize client multiple times.
	mu     sync.Mutex
	client *Client
	// Time after which client hosts needs to be validated.
	// Can be extended if hosts pass connectivity validation.
	hostsTTL time.Time
	// Time after which client is invalid.
	ttl atomic.Time
}

// DefaultHostsValidity is the default duration after which
// cached client will have its hosts validity re-checked.
const DefaultHostsValidity = 15 * time.Second

// isValid checks if client can be safely returned from cache.
// Client is invalid when it reaches the end of TTL, or when its hosts changed.
// In order to reduce API calls under mutex when creating many clients
// (e.g. when healthcheck svc runs pingREST for every node),
// checking for changed hosts is done only every hostsValidity.
func (c *clientTTL) isValid(ctx context.Context, hostsValidity time.Duration) (bool, error) {
	// Check client TTL (if set)
	if ttl := c.ttl.Load(); ttl.IsZero() || ttl.Before(timeutc.Now()) {
		return false, nil
	}
	// Check hosts TTL and refresh if they didn't change
	if c.hostsTTL.Before(timeutc.Now()) {
		changed, err := c.client.CheckHostsChanged(ctx)
		switch {
		case err != nil:
			return false, errors.Wrap(err, "check if client's hosts changed")
		case changed:
			return false, nil
		default:
			c.hostsTTL = timeutc.Now().Add(hostsValidity)
		}
	}
	return true, nil
}

// CachedProvider is a provider implementation that reuses clients.
// Due to Client being safe to use after Client.Close(),
// CachedProvider might close expired yet currently used
// clients returned from CachedProvider.Client.
// Close needs to be called to clean up resources when CachedProvider
// is no longer needed.
type CachedProvider struct {
	inner         ProviderFunc
	validity      time.Duration
	hostsValidity time.Duration
	clients       map[uuid.UUID]*clientTTL
	mu            sync.Mutex
	logger        log.Logger
}

// NewCachedProvider returns CachedProvider using f to create clients,
// invalidates cached clients after cacheInvalidationTimeout,
// and checks whether cached clients' hosts changed every hostsValidity.
// The cacheInvalidationTimeout must not be negative (0 disables caching),
// and the hostsValidity must not be negative (0 checks hosts on every Client call).
func NewCachedProvider(f ProviderFunc, cacheInvalidationTimeout, hostsValidity time.Duration,
	logger log.Logger,
) (*CachedProvider, error) {
	if cacheInvalidationTimeout < 0 {
		return nil, errors.Errorf("invalid cache invalidation timeout %s: must not be negative", cacheInvalidationTimeout)
	}
	if hostsValidity < 0 {
		return nil, errors.Errorf("invalid hosts validity %s: must not be negative", hostsValidity)
	}

	return &CachedProvider{
		inner:         f,
		validity:      cacheInvalidationTimeout,
		hostsValidity: hostsValidity,
		clients:       make(map[uuid.UUID]*clientTTL),
		logger:        logger.Named("cache-provider"),
	}, nil
}

// Client is the cached ProviderFunc.
func (p *CachedProvider) Client(ctx context.Context, clusterID uuid.UUID) (*Client, error) {
	// Get current client cache entry
	c := p.getClientTTL(clusterID)
	if c == nil {
		return nil, errors.New("provider was already closed")
	}

	// Perform validation and necessary client creation
	// under per cluster mutex. This way multiple calls
	// to Client encountering stale client won't result
	// in creation of multiple clients.
	// Note that p.mu is not held here, as we don't want
	// to stall Client calls for different clusters with
	// potentially fresh clients.
	c.mu.Lock()
	defer c.mu.Unlock()

	if valid, err := c.isValid(ctx, p.hostsValidity); err != nil {
		p.logger.Error(ctx, "Cannot check client validity", "error", err)
	} else if valid {
		return c.client, nil
	}

	// If not found or invalid, create a new one
	client, err := p.inner(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	// Close the replaced client, so that it does not leak resources
	if c.client != nil {
		if err := c.client.Close(); err != nil {
			// Should never happen, as client.Close() always returns nil
			p.logger.Error(ctx, "Failed to close replaced client", "cluster_id", clusterID, "error", err)
		}
	}

	// Reacquire p.mu to verify that cached client entry
	// wasn't deleted in the meantime by Delete.
	// Note that p.mu and c.mu will both be held in the next block.
	p.mu.Lock()
	defer p.mu.Unlock()

	// Save new client under the same pointer
	c.client = client
	c.ttl.Store(timeutc.Now().Add(p.validity))
	c.hostsTTL = timeutc.Now().Add(p.hostsValidity)

	// If client entry was deleted in the meantime,
	// we need to pre-emptively close the client,
	// as we won't track it in the cached map and won't
	// be able to release its resources when it's replaced.
	// Closed client can still be used, it just won't update
	// epsilon greedy host pool.
	fetchedC, ok := p.clients[clusterID]
	if !ok || fetchedC != c {
		p.logger.Info(ctx, "Client was deleted from provider while it was being initialized", "cluster_id", clusterID)
		if err := c.client.Close(); err != nil {
			// Should never happen, as client.Close() always returns nil
			p.logger.Error(ctx, "Failed to close client deleted from provider while it was initialized",
				"cluster_id", clusterID, "error", err)
		}
	}
	return c.client, nil
}

// getClientTTL returns client entry from the cached pool
// or adds a new uninitialized one if it does not exist.
func (p *CachedProvider) getClientTTL(clusterID uuid.UUID) *clientTTL {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.clients == nil {
		return nil
	}

	c, ok := p.clients[clusterID]
	if !ok {
		c = new(clientTTL)
		p.clients[clusterID] = c
	}
	return c
}

// Invalidate invalidates the client forcing it to be recreated on the next Client call.
func (p *CachedProvider) Invalidate(clusterID uuid.UUID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.clients == nil {
		return
	}

	c, ok := p.clients[clusterID]
	if !ok {
		return // Nothing to do, client was already deleted
	}
	c.ttl.Store(time.Time{})
}

// Delete removes and closes client to clear up resources.
func (p *CachedProvider) Delete(clusterID uuid.UUID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleteLocked(clusterID)
}

// deleteLocked removes and closes client to clear up resources.
// It must be called with p.mu held.
func (p *CachedProvider) deleteLocked(clusterID uuid.UUID) {
	if p.clients == nil {
		return
	}

	c, ok := p.clients[clusterID]
	if !ok {
		return // Nothing to do, client was already deleted
	}

	c.ttl.Store(time.Time{})
	delete(p.clients, clusterID)

	if c.client == nil {
		return
	}
	if err := c.client.Close(); err != nil {
		// Should never happen, as client.Close() always returns nil
		p.logger.Error(context.Background(), "Cannot close deleted client", "cluster_id", clusterID, "error", err)
	}
}

// Close removes all clients and closes them to clear up any resources.
func (p *CachedProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for clusterID := range p.clients {
		p.deleteLocked(clusterID)
	}
	// Make next calls to provider return error or be a no-op
	p.clients = nil
	return nil
}
