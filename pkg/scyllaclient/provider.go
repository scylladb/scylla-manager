// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"sync"
	"time"

	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// ProviderFunc is a function that returns a Client for a given cluster.
type ProviderFunc func(ctx context.Context, clusterID uuid.UUID) (*Client, error)

// hostCheckValidity specifies how often to check if hosts changed.
const hostCheckValidity = 15 * time.Minute

type clientTTL struct {
	client *Client
	ttl    time.Time
}

// CachedProvider is a provider implementation that reuses clients.
type CachedProvider struct {
	inner    ProviderFunc
	validity time.Duration
	clients  map[uuid.UUID]clientTTL
	mu       sync.Mutex
}

func NewCachedProvider(f ProviderFunc) *CachedProvider {
	return &CachedProvider{
		inner:    f,
		validity: hostCheckValidity,
		clients:  make(map[uuid.UUID]clientTTL),
	}
}

// Client is the cached ProviderFunc.
func (p *CachedProvider) Client(ctx context.Context, clusterID uuid.UUID) (*Client, error) {
	p.mu.Lock()
	c, ok := p.clients[clusterID]
	p.mu.Unlock()

	// Cache hit
	if ok {
		if c.ttl.After(timeutc.Now()) {
			return c.client, nil
		}

		// Check if hosts did not change before returning
		changed, err := c.client.CheckHostsChanged(ctx)
		if err != nil {
			return nil, err
		}
		if !changed {
			return c.client, nil
		}
	}

	// If not found or hosts changed create a new one
	client, err := p.inner(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	c = clientTTL{
		client: client,
		ttl:    timeutc.Now().Add(p.validity),
	}

	p.mu.Lock()
	p.clients[clusterID] = c
	p.mu.Unlock()

	return c.client, nil
}

// Invalidate removes client for clusterID from cache.
func (p *CachedProvider) Invalidate(clusterID uuid.UUID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.clients, clusterID)
}

// Close removes all clients and closes them to clear up any resources.
func (p *CachedProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for clusterID, c := range p.clients {
		delete(p.clients, clusterID)
		c.client.Close()
	}

	return nil
}
