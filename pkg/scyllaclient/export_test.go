// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"encoding/json"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/prom"
)

func NoRetry(ctx context.Context) context.Context {
	return noRetry(ctx)
}

func CustomTimeout(ctx context.Context, d time.Duration) context.Context {
	return customTimeout(ctx, d)
}

func WithShouldRetryHandler(ctx context.Context, f func(error) *bool) context.Context {
	return withShouldRetryHandler(ctx, f)
}

func TakeSnapshotShouldRetryHandler(err error) *bool {
	return takeSnapshotShouldRetryHandler(err)
}

func PickNRandomHosts(n int, hosts []string) []string {
	return pickNRandomHosts(n, hosts)
}

func RcloneSplitRemotePath(remotePath string) (string, string, error) {
	return rcloneSplitRemotePath(remotePath)
}

func (p *CachedProvider) SetValidity(d time.Duration) {
	p.validity = d
}

// CachedClients returns the amount of entries kept in the client cache.
func (p *CachedProvider) CachedClients() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.clients)
}

func (c *Client) Hosts(ctx context.Context) ([]string, error) {
	return c.hosts(ctx)
}

func ReadListStart(dec *json.Decoder) error {
	return readListStart(dec)
}

func ReadListEnd(dec *json.Decoder) error {
	return readListEnd(dec)
}

func (c *Client) Metrics(ctx context.Context, host, name string) (map[string]*prom.MetricFamily, error) {
	return c.metrics(ctx, host, name)
}
