// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"time"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/client/operations"
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

func (c *Client) Hosts(ctx context.Context) ([]string, error) {
	return c.hosts(ctx)
}

func (c *Client) RawRepair(ctx context.Context, ks, tab, master string) (int32, error) {
	p := operations.StorageServiceRepairAsyncByKeyspacePostParams{
		Context:        forceHost(ctx, master),
		Keyspace:       ks,
		ColumnFamilies: &tab,
	}
	resp, err := c.scyllaOps.StorageServiceRepairAsyncByKeyspacePost(&p)
	if err != nil {
		return 0, err
	}
	return resp.GetPayload(), nil
}
