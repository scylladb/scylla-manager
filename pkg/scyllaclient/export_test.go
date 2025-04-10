// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"time"
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
