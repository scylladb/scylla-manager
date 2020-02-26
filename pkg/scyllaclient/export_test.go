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

func PickNRandomHosts(n int, hosts []string) []string {
	return pickNRandomHosts(n, hosts)
}

func RcloneSplitRemotePath(remotePath string) (string, string, error) {
	return rcloneSplitRemotePath(remotePath)
}

func (p *CachedProvider) SetValidity(d time.Duration) {
	p.validity = d
}
