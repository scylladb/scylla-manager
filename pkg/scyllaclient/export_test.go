// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"time"
)

func PickNRandomHosts(n int, hosts []string) []string {
	return pickNRandomHosts(n, hosts)
}

func RcloneSplitRemotePath(remotePath string) (string, string, error) {
	return rcloneSplitRemotePath(remotePath)
}

func NoRetry(ctx context.Context) context.Context {
	return noRetry(ctx)
}

func (p *CachedProvider) SetValidity(d time.Duration) {
	p.validity = d
}
