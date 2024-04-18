// Copyright (C) 2024 ScyllaDB

package configcache

import "errors"

var (
	// ErrNoClusterConfig is thrown when there is no config for given cluster in cache.
	ErrNoClusterConfig = errors.New("no cluster config available")
	// ErrNoHostConfig is thrown when there is no config for given host in cache.
	ErrNoHostConfig = errors.New("no host config available")
)
