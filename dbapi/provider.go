// Copyright (C) 2017 ScyllaDB

package dbapi

import "github.com/scylladb/mermaid"

// ProviderFunc is a function that returns a Client for a given cluster.
type ProviderFunc func(clusterID mermaid.UUID) (*Client, error)
