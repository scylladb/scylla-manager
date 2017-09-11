// Copyright (C) 2017 ScyllaDB

package dbapi

import "github.com/scylladb/mermaid/uuid"

// ProviderFunc is a function that returns a Client for a given cluster.
type ProviderFunc func(clusterID uuid.UUID) (*Client, error)
