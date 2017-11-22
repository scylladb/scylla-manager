// Copyright (C) 2017 ScyllaDB

package scylla

import (
	"context"

	"github.com/scylladb/mermaid/uuid"
)

// ProviderFunc is a function that returns a Client for a given cluster.
type ProviderFunc func(ctx context.Context, clusterID uuid.UUID) (*Client, error)
