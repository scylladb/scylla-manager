// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"context"

	"github.com/scylladb/mermaid/uuid"
)

// ProviderFunc returns cluster based on ID. If nothing was found
// mermaid.ErrNotFound is returned.
type ProviderFunc func(ctx context.Context, id uuid.UUID) (*Cluster, error)
