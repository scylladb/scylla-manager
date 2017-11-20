// Copyright (C) 2017 ScyllaDB

package cluster

import "github.com/scylladb/mermaid/uuid"

// Cluster specifies a cluster properties.
type Cluster struct {
	ID         uuid.UUID
	Name       string
	Hosts      []string
	ShardCount int64
}
