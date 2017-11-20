// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/uuid"
)

// Cluster specifies a cluster properties.
type Cluster struct {
	ID         uuid.UUID
	Name       string
	Hosts      []string
	ShardCount int64
}

// Validate checks if all the fields are properly set.
func (c *Cluster) Validate() error {
	if c == nil {
		return errors.New("nil")
	}
	return nil
}

// Filter filters Clusters.
type Filter struct {
	Name string
}

// Validate checks if all the fields are properly set.
func (f *Filter) Validate() error {
	if f == nil {
		return errors.New("nil")
	}

	return nil
}
