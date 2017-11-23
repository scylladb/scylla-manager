// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/uuid"
)

// Cluster specifies a cluster properties.
type Cluster struct {
	ID         uuid.UUID `json:"id"`
	Name       string    `json:"name"`
	Hosts      []string  `json:"hosts"`
	ShardCount int64     `json:"shard_count"`
}

// Validate checks if all the fields are properly set.
func (c *Cluster) Validate() error {
	if c == nil {
		return errors.New("nil")
	}
	if len(c.Hosts) == 0 {
		return errors.New("missing hosts")
	}
	if c.ShardCount <= 0 {
		return errors.New("invalid shard_count")
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
