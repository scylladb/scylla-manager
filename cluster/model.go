// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/multierr"
)

// Cluster specifies a cluster properties.
type Cluster struct {
	ID         uuid.UUID `json:"id"`
	Name       string    `json:"name"`
	Hosts      []string  `json:"hosts"`
	ShardCount int64     `json:"shard_count"`
}

// Validate checks if all the fields are properly set.
func (c *Cluster) Validate() (err error) {
	if c == nil {
		return mermaid.ErrNilPtr
	}

	if len(c.Hosts) == 0 {
		err = multierr.Append(err, errors.New("missing hosts"))
	}
	if c.ShardCount <= 0 {
		err = multierr.Append(err, errors.New("invalid shard_count"))
	}

	return
}

// Filter filters Clusters.
type Filter struct {
	Name string
}

// Validate checks if all the fields are properly set.
func (f *Filter) Validate() error {
	if f == nil {
		return mermaid.ErrNilPtr
	}

	return nil
}
