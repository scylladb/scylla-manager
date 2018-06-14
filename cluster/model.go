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
	ID              uuid.UUID `json:"id"`
	Name            string    `json:"name"`
	Hosts           []string  `json:"hosts"`
	ShardCount      int64     `json:"shard_count"`
	SSHUser         string    `json:"ssh_user,omitempty"`
	SSHIdentityFile []byte    `json:"ssh_identity_file,omitempty" db:"-"`
}

// String returns cluster Name or ID if Name is is empty.
func (c *Cluster) String() string {
	if c == nil {
		return ""
	}
	if c.Name != "" {
		return c.Name
	}
	return c.ID.String()
}

// Validate checks if all the fields are properly set.
func (c *Cluster) Validate() error {
	if c == nil {
		return errors.Wrap(mermaid.ErrNilPtr, "invalid filter")
	}

	var err error
	if _, e := uuid.Parse(c.Name); e == nil {
		err = multierr.Append(err, errors.New("name cannot be an UUID"))
	}
	if len(c.Hosts) == 0 {
		err = multierr.Append(err, errors.New("missing hosts"))
	}
	if c.ShardCount <= 0 {
		err = multierr.Append(err, errors.New("invalid shard_count"))
	}

	return mermaid.ErrValidate(err, "invalid cluster")
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

	var err error
	if _, e := uuid.Parse(f.Name); e == nil {
		err = multierr.Append(err, errors.New("name cannot be an UUID"))
	}

	return err
}
