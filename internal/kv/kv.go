// Copyright (C) 2017 ScyllaDB

package kv

import (
	"github.com/scylladb/mermaid/uuid"
)

// Store stores binary data.
type Store interface {
	// Get returns saved data, if nothing is found ErrNotFound is returned.
	Get(id uuid.UUID) ([]byte, error)
	// Put updates data for a given ID.
	Put(id uuid.UUID, data []byte) error
}
