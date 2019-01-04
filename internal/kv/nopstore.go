// Copyright (C) 2017 ScyllaDB

package kv

import (
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/uuid"
)

// NopStore implements Store with no-op implementations.
type NopStore struct{}

// Get implements kv.Store.
func (NopStore) Get(id uuid.UUID) ([]byte, error) {
	return nil, mermaid.ErrNotFound
}

// Put implements kv.Store
func (NopStore) Put(id uuid.UUID, data []byte) error {
	return nil
}
