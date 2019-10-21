// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/kv"
	"github.com/scylladb/mermaid/uuid"
)

func putWithRollback(s kv.Store, id uuid.UUID, value []byte) (rollback func(), err error) {
	var old []byte
	old, err = s.Get(id)
	if err != nil && err != mermaid.ErrNotFound {
		return nil, errors.Wrap(err, "read old value")
	}
	if err := s.Put(id, value); err != nil {
		return nil, errors.Wrap(err, "write value")
	}
	return func() {
		s.Put(id, old) // nolint:errcheck
	}, nil
}
