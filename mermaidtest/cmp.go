// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/uuid"
)

// UUIDComparer creates a cmp.Comparer for comparing to uuid.UUID's.
func UUIDComparer() cmp.Option {
	return cmp.Comparer(func(a, b uuid.UUID) bool { return a == b })
}
