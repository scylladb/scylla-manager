// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/uuid"
)

// UUIDComparer creates a cmp.Comparer for comparing to uuid.UUID's.
func UUIDComparer() cmp.Option {
	return cmp.Comparer(func(a, b uuid.UUID) bool { return a == b })
}

// DateTimeComparer creates a cmp.Comparer for comparing strfmt.DateTime's.
func DateTimeComparer() cmp.Option {
	return cmp.Comparer(func(a, b strfmt.DateTime) bool {
		return time.Time(a).Equal(time.Time(b))
	})
}
