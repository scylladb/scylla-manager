// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/pkg/util/uuid"
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

// NearTimeComparer creates a cmp.Comparer for comparing time.Time values
// that are within a threshold duration.
// First value has to be before second.
func NearTimeComparer(d time.Duration) cmp.Option {
	return cmp.Comparer(func(a, b *time.Time) bool {
		if a == nil && b == nil {
			return true
		}
		if a == nil && b != nil {
			return false
		}
		if a != nil && b == nil {
			return false
		}
		if a.Before(*b) {
			return b.Sub(*a) < d
		}
		return a.Sub(*b) < d
	})
}
