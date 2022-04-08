// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"math"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
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

// NearTimeComparer creates a cmp.Comparer for comparing *time.Time values
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

// NearDurationComparer creates a cmp.Comparer for comparing time.Duration
// values that are within a threshold duration.
func NearDurationComparer(d time.Duration) cmp.Option {
	return cmp.Comparer(func(a, b time.Duration) bool {
		return math.Abs(float64(a-b)) < float64(d)
	})
}
