// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// UUIDMatcher gomock.Matcher interface implementation for UUID.
type UUIDMatcher struct {
	expected uuid.UUID
}

// NewUUIDMatcher returns gomock.Matcher for UUIDs.
func NewUUIDMatcher(expected uuid.UUID) *UUIDMatcher {
	return &UUIDMatcher{expected: expected}
}

// Matches returns whether v is a match.
func (m UUIDMatcher) Matches(v interface{}) bool {
	id, ok := v.(uuid.UUID)
	if !ok {
		return false
	}
	return cmp.Equal(m.expected, id, UUIDComparer())
}

func (m *UUIDMatcher) String() string {
	return fmt.Sprintf("is equal to: %s", m.expected.String())
}
