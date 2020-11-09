// Copyright (C) 2017 ScyllaDB

package secrets

import (
	"errors"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

var (
	// ErrEmptyKeyValue is returned when provided KeyValue have nil clusterID
	// or empty key.
	ErrEmptyKeyValue = errors.New("ClusterID and Key are mandatory")
)

// Store is interface used for storing secret values.
// In order to store value, provided type must implement KeyValue interface.
type Store interface {
	Put(secret KeyValue) error
	Get(secret KeyValue) error
	Delete(secret KeyValue) error
	DeleteAll(clusterID uuid.UUID) error
}
