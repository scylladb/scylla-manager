// Copyright (C) 2017 ScyllaDB

package secrets

import (
	"encoding"

	"github.com/scylladb/mermaid/uuid"
)

// KeyValue interface of a secret value.
// Every secret value has to implement this interface in order to be supported
// by Secret services.
type KeyValue interface {
	Key() (clusterID uuid.UUID, key string)
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}
