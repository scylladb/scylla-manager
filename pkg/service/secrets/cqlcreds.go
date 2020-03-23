// Copyright (C) 2017 ScyllaDB

package secrets

import (
	"encoding/json"

	"github.com/scylladb/mermaid/pkg/util/uuid"
)

const (
	cqlCredsKey = "cql_creds"
)

// CQLCreds specifies CQL credentials to cluster.
type CQLCreds struct {
	ClusterID uuid.UUID `json:"-"`
	Username  string    `json:"username"`
	Password  string    `json:"password"`
}

// Key returns pair of `clusterID` and `key` used for identifying credentials.
func (v *CQLCreds) Key() (clusterID uuid.UUID, key string) {
	return v.ClusterID, cqlCredsKey
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (v *CQLCreds) MarshalBinary() (data []byte, err error) {
	if v.Username == "" {
		return nil, nil
	}
	return json.Marshal(v)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (v *CQLCreds) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}
