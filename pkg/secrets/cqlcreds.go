// Copyright (C) 2017 ScyllaDB

package secrets

import (
	"encoding/json"

	"github.com/scylladb/scylla-manager/pkg/store"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// CQLCreds specifies CQL credentials to cluster.
type CQLCreds struct {
	ClusterID uuid.UUID `json:"-"`
	Username  string    `json:"username"`
	Password  string    `json:"password"`
}

var _ store.Entry = &CQLCreds{}

func (v *CQLCreds) Key() (clusterID uuid.UUID, key string) {
	return v.ClusterID, "cql_creds"
}

func (v *CQLCreds) MarshalBinary() (data []byte, err error) {
	if v.Username == "" {
		return nil, nil
	}
	return json.Marshal(v)
}

func (v *CQLCreds) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}
