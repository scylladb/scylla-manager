// Copyright (C) 2025 ScyllaDB

package secrets

import (
	"encoding/json"

	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// AlternatorCreds specifies alternator credentials to cluster.
type AlternatorCreds struct {
	ClusterID       uuid.UUID `json:"-"`
	AccessKeyID     string    `json:"access_key_id"`
	SecretAccessKey string    `json:"secret_access_key"`
}

var _ store.Entry = &AlternatorCreds{}

func (v *AlternatorCreds) Key() (clusterID uuid.UUID, key string) {
	return v.ClusterID, "alternator_creds"
}

func (v *AlternatorCreds) MarshalBinary() (data []byte, err error) {
	if v.AccessKeyID == "" {
		return nil, nil
	}
	return json.Marshal(v)
}

func (v *AlternatorCreds) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}
