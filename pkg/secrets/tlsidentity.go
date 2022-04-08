// Copyright (C) 2017 ScyllaDB

package secrets

import (
	"encoding/json"

	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TLSIdentity defines TLS credentials to cluster.
type TLSIdentity struct {
	ClusterID  uuid.UUID `json:"-"`
	Cert       []byte    `json:"cert"`
	PrivateKey []byte    `json:"private_key"`
}

var _ store.Entry = &TLSIdentity{}

func (v *TLSIdentity) Key() (clusterID uuid.UUID, key string) {
	return v.ClusterID, "tls_identity"
}

func (v *TLSIdentity) MarshalBinary() (data []byte, err error) {
	if v.Cert == nil && v.PrivateKey == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

func (v *TLSIdentity) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}
