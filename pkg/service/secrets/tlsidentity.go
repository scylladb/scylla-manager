// Copyright (C) 2017 ScyllaDB

package secrets

import (
	"encoding/json"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

const (
	tlsIdentityKey = "tls_identity"
)

// TLSIdentity defines TLS credentials to cluster.
type TLSIdentity struct {
	ClusterID  uuid.UUID `json:"-"`
	Cert       []byte    `json:"cert"`
	PrivateKey []byte    `json:"private_key"`
}

// Key returns pair of `clusterID` and `key` used for identifying credentials.
func (v *TLSIdentity) Key() (clusterID uuid.UUID, key string) {
	return v.ClusterID, tlsIdentityKey
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (v *TLSIdentity) MarshalBinary() (data []byte, err error) {
	if v.Cert == nil && v.PrivateKey == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (v *TLSIdentity) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}
