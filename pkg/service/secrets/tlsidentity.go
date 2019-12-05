// Copyright (C) 2017 ScyllaDB

package secrets

import (
	"encoding/json"

	"github.com/scylladb/mermaid/pkg/util/uuid"
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

// MakeTLSIdentity returns instance of TLSIdentity.
func MakeTLSIdentity(clusterID uuid.UUID, cert, key []byte) *TLSIdentity {
	return &TLSIdentity{
		ClusterID:  clusterID,
		Cert:       cert,
		PrivateKey: key,
	}
}

// Key returns pair of `clusterID` and `key` used for identifying credentials.
func (i *TLSIdentity) Key() (clusterID uuid.UUID, key string) {
	return i.ClusterID, tlsIdentityKey
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (i *TLSIdentity) MarshalBinary() (data []byte, err error) {
	if i.Cert == nil && i.PrivateKey == nil {
		return nil, nil
	}
	return json.Marshal(i)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (i *TLSIdentity) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, i)
}
