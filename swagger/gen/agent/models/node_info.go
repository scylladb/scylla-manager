// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// NodeInfo node info
//
// Information about Scylla node.
//
// swagger:model NodeInfo
type NodeInfo struct {

	// Scylla Manager Agent version.
	AgentVersion string `json:"agent_version,omitempty"`

	// Address for Alternator API requests.
	AlternatorAddress string `json:"alternator_address,omitempty"`

	// Whether Alternator requires authentication.
	AlternatorEnforceAuthorization bool `json:"alternator_enforce_authorization,omitempty"`

	// Port for Alternator HTTPS API server.
	AlternatorHTTPSPort string `json:"alternator_https_port,omitempty"`

	// Port for Alternator API server.
	AlternatorPort string `json:"alternator_port,omitempty"`

	// Address for REST API requests.
	APIAddress string `json:"api_address,omitempty"`

	// Port for REST API server.
	APIPort string `json:"api_port,omitempty"`

	// Address that is broadcasted to tell other Scylla nodes to connect to. Related to listen_address.
	BroadcastAddress string `json:"broadcast_address,omitempty"`

	// Address that is broadcasted to tell the clients to connect to.
	BroadcastRPCAddress string `json:"broadcast_rpc_address,omitempty"`

	// Whether client encryption is enabled.
	ClientEncryptionEnabled bool `json:"client_encryption_enabled,omitempty"`

	// Whether client authorization is required.
	ClientEncryptionRequireAuth bool `json:"client_encryption_require_auth,omitempty"`

	// Whether Scylla uses RAFT for cluster management and DDL.
	ConsistentClusterManagement bool `json:"consistent_cluster_management,omitempty"`

	// Logical CPU count.
	CPUCount int64 `json:"cpu_count,omitempty"`

	// Whether CQL requires password authentication.
	CqlPasswordProtected bool `json:"cql_password_protected,omitempty"`

	// Whether tablets are enabled.
	EnableTablets bool `json:"enable_tablets,omitempty"`

	// Address Scylla listens for connections from other nodes.
	ListenAddress string `json:"listen_address,omitempty"`

	// Total available memory.
	MemoryTotal int64 `json:"memory_total,omitempty"`

	// Port for the CQL native transport to listen for clients on.
	NativeTransportPort string `json:"native_transport_port,omitempty"`

	// Port for the encrypted CQL native transport to listen for clients on.
	NativeTransportPortSsl string `json:"native_transport_port_ssl,omitempty"`

	// Address for Prometheus queries.
	PrometheusAddress string `json:"prometheus_address,omitempty"`

	// Port for Prometheus server.
	PrometheusPort string `json:"prometheus_port,omitempty"`

	// Address on which Scylla is going to expect Thrift and CQL clients connections.
	RPCAddress string `json:"rpc_address,omitempty"`

	// Port for Thrift to listen for clients on.
	RPCPort string `json:"rpc_port,omitempty"`

	// Scylla version.
	ScyllaVersion string `json:"scylla_version,omitempty"`

	// Whether Scylla supports uuid-like sstable naming.
	SstableUUIDFormat bool `json:"sstable_uuid_format,omitempty"`

	// Uptime in seconds.
	Uptime int64 `json:"uptime,omitempty"`
}

// Validate validates this node info
func (m *NodeInfo) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *NodeInfo) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *NodeInfo) UnmarshalBinary(b []byte) error {
	var res NodeInfo
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
