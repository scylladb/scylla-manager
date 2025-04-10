// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"net"
	"net/url"
	"time"

	"github.com/pkg/errors"
	scyllaversion "github.com/scylladb/scylla-manager/v3/pkg/util/version"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/client/operations"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

// NodeInfo provides basic information about Scylla node.
type NodeInfo models.NodeInfo

// NodeInfo returns basic information about `host` node.
func (c *Client) NodeInfo(ctx context.Context, host string) (*NodeInfo, error) {
	p := operations.NodeInfoParams{
		Context: forceHost(ctx, host),
	}
	resp, err := c.agentOps.NodeInfo(&p)
	if err != nil {
		return nil, errors.Wrap(err, "node info")
	}
	return (*NodeInfo)(resp.Payload), nil
}

// AnyNodeInfo returns basic information about any node.
func (c *Client) AnyNodeInfo(ctx context.Context) (*NodeInfo, error) {
	p := operations.NodeInfoParams{
		Context: ctx,
	}
	resp, err := c.agentOps.NodeInfo(&p)
	if err != nil {
		return nil, errors.Wrap(err, "node info")
	}
	return (*NodeInfo)(resp.Payload), nil
}

// GetPinnedCPU returns CPUs to which agent is pinned.
func (c *Client) GetPinnedCPU(ctx context.Context, host string) ([]int64, error) {
	p := operations.GetCPUParams{
		Context: forceHost(ctx, host),
	}
	resp, err := c.agentOps.GetCPU(&p)
	if err != nil {
		return nil, errors.Wrap(err, "get CPUs")
	}
	return resp.Payload.CPU, nil
}

// UnpinFromCPU allows agent to use all CPUs.
func (c *Client) UnpinFromCPU(ctx context.Context, host string) error {
	p := operations.UnpinCPUParams{
		Context: forceHost(ctx, host),
	}
	_, err := c.agentOps.UnpinCPU(&p)
	return err
}

// PinCPU pins agent to CPUs according to the start-up logic.
func (c *Client) PinCPU(ctx context.Context, host string) error {
	p := operations.PinCPUParams{
		Context: forceHost(ctx, host),
	}
	_, err := c.agentOps.PinCPU(&p)
	return err
}

// cqlAddr returns CQL address from NodeInfo.
func (ni *NodeInfo) cqlAddr(fallback string) string {
	addr, port := ni.cqlListenAddr(fallback), ni.cqlPort()
	return net.JoinHostPort(addr, port)
}

// cqlSSLAddr returns CQL SSL address from NodeInfo.
func (ni *NodeInfo) cqlSSLAddr(fallback string) string {
	addr, port := ni.cqlListenAddr(fallback), ni.cqlSSLPort()
	return net.JoinHostPort(addr, port)
}

// CQLAddr returns either CQL or CQL SSL address from Node Info depending on the cluster configuration.
// Scylla can have separate rpc_address (CQL), listen_address and respectfully
// broadcast_rpc_address and broadcast_address if some 3rd party routing
// is added.
// `fallback` argument is used in case any of above addresses is zero address.
func (ni *NodeInfo) CQLAddr(fallback string, clusterTLSAddrDisabled bool) string {
	if ni.ClientEncryptionEnabled && !clusterTLSAddrDisabled {
		return ni.cqlSSLAddr(fallback)
	}
	return ni.cqlAddr(fallback)
}

func (ni *NodeInfo) cqlListenAddr(fallback string) string {
	const ipv4Zero, ipv6Zero = "0.0.0.0", "::0"

	if ni.BroadcastRPCAddress != "" {
		return ni.BroadcastRPCAddress
	}
	if ni.RPCAddress != "" {
		if ni.RPCAddress == ipv4Zero || ni.RPCAddress == ipv6Zero {
			return fallback
		}
		return ni.RPCAddress
	}
	if ni.ListenAddress == ipv4Zero || ni.ListenAddress == ipv6Zero {
		return fallback
	}

	return ni.ListenAddress
}

// cqlPort returns CQL port from NodeInfo.
func (ni *NodeInfo) cqlPort() string {
	return ni.NativeTransportPort
}

// cqlSSLPort returns CQL SSL port from NodeInfo.
func (ni *NodeInfo) cqlSSLPort() string {
	return ni.NativeTransportPortSsl
}

// AlternatorEnabled returns if Alternator is enabled on host.
func (ni *NodeInfo) AlternatorEnabled() bool {
	return (ni.AlternatorHTTPSPort != "0" && ni.AlternatorHTTPSPort != "") ||
		(ni.AlternatorPort != "0" && ni.AlternatorPort != "")
}

// AlternatorEncryptionEnabled returns if Alternator uses encrypted traffic.
func (ni *NodeInfo) AlternatorEncryptionEnabled() bool {
	return ni.AlternatorHTTPSPort != "0" && ni.AlternatorHTTPSPort != ""
}

// SupportsAlternatorQuery returns if Alternator supports querying system tables.
func (ni NodeInfo) SupportsAlternatorQuery() (bool, error) {
	// Detect master builds
	if scyllaversion.MasterVersion(ni.ScyllaVersion) {
		return true, nil
	}

	supports, err := scyllaversion.CheckConstraint(ni.ScyllaVersion, ">= 4.1, < 2000")
	if err != nil {
		return false, errors.Errorf("Unsupported Scylla version: %s", ni.ScyllaVersion)
	}

	return supports, nil
}

// AlternatorAddr returns Alternator address from NodeInfo.
// It chooses right address and port based on information stored in NodeInfo.
// HTTPS port has preference over HTTP.
// `fallback` argument is used in case alternator_addresses is zero address.
func (ni *NodeInfo) AlternatorAddr(fallback string) string {
	const ipv4Zero, ipv6Zero = "0.0.0.0", "::0"

	u := url.URL{
		Scheme: "http",
	}

	port := ni.AlternatorPort
	if ni.AlternatorHTTPSPort != "" && ni.AlternatorHTTPSPort != "0" {
		port = ni.AlternatorHTTPSPort
		u.Scheme = "https"
	}
	if ni.AlternatorAddress != "" {
		if ni.AlternatorAddress == ipv4Zero || ni.AlternatorAddress == ipv6Zero {
			u.Host = net.JoinHostPort(fallback, port)
		} else {
			u.Host = net.JoinHostPort(ni.AlternatorAddress, port)
		}
	} else {
		u.Host = net.JoinHostPort(fallback, port)
	}

	return u.String()
}

// CQLTLSEnabled returns whether TLS and client certificate
// authorization is enabled for CQL frontend.
func (ni NodeInfo) CQLTLSEnabled() (tlsEnabled, certAuth bool) {
	return ni.ClientEncryptionEnabled, ni.ClientEncryptionRequireAuth
}

// AlternatorTLSEnabled returns whether TLS and client certificate
// authorization is enabled for Alternator frontend.
func (ni NodeInfo) AlternatorTLSEnabled() (tlsEnabled, certAuth bool) {
	// Alternator doesn't support client cert authorization.
	certAuth = false
	return ni.AlternatorEncryptionEnabled(), certAuth
}

// SupportsRepairSmallTableOptimization returns true if /storage_service/repair_async/{keyspace} supports small_table_optimization param.
func (ni *NodeInfo) SupportsRepairSmallTableOptimization() (bool, error) {
	// Detect master builds
	if scyllaversion.MasterVersion(ni.ScyllaVersion) {
		return true, nil
	}
	// Check OSS
	supports, err := scyllaversion.CheckConstraint(ni.ScyllaVersion, ">= 6.0, < 2000")
	if err != nil {
		return false, errors.Errorf("Unsupported Scylla version: %s", ni.ScyllaVersion)
	}
	if supports {
		return true, nil
	}
	// Check ENT
	supports, err = scyllaversion.CheckConstraint(ni.ScyllaVersion, ">= 2024.1.5")
	if err != nil {
		return false, errors.Errorf("Unsupported Scylla version: %s", ni.ScyllaVersion)
	}
	return supports, nil
}

// SupportsTabletRepair returns true if /storage_service/tablets/repair API is exposed.
func (ni *NodeInfo) SupportsTabletRepair() (bool, error) {
	// Detect master builds
	if scyllaversion.MasterVersion(ni.ScyllaVersion) {
		return true, nil
	}
	// Check ENT
	return scyllaversion.CheckConstraint(ni.ScyllaVersion, ">= 2025.1")
}

// SafeDescribeMethod describes supported methods to ensure that scylla schema is consistent.
type SafeDescribeMethod string

var (
	// SafeDescribeMethodReadBarrierAPI shows when scylla read barrier api can be used.
	SafeDescribeMethodReadBarrierAPI SafeDescribeMethod = "read_barrier_api"
	// SafeDescribeMethodReadBarrierCQL shows when scylla csq read barrier can be used.
	SafeDescribeMethodReadBarrierCQL SafeDescribeMethod = "read_barrier_cql"
)

// SupportsSafeDescribeSchemaWithInternals returns not empty SafeDescribeMethod if the output of DESCRIBE SCHEMA WITH INTERNALS
// is safe to use with backup/restore procedure and which method should be used to make sure that schema is consistent.
func (ni *NodeInfo) SupportsSafeDescribeSchemaWithInternals() (SafeDescribeMethod, error) {
	// Detect master builds
	if scyllaversion.MasterVersion(ni.ScyllaVersion) {
		return SafeDescribeMethodReadBarrierAPI, nil
	}

	type featureByVersion struct {
		Constraint string
		Method     SafeDescribeMethod
	}

	for _, fv := range []featureByVersion{
		{Constraint: ">= 6.1, < 2000", Method: SafeDescribeMethodReadBarrierAPI},
		{Constraint: ">= 2024.2, > 1000", Method: SafeDescribeMethodReadBarrierCQL},
		{Constraint: ">= 6.0, < 2000", Method: SafeDescribeMethodReadBarrierCQL},
	} {
		supports, err := scyllaversion.CheckConstraint(ni.ScyllaVersion, fv.Constraint)
		if err != nil {
			return "", errors.Errorf("Unsupported Scylla version: %s", ni.ScyllaVersion)
		}
		if supports {
			return fv.Method, nil
		}
	}

	return "", nil
}

// FreeOSMemory calls debug.FreeOSMemory on the agent to return memory to OS.
func (c *Client) FreeOSMemory(ctx context.Context, host string) error {
	p := operations.FreeOSMemoryParams{
		Context: forceHost(ctx, host),
	}
	_, err := c.agentOps.FreeOSMemory(&p)
	return errors.Wrap(err, "free OS memory")
}

// CloudMetadata returns instance metadata from agent node.
func (c *Client) CloudMetadata(ctx context.Context, host string) (InstanceMetadata, error) {
	ctx = forceHost(ctx, host)
	// Agent metadata endpoint timeouts after 5 seconds
	// if the node is not in the cloud. We need to take
	// it into consideration when setting timeout on SM side.
	ctx = customTimeout(ctx, c.config.Timeout+5*time.Second)
	p := operations.MetadataParams{
		Context: ctx,
	}

	meta, err := c.agentOps.Metadata(&p)
	if err != nil {
		return InstanceMetadata{}, errors.Wrap(err, "cloud metadata")
	}

	payload := meta.GetPayload()
	if payload == nil {
		return InstanceMetadata{}, errors.New("payload is nil")
	}

	return InstanceMetadata{
		CloudProvider: payload.CloudProvider,
		InstanceType:  payload.InstanceType,
	}, nil
}
