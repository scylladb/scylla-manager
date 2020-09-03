// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"net"
	"net/url"
	"time"

	"github.com/scylladb/mermaid/pkg/scyllaclient/internal/agent/client/operations"
	"github.com/scylladb/mermaid/pkg/scyllaclient/internal/agent/models"
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
		return nil, err
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
		return nil, err
	}
	return (*NodeInfo)(resp.Payload), nil
}

// CQLAddr returns CQL address from NodeInfo.
// Scylla can have separate rpc_address (CQL), listen_address and respectfully
// broadcast_rpc_address and broadcast_address if some 3rd party routing
// is added.
// `fallback` argument is used in case any of above addresses is zero address.
func (ni *NodeInfo) CQLAddr(fallback string) string {
	addr, port := ni.cqlAddr(fallback), ni.CQLPort(fallback)
	return net.JoinHostPort(addr, port)
}

func (ni *NodeInfo) cqlAddr(fallback string) string {
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

// CQLPort returns CQL port from NodeInfo.
// `fallbackAddress` argument is needed for Scylla bug workaround, see CQLAddr for description.
func (ni *NodeInfo) CQLPort(fallbackAddress string) string {
	if ni.ClientEncryptionEnabled {
		// Scylla API always returns non-empty NativeTransportPortSSL even when
		// value is explicitly disabled in configuration file.
		// This makes impossible to determine which port is being used for CQL
		// frontend. To workaround it, we try to dial SSL port when
		// client encryption is enabled. If any error happens, assume this port
		// is not used.
		// Ref: https://github.com/scylladb/scylla/issues/7206

		d := &net.Dialer{
			Timeout: time.Second,
		}
		addr := net.JoinHostPort(ni.cqlAddr(fallbackAddress), ni.NativeTransportPortSsl)
		c, err := d.Dial("tcp", addr)
		if err != nil {
			return ni.NativeTransportPort
		}
		defer c.Close()
		return ni.NativeTransportPortSsl
	}
	return ni.NativeTransportPort
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
func (ni *NodeInfo) SupportsAlternatorQuery() (bool, error) {
	sf, err := makeScyllaFeatures(ni.ScyllaVersion)
	if err != nil {
		return false, err
	}
	return sf.AlternatorQueryPing, nil
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

// FreeOSMemory calls debug.FreeOSMemory on the agent to return memory to OS.
func (c *Client) FreeOSMemory(ctx context.Context, host string) error {
	p := operations.FreeOSMemoryParams{
		Context: forceHost(ctx, host),
	}
	_, err := c.agentOps.FreeOSMemory(&p)
	return err
}
