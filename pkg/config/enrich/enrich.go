// Copyright (C) 2017 ScyllaDB

package enrich

import (
	"context"
	"net"

	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

// AgentConfigFromAPI fetches address info from the node and updates the configuration.
func AgentConfigFromAPI(ctx context.Context, addr string, c *config.AgentConfig) error {
	scyllaConfig, err := scyllaConfigFromAPI(ctx, addr)
	if err != nil {
		return err
	}
	c.Scylla = scyllaConfig

	if c.HTTPS == "" {
		c.HTTPS = net.JoinHostPort(c.Scylla.ListenAddress, config.DefaultAgentHTTPSPort)
	}

	return nil
}

func scyllaConfigFromAPI(ctx context.Context, addr string) (c config.ScyllaConfig, err error) {
	c.APIAddress, c.APIPort, _ = net.SplitHostPort(addr)

	client := scyllaclient.NewConfigClient(addr)
	if c.ListenAddress, err = client.ListenAddress(ctx); err != nil {
		return
	}
	if c.PrometheusAddress, err = client.PrometheusAddress(ctx); err != nil {
		return
	}
	if c.PrometheusPort, err = client.PrometheusPort(ctx); err != nil {
		return
	}
	if c.DataDirectory, err = client.DataDirectory(ctx); err != nil {
		return
	}

	if c.BroadcastRPCAddress, err = client.BroadcastRPCAddress(ctx); err != nil {
		return
	}
	if c.RPCAddress, err = client.RPCAddress(ctx); err != nil {
		return
	}

	return
}
