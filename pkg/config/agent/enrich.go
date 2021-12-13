// Copyright (C) 2017 ScyllaDB

package agent

import (
	"context"
	"fmt"
	"net"

	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

// EnrichConfigFromAPI fetches address info from the node and updates the configuration.
func EnrichConfigFromAPI(ctx context.Context, addr string, c *Config) error {
	scyllaConfig, err := scyllaConfigFromAPI(ctx, addr)
	if err != nil {
		return err
	}
	c.Scylla = scyllaConfig

	if c.HTTPS == "" {
		c.HTTPS = net.JoinHostPort(c.Scylla.ListenAddress, fmt.Sprint(c.HTTPSPort))
	}

	return nil
}

func scyllaConfigFromAPI(ctx context.Context, addr string) (c ScyllaConfig, err error) {
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
