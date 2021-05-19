// Copyright (C) 2017 ScyllaDB

package enrich

import (
	"context"
	"fmt"
	"net"

	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

// AgentConfigFromAPI fetches address info from the node and updates the configuration.
func AgentConfigFromAPI(ctx context.Context, addr string, c *config.AgentConfig) error {
	external, err := fetchScyllaConfig(ctx, addr)
	if err != nil {
		return err
	}

	c.Scylla.ListenAddress = external.ListenAddress
	c.Scylla.PrometheusAddress = external.PrometheusAddress
	c.Scylla.PrometheusPort = external.PrometheusPort

	if c.HTTPS == "" {
		c.HTTPS = net.JoinHostPort(c.Scylla.ListenAddress, fmt.Sprint(c.HTTPSPort))
	}

	if external.DataDirectory != "" {
		c.Scylla.DataDirectory = external.DataDirectory
	}

	return nil
}

func fetchScyllaConfig(ctx context.Context, addr string) (c config.ScyllaConfig, err error) {
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
	return
}
