// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	api "github.com/scylladb/mermaid/scyllaclient/internal/scylla_v2/client"
	"github.com/scylladb/mermaid/scyllaclient/internal/scylla_v2/client/config"
)

// ConfigClient provides means to interact with Scylla config API on a given
// host if it's directly accessible.
type ConfigClient struct {
	client *api.Scylla2
}

func NewConfigClient(addr string) *ConfigClient {
	t := api.DefaultTransportConfig().WithHost(addr)
	return &ConfigClient{
		client: api.NewHTTPClientWithConfig(strfmt.Default, t),
	}
}

// ListenAddress returns node listen address.
func (c *ConfigClient) ListenAddress(ctx context.Context) (string, error) {
	resp, err := c.client.Config.FindConfigListenAddress(config.NewFindConfigListenAddressParamsWithContext(ctx))
	if err != nil {
		return "", err
	}
	return resp.Payload, err
}

// BroadcastAddress returns node broadcast address.
func (c *ConfigClient) BroadcastAddress(ctx context.Context) (string, error) {
	resp, err := c.client.Config.FindConfigBroadcastAddress(config.NewFindConfigBroadcastAddressParamsWithContext(ctx))
	if err != nil {
		return "", err
	}
	return resp.Payload, err
}

// PrometheusAddress returns node prometheus address.
func (c *ConfigClient) PrometheusAddress(ctx context.Context) (string, error) {
	resp, err := c.client.Config.FindConfigPrometheusAddress(config.NewFindConfigPrometheusAddressParamsWithContext(ctx))
	if err != nil {
		return "", err
	}
	return resp.Payload, err
}

// PrometheusPort returns node prometheus port.
func (c *ConfigClient) PrometheusPort(ctx context.Context) (string, error) {
	resp, err := c.client.Config.FindConfigPrometheusPort(config.NewFindConfigPrometheusPortParamsWithContext(ctx))
	if err != nil {
		return "", err
	}
	return fmt.Sprint(resp.Payload), err
}
