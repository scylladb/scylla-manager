// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"net/http"
	"sync"
	"time"

	api "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/hailocab/go-hostpool"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	rcloneOperations "github.com/scylladb/mermaid/scyllaclient/internal/rclone/client/operations"
	scyllaOperations "github.com/scylladb/mermaid/scyllaclient/internal/scylla/client/operations"
)

var initOnce sync.Once

//go:generate ./gen-internal.sh

// Client provides means to interact with Scylla nodes.
type Client struct {
	config Config
	logger log.Logger

	scyllaOpts *scyllaOperations.Client
	rcloneOpts *rcloneOperations.Client
	transport  http.RoundTripper
}

// NewClient creates new scylla HTTP client.
func NewClient(config Config, logger log.Logger) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	initOnce.Do(func() {
		// Timeout is defined in http client that we provide in api.NewWithClient.
		// If Context is provided to operation, which is always the case here,
		// this value has no meaning since OpenAPI runtime ignores it.
		api.DefaultTimeout = 0
		// Disable debug output to stderr, it could have been enabled by setting
		// SWAGGER_DEBUG or DEBUG env variables.
		middleware.Debug = false
	})

	// Copy hosts
	hosts := make([]string, len(config.Hosts))
	copy(hosts, config.Hosts)

	pool := hostpool.NewEpsilonGreedy(hosts, config.PoolDecayDuration, &hostpool.LinearEpsilonValueCalculator{})

	if config.Transport == nil {
		config.Transport = http.DefaultTransport
	}
	transport := config.Transport
	transport = mwTimeout(transport, config.RequestTimeout)
	transport = mwLogger(transport, logger)
	transport = mwHostPool(transport, pool, config.AgentPort)
	transport = mwRetry(transport, len(config.Hosts), logger)
	transport = mwOpenAPIFix(transport)

	client := api.NewWithClient(
		"mermaid.magic.host", "", []string{"http"},
		&http.Client{
			Timeout:   config.Timeout,
			Transport: transport,
		},
	)
	// debug can be turned on by SWAGGER_DEBUG or DEBUG env variable
	client.Debug = false

	return &Client{
		config:     config,
		logger:     logger,
		scyllaOpts: scyllaOperations.New(client, strfmt.Default),
		rcloneOpts: rcloneOperations.New(client, strfmt.Default),
		transport:  transport,
	}, nil
}

// Timeout returns a timeout for a request.
func (c *Client) Timeout() time.Duration {
	return c.config.Timeout
}
