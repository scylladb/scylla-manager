package dbapi

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/scylladb/mermaid/dbapi/internal/client/operations"
	"github.com/scylladb/mermaid/log"
)

// Client provides means to interact with Scylla nodes. It uses the Scylla REST
// API but provides access to only selected functionalities. Single client can
// be used to communicate with different nodes.
type Client struct {
	operations *operations.Client
	client     *http.Client
	logger     log.Logger
}

// NewClient creates a new client.
func NewClient(l log.Logger) *Client {
	return &Client{
		operations: operations.New(client.New("", "", []string{"http"}), strfmt.Default),
		client: &http.Client{
			Transport: transport{
				parent: http.DefaultTransport,
				logger: l,
			},
		},
		logger: l,
	}
}

// Tokens returns list of tokens in the cluster.
func (c *Client) Tokens(ctx context.Context, host string) ([]int64, error) {
	resp, err := c.operations.GetTokenEndpoint(&operations.GetTokenEndpointParams{
		Context:    withHost(ctx, host),
		HTTPClient: c.client,
	})
	if err != nil {
		return nil, err
	}

	tokens := make([]int64, len(resp.Payload))
	for i, p := range resp.Payload {
		v, err := strconv.ParseInt(p.Key, 10, 64)
		if err != nil {
			return tokens, fmt.Errorf("parsing failed at pos %d: %s", i, err)
		}
		tokens[i] = v
	}

	return tokens, nil
}
