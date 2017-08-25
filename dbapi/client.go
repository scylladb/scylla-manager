package dbapi

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/cespare/xxhash"
	"github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/hailocab/go-hostpool" // shipped with gocql
	"github.com/scylladb/mermaid"
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
func NewClient(l log.Logger, hosts []string) *Client {
	hp := hostpool.NewEpsilonGreedy(hosts, 0, &hostpool.LinearEpsilonValueCalculator{})

	return &Client{
		operations: operations.New(client.New("", "", []string{"http"}), strfmt.Default),
		client: &http.Client{
			Transport: transport{
				parent: http.DefaultTransport,
				pool:   hp,
				logger: l,
			},
		},
		logger: l,
	}
}

// DatacenterHosts returns addresses of endpoints in a given datacenter.
func (c *Client) DatacenterHosts(ctx context.Context, dc, keyspace string) ([]string, error) {
	resp, err := c.operations.DescribeRing(&operations.DescribeRingParams{
		Context:    ctx,
		HTTPClient: c.client,
		Keyspace:   keyspace,
	})
	if err != nil {
		return nil, err
	}

	h := mermaid.Uniq{}
	for _, p := range resp.Payload {
		for _, e := range p.EndpointDetails {
			if e.Datacenter == dc {
				h.Put(e.Host)
			}
		}
	}

	return h.Slice(), nil
}

// Tokens returns list of tokens in a cluster.
func (c *Client) Tokens(ctx context.Context) ([]int64, error) {
	resp, err := c.operations.GetTokenEndpoint(&operations.GetTokenEndpointParams{
		Context:    ctx,
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

// TopologyHash returns hash of all tokens.
func (c *Client) TopologyHash(ctx context.Context) (uint64, error) {
	tokens, err := c.Tokens(ctx)
	if err != nil {
		return 0, err
	}

	var (
		xx = xxhash.New()
		b  = make([]byte, 8)
		u  uint64
	)
	for _, t := range tokens {
		if t >= 0 {
			u = uint64(t)
		} else {
			u = uint64(math.MaxInt64 + t)
		}
		binary.LittleEndian.PutUint64(b, u)
		xx.Write(b)
	}
	return xx.Sum64(), nil
}
