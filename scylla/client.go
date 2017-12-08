// Copyright (C) 2017 ScyllaDB

package scylla

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"

	api "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/hailocab/go-hostpool" // shipped with gocql
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/scylla/internal/client/operations"
)

// DefaultPort is Scylla API port.
var DefaultPort = "10000"

//go:generate ./gen_internal.sh

// Client provides means to interact with Scylla nodes.
type Client struct {
	operations *operations.Client
	client     *http.Client
	logger     log.Logger

	config Config
}

// WithConfig is a temporary solution until core exposes the configuration API.
// https://github.com/scylladb/scylla/issues/2761
func WithConfig(c *Client, config Config) *Client {
	c.config = config
	return c
}

// NewClient creates a new client.
func NewClient(hosts []string, rt http.RoundTripper, l log.Logger) (*Client, error) {
	if len(hosts) == 0 {
		return nil, errors.New("missing hosts")
	}

	addrs := make([]string, len(hosts))
	for i, h := range hosts {
		addrs[i] = withPort(h)
	}
	pool := hostpool.NewEpsilonGreedy(addrs, 0, &hostpool.LinearEpsilonValueCalculator{})

	return &Client{
		operations: operations.New(api.New("", "", []string{"http"}), strfmt.Default),
		client: &http.Client{
			Transport: transport{
				parent: rt,
				pool:   pool,
				logger: l,
			},
		},
		logger: l,
	}, nil
}

func withPort(host string) string {
	_, port, _ := net.SplitHostPort(host)
	if port != "" {
		return host
	}

	return fmt.Sprint(host, ":", DefaultPort)
}

// Datacenter returns the local datacenter name.
func (c *Client) Datacenter(ctx context.Context) (string, error) {
	resp, err := c.operations.GetDatacenter(&operations.GetDatacenterParams{
		Context:    ctx,
		HTTPClient: c.client,
	})
	if err != nil {
		return "", err
	}

	return resp.Payload, nil
}

// Keyspaces retrurn a list of all the keyspaces.
func (c *Client) Keyspaces(ctx context.Context) ([]string, error) {
	resp, err := c.operations.GetKeyspaces(&operations.GetKeyspacesParams{
		Context:    ctx,
		HTTPClient: c.client,
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(resp.Payload)

	return resp.Payload, nil
}

// DescribeRing returns list of datacenters and a token range description
// for a given keyspace.
func (c *Client) DescribeRing(ctx context.Context, keyspace string) ([]string, []*TokenRange, error) {
	resp, err := c.operations.DescribeRing(&operations.DescribeRingParams{
		Context:    ctx,
		HTTPClient: c.client,
		Keyspace:   keyspace,
	})
	if err != nil {
		return nil, nil, err
	}

	var (
		dcs = mermaid.Uniq{}
		trs = make([]*TokenRange, len(resp.Payload))
	)
	for i, p := range resp.Payload {
		// allocate memory
		trs[i] = new(TokenRange)
		r := trs[i]

		// parse tokens
		r.StartToken, err = strconv.ParseInt(p.StartToken, 10, 64)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to parse StartToken")
		}
		r.EndToken, err = strconv.ParseInt(p.EndToken, 10, 64)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to parse EndToken")
		}

		// group hosts into datacenters
		if len(dcs) == 0 {
			r.Hosts = make(map[string][]string, 5)
		} else {
			r.Hosts = make(map[string][]string, len(dcs))
		}
		for _, e := range p.EndpointDetails {
			dcs.Put(e.Datacenter)
			r.Hosts[e.Datacenter] = append(r.Hosts[e.Datacenter], e.Host)
		}
	}

	return dcs.Slice(), trs, nil
}

// HostConfig returns configuration of a node.
func (c *Client) HostConfig(ctx context.Context, host string) (Config, error) {
	if c.config == nil {
		return nil, errors.New("no host config")
	}
	return c.config, nil
}

// HostPendingCompactions returns number of pending compactions on a host.
func (c *Client) HostPendingCompactions(ctx context.Context, host string) (int32, error) {
	resp, err := c.operations.GetAllPendingCompactions(&operations.GetAllPendingCompactionsParams{
		Context:    withHostPort(ctx, host),
		HTTPClient: c.client,
	})
	if err != nil {
		return 0, err
	}

	return resp.Payload, nil
}

// Partitioner returns cluster partitioner name.
func (c *Client) Partitioner(ctx context.Context) (string, error) {
	resp, err := c.operations.GetPartitionerName(&operations.GetPartitionerNameParams{
		Context:    ctx,
		HTTPClient: c.client,
	})
	if err != nil {
		return "", err
	}

	return resp.Payload, nil
}

// RepairConfig specifies what to repair.
type RepairConfig struct {
	Keyspace string
	Tables   []string
	Ranges   string
}

// Repair invokes async repair and returns the repair command ID.
func (c *Client) Repair(ctx context.Context, host string, config *RepairConfig) (int32, error) {
	p := operations.RepairAsyncParams{
		Context:    withHostPort(ctx, host),
		HTTPClient: c.client,
		Keyspace:   config.Keyspace,
		Ranges:     &config.Ranges,
	}
	if config.Tables != nil {
		tables := strings.Join(config.Tables, ",")
		p.ColumnFamilies = &tables
	}

	resp, err := c.operations.RepairAsync(&p)
	if err != nil {
		return 0, err
	}

	return resp.Payload, nil
}

// RepairStatus returns current status of a repair command.
func (c *Client) RepairStatus(ctx context.Context, host, keyspace string, id int32) (CommandStatus, error) {
	resp, err := c.operations.RepairAsyncStatus(&operations.RepairAsyncStatusParams{
		Context:    withHostPort(ctx, host),
		HTTPClient: c.client,
		Keyspace:   keyspace,
		ID:         id,
	})
	if err != nil {
		return "", err
	}

	return CommandStatus(resp.Payload), nil
}

// Tables returns a slice of table names in a given keyspace.
func (c *Client) Tables(ctx context.Context, keyspace string) ([]string, error) {
	resp, err := c.operations.GetColumnFamilyName(&operations.GetColumnFamilyNameParams{
		Context:    ctx,
		HTTPClient: c.client,
	})
	if err != nil {
		return nil, err
	}

	var (
		prefix = keyspace + ":"
		tables []string
	)
	for _, v := range resp.Payload {
		if strings.HasPrefix(v, prefix) {
			tables = append(tables, v[len(prefix):])
		}
	}

	return tables, nil
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
