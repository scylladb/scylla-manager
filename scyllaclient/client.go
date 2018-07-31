// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	api "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/hailocab/go-hostpool" // shipped with gocql
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/retryablehttp"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/scyllaclient/internal/client/operations"
	"go.uber.org/multierr"
)

// DefaultAPIPort is Scylla API port.
var (
	DefaultAPIPort        = "10000"
	DefaultPrometheusPort = "9180"
)

var disableOpenAPIDebugOnce sync.Once

//go:generate ./gen_internal.sh

// Client provides means to interact with Scylla nodes.
type Client struct {
	transport  http.RoundTripper
	operations *operations.Client
	logger     log.Logger
}

// NewClient creates a new client.
func NewClient(hosts []string, rt http.RoundTripper, l log.Logger) (*Client, error) {
	if len(hosts) == 0 {
		return nil, errors.New("missing hosts")
	}

	addrs := make([]string, len(hosts))
	for i, h := range hosts {
		addrs[i] = withPort(h, DefaultAPIPort)
	}
	pool := hostpool.NewEpsilonGreedy(addrs, 0, &hostpool.LinearEpsilonValueCalculator{})

	t := retryablehttp.NewTransport(transport{
		parent: rt,
		pool:   pool,
		logger: l,
	}, l)
	t.CheckRetry = func(req *http.Request, resp *http.Response, err error) (bool, error) {
		// do not retry ping
		if resp != nil && resp.Request.URL.Path == "/" {
			return false, nil
		}
		return retryablehttp.DefaultRetryPolicy(req, resp, err)
	}

	disableOpenAPIDebugOnce.Do(func() {
		middleware.Debug = false
	})

	r := api.NewWithClient("mermaid.magic.host", "", []string{"http"},
		&http.Client{
			Timeout:   mermaid.DefaultRPCTimeout,
			Transport: t,
		},
	)
	// debug can be turned on by SWAGGER_DEBUG or DEBUG env variable
	r.Debug = false
	return &Client{
		transport:  t,
		operations: operations.New(r, strfmt.Default),
		logger:     l,
	}, nil
}

func withPort(hostPort, port string) string {
	_, p, _ := net.SplitHostPort(hostPort)
	if p != "" {
		return hostPort
	}

	return fmt.Sprint(hostPort, ":", port)
}

// ClusterName returns cluster name.
func (c *Client) ClusterName(ctx context.Context) (string, error) {
	resp, err := c.operations.GetClusterName(&operations.GetClusterNameParams{
		Context: ctx,
	})
	if err != nil {
		return "", err
	}

	return resp.Payload, nil
}

// Datacenter returns the local datacenter name.
func (c *Client) Datacenter(ctx context.Context) (string, error) {
	resp, err := c.operations.GetDatacenter(&operations.GetDatacenterParams{
		Context: ctx,
	})
	if err != nil {
		return "", err
	}

	return resp.Payload, nil
}

type dcHost struct {
	dc   string
	host string
	err  error
}

// Datacenters returns the available datacenters in this cluster.
func (c *Client) Datacenters(ctx context.Context) (map[string][]string, error) {
	resp, err := c.operations.GetHostIDMap(operations.NewGetHostIDMapParams().WithContext(ctx))
	if err != nil {
		return nil, err
	}

	out := make(chan dcHost, runtime.NumCPU()+1)

	for _, p := range resp.Payload {
		go func(ctx context.Context, out chan dcHost, host string) {
			dh := dcHost{
				host: host,
			}
			dh.dc, dh.err = c.HostDatacenter(ctx, host)
			out <- dh
		}(ctx, out, p.Key)
	}

	res := make(map[string][]string)
	var errs error

	for range resp.Payload {
		dcHost := <-out
		if dcHost.err != nil {
			errs = multierr.Append(errs, err)
			continue
		}
		if dc, ok := res[dcHost.dc]; ok {
			res[dcHost.dc] = append(dc, dcHost.host)
		} else {
			res[dcHost.dc] = []string{dcHost.host}
		}
	}

	return res, errs
}

// HostDatacenter looks up the datacenter that the given host belongs to.
func (c *Client) HostDatacenter(ctx context.Context, host string) (string, error) {
	resp, err := c.operations.GetDatacenter(operations.NewGetDatacenterParams().WithContext(ctx).WithHost(&host))
	if err != nil {
		return "", err
	}
	return resp.Payload, nil
}

// Hosts returns a list of all hosts in a cluster.
func (c *Client) Hosts(ctx context.Context) ([]string, error) {
	resp, err := c.operations.GetHostIDMap(operations.NewGetHostIDMapParams().WithContext(ctx))
	if err != nil {
		return nil, err
	}

	v := make([]string, len(resp.Payload))
	for i := 0; i < len(resp.Payload); i++ {
		v[i] = resp.Payload[i].Key
	}
	return v, nil
}

// Keyspaces return a list of all the keyspaces.
func (c *Client) Keyspaces(ctx context.Context) ([]string, error) {
	resp, err := c.operations.GetKeyspaces(&operations.GetKeyspacesParams{
		Context: ctx,
	})
	if err != nil {
		return nil, err
	}

	var v []string
	for _, s := range resp.Payload {
		// ignore system tables on old Scylla versions
		// see https://github.com/scylladb/scylla/issues/1380
		if !strings.HasPrefix(s, "system") {
			v = append(v, s)
		}
	}
	sort.Strings(v)

	return v, nil
}

// DescribeRing returns list of datacenters and a token range description
// for a given keyspace.
func (c *Client) DescribeRing(ctx context.Context, keyspace string) ([]string, []*TokenRange, error) {
	resp, err := c.operations.DescribeRing(&operations.DescribeRingParams{
		Context:  ctx,
		Keyspace: keyspace,
	})
	if err != nil {
		return nil, nil, err
	}

	var (
		dcs = strset.New()
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
		if dcs.Size() == 0 {
			r.Hosts = make(map[string][]string, 5)
		} else {
			r.Hosts = make(map[string][]string, dcs.Size())
		}
		for _, e := range p.EndpointDetails {
			dcs.Add(e.Datacenter)
			r.Hosts[e.Datacenter] = append(r.Hosts[e.Datacenter], e.Host)
		}
	}

	return dcs.List(), trs, nil
}

// ClosestDC takes set of DCs with hosts and return the one that is closest.
// Close is determined as the lowest latency over 10 Ping() invocations across a
// random selection of hosts for each DC.
func (c *Client) ClosestDC(ctx context.Context, dcs map[string][]string) (string, error) {
	if len(dcs) == 0 {
		return "", errors.Errorf("no dcs to choose from")
	}

	var (
		fastestDC   string
		fastestTime int64 = math.MaxInt64
	)
	r := rand.New(rand.NewSource(timeutc.Now().UnixNano()))
	for dc, hosts := range dcs {
		hosts = pickNRandomHosts(r, 2, hosts)
		ts, err := c.measureHosts(ctx, hosts)
		if err != nil {
			return "", errors.Wrapf(err, "unable to measure the latency of the hosts %s", hosts)
		}
		if ts < fastestTime {
			fastestDC = dc
			fastestTime = ts
		}
	}

	return fastestDC, nil
}

func pickNRandomHosts(r *rand.Rand, n int, hosts []string) []string {
	if n >= len(hosts) {
		return hosts
	}

	idxs := make(map[int]struct{})
	rh := make([]string, 0, n)
	for ; n > 0; n-- {
		idx := r.Intn(len(hosts))
		if _, ok := idxs[idx]; !ok {
			idxs[idx] = struct{}{}
			rh = append(rh, hosts[idx])
		} else {
			n++
		}
	}
	return rh
}

func (c *Client) measureHosts(ctx context.Context, hosts []string) (int64, error) {
	var min int64 = math.MaxInt64
	for _, host := range hosts {
		cur, err := c.measure(ctx, host, 3)
		if err != nil {
			return 0, err
		}
		if cur < min {
			min = cur
		}
	}
	return min, nil
}
func (c *Client) measure(ctx context.Context, host string, laps int) (int64, error) {
	_, err := c.Ping(ctx, host)
	if err != nil {
		return 0, err
	}
	var sum int64

	for i := 0; i < laps; i++ {
		d, err := c.Ping(ctx, host)
		if err != nil {
			return 0, err
		}
		sum += int64(d)
	}

	return sum / int64(laps), nil
}

// HostPendingCompactions returns number of pending compactions on a host.
func (c *Client) HostPendingCompactions(ctx context.Context, host string) (int32, error) {
	resp, err := c.operations.GetAllPendingCompactions(&operations.GetAllPendingCompactionsParams{
		Context: forceHost(ctx, host),
	})
	if err != nil {
		return 0, err
	}

	return resp.Payload, nil
}

// Partitioner returns cluster partitioner name.
func (c *Client) Partitioner(ctx context.Context) (string, error) {
	resp, err := c.operations.GetPartitionerName(&operations.GetPartitionerNameParams{
		Context: ctx,
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
	DC       []string
	Hosts    []string
	Ranges   string
}

// Repair invokes async repair and returns the repair command ID.
func (c *Client) Repair(ctx context.Context, host string, config *RepairConfig) (int32, error) {
	p := operations.RepairAsyncParams{
		Context:  forceHost(ctx, host),
		Keyspace: config.Keyspace,
		Ranges:   &config.Ranges,
	}

	if config.Tables != nil {
		tables := strings.Join(config.Tables, ",")
		p.ColumnFamilies = &tables
	}
	if len(config.DC) > 0 {
		dcs := strings.Join(config.DC, ",")
		p.DataCenters = &dcs
	}
	if len(config.Hosts) > 0 {
		hosts := strings.Join(config.Hosts, ",")
		p.Hosts = &hosts
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
		Context:  forceHost(ctx, host),
		Keyspace: keyspace,
		ID:       id,
	})
	if err != nil {
		return "", err
	}

	return CommandStatus(resp.Payload), nil
}

// ShardCount returns number of shards in a node.
func (c *Client) ShardCount(ctx context.Context, host string) (uint, error) {
	u := url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/metrics",
	}

	r, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, err
	}
	r = r.WithContext(context.WithValue(ctx, ctxHost, withPort(host, DefaultPrometheusPort)))

	resp, err := c.transport.RoundTrip(r)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var shards uint
	s := bufio.NewScanner(resp.Body)
	for s.Scan() {
		if strings.HasPrefix(s.Text(), "scylla_database_total_writes{") {
			shards++
		}
	}

	return shards, nil
}

// Tables returns a slice of table names in a given keyspace.
func (c *Client) Tables(ctx context.Context, keyspace string) ([]string, error) {
	resp, err := c.operations.GetColumnFamilyName(&operations.GetColumnFamilyNameParams{
		Context: ctx,
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
		Context: ctx,
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

// Ping checks if host is available using HTTP ping.
func (c *Client) Ping(ctx context.Context, host string) (time.Duration, error) {
	u := url.URL{
		Scheme: "http",
		Host:   host,
		Path:   "/",
	}

	r, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, err
	}
	r = r.WithContext(forceHost(ctx, host))

	t := timeutc.Now()
	resp, err := c.transport.RoundTrip(r)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	return timeutc.Since(t), nil
}
