// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
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
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/scyllaclient/internal/client/operations"
	"go.uber.org/multierr"
)

// DefaultAPIPort is Scylla API port.
var (
	DefaultAPIPort        = "10000"
	DefaultPrometheusPort = "9180"

	// Timeout specifies end-to-end time to complete Scylla REST API request
	// including retries.
	Timeout = 30 * time.Second

	// RequestTimeout specifies time to complete a single request to Scylla
	// REST API possibly including opening SSH tunneled connection.
	RequestTimeout = 5 * time.Second

	// PoolDecayDuration specifies size of time window to measure average
	// request time in Epsilon-Greedy host pool.
	PoolDecayDuration = 30 * time.Minute
)

var initOnce sync.Once

//go:generate ./gen_internal.sh

// Client provides means to interact with Scylla nodes.
type Client struct {
	operations *operations.Client
	inner      http.RoundTripper
	transport  http.RoundTripper
	logger     log.Logger
}

func NewClient(hosts []string, transport http.RoundTripper, l log.Logger) (*Client, error) {
	if len(hosts) == 0 {
		return nil, errors.New("missing hosts")
	}

	inner := transport

	initOnce.Do(func() {
		// Timeout is defined in http client that we provide in api.NewWithClient.
		// If Context is provided to operation, which is always the case here,
		// this value has no meaning since OpenAPI runtime ignores it.
		api.DefaultTimeout = 0
		// Disable debug output to stderr, it could have been enabled by setting
		// SWAGGER_DEBUG or DEBUG env variables.
		middleware.Debug = false
	})

	addrs := make([]string, len(hosts))
	for i, h := range hosts {
		addrs[i] = withPort(h, DefaultAPIPort)
	}
	pool := hostpool.NewEpsilonGreedy(addrs, PoolDecayDuration, &hostpool.LinearEpsilonValueCalculator{})

	transport = mwTimeout(transport, RequestTimeout)
	transport = mwLogger(transport, l)
	transport = mwHostPool(transport, pool)
	transport = mwRetry(transport, len(hosts), l)
	transport = mwOpenAPIFix(transport)

	client := api.NewWithClient(
		"mermaid.magic.host", "", []string{"http"},
		&http.Client{
			Timeout:   Timeout,
			Transport: transport,
		},
	)
	// debug can be turned on by SWAGGER_DEBUG or DEBUG env variable
	client.Debug = false

	return &Client{
		operations: operations.New(client, strfmt.Default),
		inner:      inner,
		transport:  transport,
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
	resp, err := c.operations.GetClusterName(&operations.GetClusterNameParams{Context: ctx})
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
	resp, err := c.operations.GetHostIDMap(&operations.GetHostIDMapParams{Context: ctx})
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
	resp, err := c.operations.GetDatacenter(&operations.GetDatacenterParams{
		Context: ctx,
		Host:    &host,
	})
	if err != nil {
		return "", err
	}
	return resp.Payload, nil
}

// Hosts returns a list of all hosts in a cluster.
func (c *Client) Hosts(ctx context.Context) ([]string, error) {
	resp, err := c.operations.GetHostIDMap(&operations.GetHostIDMapParams{Context: ctx})
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
	resp, err := c.operations.GetKeyspaces(&operations.GetKeyspacesParams{Context: ctx})
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

// ClosestDC takes output of Datacenters, a map from DC to it's hosts and
// returns DCs sorted by speed the hosts respond. It's determined by
// the lowest latency over 3 Ping() invocations across random selection of
// hosts for each DC.
func (c *Client) ClosestDC(ctx context.Context, dcs map[string][]string) ([]string, error) {
	if len(dcs) == 0 {
		return nil, errors.Errorf("no dcs to choose from")
	}

	// Single DC no need to measure anything.
	if len(dcs) == 1 {
		for dc := range dcs {
			return []string{dc}, nil
		}
	}

	type dcRTT struct {
		dc  string
		rtt time.Duration
	}
	out := make(chan dcRTT, runtime.NumCPU()+1)
	size := 0

	// Test latency of 3 random hosts from each DC.
	for dc, hosts := range dcs {
		dc := dc
		hosts := pickNRandomHosts(3, hosts)
		size += len(hosts)

		for _, h := range hosts {
			h := h
			go func() {
				rtt, err := c.measure(ctx, h, 3)
				if err != nil {
					c.logger.Info(ctx, "Host RTT measurement failed",
						"dc", dc,
						"host", h,
						"err", err,
					)
					rtt = math.MaxInt64
				}
				out <- dcRTT{dc: dc, rtt: rtt}
			}()
		}
	}

	// Select the lowest latency for each DC.
	min := make(map[string]time.Duration, len(dcs))
	for i := 0; i < size; i++ {
		v := <-out
		if m, ok := min[v.dc]; !ok || m > v.rtt {
			min[v.dc] = v.rtt
		}
	}

	// Sort DCs by lowest latency.
	sored := make([]string, 0, len(dcs))
	for dc := range dcs {
		sored = append(sored, dc)
	}
	sort.Slice(sored, func(i, j int) bool {
		return min[sored[i]] < min[sored[j]]
	})

	// All hosts failed...
	if min[sored[0]] == math.MaxInt64 {
		return nil, errors.New("failed to connect to any node")
	}

	return sored, nil
}

func pickNRandomHosts(n int, hosts []string) []string {
	if n >= len(hosts) {
		return hosts
	}

	rand := rand.New(rand.NewSource(timeutc.Now().UnixNano()))

	idxs := make(map[int]struct{})
	rh := make([]string, 0, n)
	for ; n > 0; n-- {
		idx := rand.Intn(len(hosts))
		if _, ok := idxs[idx]; !ok {
			idxs[idx] = struct{}{}
			rh = append(rh, hosts[idx])
		} else {
			n++
		}
	}
	return rh
}

func (c *Client) measure(ctx context.Context, host string, laps int) (time.Duration, error) {
	// Open connection to server.
	_, err := c.Ping(ctx, host)
	if err != nil {
		return 0, err
	}

	// Measure avg host RTT.
	mctx, cancel := context.WithTimeout(ctx, time.Duration(laps)*500*time.Millisecond)
	defer cancel()
	var sum time.Duration
	for i := 0; i < laps; i++ {
		d, err := c.Ping(mctx, host)
		if err != nil {
			return 0, err
		}
		sum += d
	}
	return sum / time.Duration(laps), nil
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
	resp, err := c.operations.GetPartitionerName(&operations.GetPartitionerNameParams{Context: ctx})
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
		hosts := strings.Join(config.Hosts, ",") + "," + host
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
	resp, err := c.operations.GetColumnFamilyName(&operations.GetColumnFamilyNameParams{Context: ctx})
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
	resp, err := c.operations.GetTokenEndpoint(&operations.GetTokenEndpointParams{Context: ctx})
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
	ctx, cancel := context.WithTimeout(noRetry(ctx), RequestTimeout)
	defer cancel()

	u := url.URL{Scheme: "http", Host: host, Path: "/"}
	r, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, err
	}
	r = r.WithContext(forceHost(ctx, host))

	t := timeutc.Now()
	resp, err := c.transport.RoundTrip(r)
	if resp != nil {
		io.Copy(ioutil.Discard, io.LimitReader(resp.Body, 1024)) // nolint: errcheck
		resp.Body.Close()
	}
	return timeutc.Since(t), err
}

// Close closes all the idle connections.
func (c *Client) Close() error {
	if t, ok := c.inner.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}
