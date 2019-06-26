// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"crypto/tls"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"sync"
	"time"

	api "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/hailocab/go-hostpool"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/timeutc"
	rcloneClient "github.com/scylladb/mermaid/scyllaclient/internal/rclone/client"
	rcloneOperations "github.com/scylladb/mermaid/scyllaclient/internal/rclone/client/operations"
	scyllaClient "github.com/scylladb/mermaid/scyllaclient/internal/scylla/client"
	scyllaOperations "github.com/scylladb/mermaid/scyllaclient/internal/scylla/client/operations"
)

var initOnce sync.Once

//go:generate ./internalgen

// DefaultTransport returns a new http.Transport with similar default values to
// http.DefaultTransport. Do not use this for transient transports as it can
// leak file descriptors over time. Only use this for transports that will be
// re-used for the same host(s).
func DefaultTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,

		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
}

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
		config.Transport = DefaultTransport()
	}
	transport := config.Transport
	transport = mwTimeout(transport, config.RequestTimeout)
	transport = mwLogger(transport, logger)
	transport = mwHostPool(transport, pool, config.AgentPort)
	transport = mwRetry(transport, len(config.Hosts), logger)
	transport = mwOpenAPIFix(transport)

	c := &http.Client{
		Timeout:   config.Timeout,
		Transport: transport,
	}

	scyllaRuntime := api.NewWithClient(
		scyllaClient.DefaultHost, scyllaClient.DefaultBasePath, []string{config.Scheme}, c,
	)
	rcloneRuntime := api.NewWithClient(
		rcloneClient.DefaultHost, rcloneClient.DefaultBasePath, []string{config.Scheme}, c,
	)
	// debug can be turned on by SWAGGER_DEBUG or DEBUG env variable
	scyllaRuntime.Debug = false
	rcloneRuntime.Debug = false

	return &Client{
		config:     config,
		logger:     logger,
		scyllaOpts: scyllaOperations.New(scyllaRuntime, strfmt.Default),
		rcloneOpts: rcloneOperations.New(rcloneRuntime, strfmt.Default),
		transport:  transport,
	}, nil
}

// Timeout returns a timeout for a request.
func (c *Client) Timeout() time.Duration {
	return c.config.Timeout
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
				rtt, err := c.PingN(ctx, h, 3)
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
	sorted := make([]string, 0, len(dcs))
	for dc := range dcs {
		sorted = append(sorted, dc)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return min[sorted[i]] < min[sorted[j]]
	})

	// All hosts failed...
	if min[sorted[0]] == math.MaxInt64 {
		return nil, errors.New("failed to connect to any node")
	}

	return sorted, nil
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

// PingN does "n" amount of pings towards the host and returns average RTT
// across all results.
// Pings are tried sequentially and if any of the pings fail function will
// return an error.
func (c *Client) PingN(ctx context.Context, host string, n int) (time.Duration, error) {
	// Open connection to server.
	_, err := c.Ping(ctx, host)
	if err != nil {
		return 0, err
	}

	// Measure avg host RTT.
	mctx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer cancel()
	var sum time.Duration
	for i := 0; i < n; i++ {
		d, err := c.Ping(mctx, host)
		if err != nil {
			return 0, err
		}
		sum += d
	}
	return sum / time.Duration(n), nil
}

// Ping checks if host is available using HTTP ping.
func (c *Client) Ping(ctx context.Context, host string) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(noRetry(ctx), c.config.RequestTimeout)
	defer cancel()

	u := url.URL{
		Scheme: c.config.Scheme,
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
	if resp != nil {
		io.Copy(ioutil.Discard, io.LimitReader(resp.Body, 1024)) // nolint: errcheck
		resp.Body.Close()
	}
	return timeutc.Since(t), err
}

// Close closes all the idle connections.
func (c *Client) Close() error {
	if t, ok := c.config.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}
