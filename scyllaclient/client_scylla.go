// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/scyllaclient/internal/scylla/client/operations"
	"go.uber.org/multierr"
)

// ClusterName returns cluster name.
func (c *Client) ClusterName(ctx context.Context) (string, error) {
	resp, err := c.scyllaOpts.StorageServiceClusterNameGet(&operations.StorageServiceClusterNameGetParams{Context: ctx})
	if err != nil {
		return "", err
	}

	return resp.Payload, nil
}

// Datacenters returns the available datacenters in this cluster.
func (c *Client) Datacenters(ctx context.Context) (map[string][]string, error) {
	resp, err := c.scyllaOpts.StorageServiceHostIDGet(&operations.StorageServiceHostIDGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}

	res := make(map[string][]string)
	var errs error

	for _, p := range resp.Payload {
		dc, err := c.HostDatacenter(ctx, p.Key)
		if err != nil {
			errs = multierr.Append(errs, err)
			continue
		}
		res[dc] = append(res[dc], p.Key)
	}

	return res, errs
}

// HostDatacenter looks up the datacenter that the given host belongs to.
func (c *Client) HostDatacenter(ctx context.Context, host string) (string, error) {
	resp, err := c.scyllaOpts.SnitchDatacenterGet(&operations.SnitchDatacenterGetParams{
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
	resp, err := c.scyllaOpts.StorageServiceHostIDGet(&operations.StorageServiceHostIDGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}

	v := make([]string, len(resp.Payload))
	for i := 0; i < len(resp.Payload); i++ {
		v[i] = resp.Payload[i].Key
	}
	return v, nil
}

// HostIDs returns a mapping from host IP to UUID.
func (c *Client) HostIDs(ctx context.Context) (map[string]string, error) {
	resp, err := c.scyllaOpts.StorageServiceHostIDGet(&operations.StorageServiceHostIDGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}

	v := make(map[string]string, len(resp.Payload))
	for i := 0; i < len(resp.Payload); i++ {
		v[resp.Payload[i].Key] = resp.Payload[i].Value
	}
	return v, nil
}

// Keyspaces return a list of all the keyspaces.
func (c *Client) Keyspaces(ctx context.Context) ([]string, error) {
	resp, err := c.scyllaOpts.StorageServiceKeyspacesGet(&operations.StorageServiceKeyspacesGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// Tables returns a slice of table names in a given keyspace.
func (c *Client) Tables(ctx context.Context, keyspace string) ([]string, error) {
	resp, err := c.scyllaOpts.ColumnFamilyNameGet(&operations.ColumnFamilyNameGetParams{Context: ctx})
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
	resp, err := c.scyllaOpts.StorageServiceTokensEndpointGet(&operations.StorageServiceTokensEndpointGetParams{Context: ctx})
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

// Partitioner returns cluster partitioner name.
func (c *Client) Partitioner(ctx context.Context) (string, error) {
	resp, err := c.scyllaOpts.StorageServicePartitionerNameGet(&operations.StorageServicePartitionerNameGetParams{Context: ctx})
	if err != nil {
		return "", err
	}

	return resp.Payload, nil
}

// ShardCount returns number of shards in a node.
func (c *Client) ShardCount(ctx context.Context, host string) (uint, error) {
	body, err := c.metrics(ctx, host)
	if err != nil {
		return 0, err
	}
	defer body.Close()

	s := bufio.NewScanner(body)

	var shards uint
	for s.Scan() {
		if strings.HasPrefix(s.Text(), "scylla_database_total_writes{") {
			shards++
		}
	}

	if shards == 0 {
		return 0, errors.New("failed to get shard count")
	}

	return shards, nil
}

// metrics returns Prometheus metrics response body, caller is responsible for
// closing the returned body.
func (c *Client) metrics(ctx context.Context, host string) (io.ReadCloser, error) {
	u := url.URL{
		Scheme: c.config.Scheme,
		Host:   host,
		Path:   "/metrics",
	}

	r, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	r = r.WithContext(forceHost(ctx, host))

	resp, err := c.transport.RoundTrip(r)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// DescribeRing returns a description of token range of a given keyspace.
func (c *Client) DescribeRing(ctx context.Context, keyspace string) (Ring, error) {
	resp, err := c.scyllaOpts.StorageServiceDescribeRingByKeyspaceGet(&operations.StorageServiceDescribeRingByKeyspaceGetParams{
		Context:  ctx,
		Keyspace: keyspace,
	})
	if err != nil {
		return Ring{}, err
	}

	ring := Ring{
		Tokens: make([]TokenRange, len(resp.Payload)),
		HostDC: map[string]string{},
	}
	dcTokens := make(map[string]int)

	for i, p := range resp.Payload {
		// parse tokens
		ring.Tokens[i].StartToken, err = strconv.ParseInt(p.StartToken, 10, 64)
		if err != nil {
			return Ring{}, errors.Wrap(err, "failed to parse StartToken")
		}
		ring.Tokens[i].EndToken, err = strconv.ParseInt(p.EndToken, 10, 64)
		if err != nil {
			return Ring{}, errors.Wrap(err, "failed to parse EndToken")
		}
		// save replicas
		ring.Tokens[i].Replicas = p.Endpoints

		// Update host to DC mapping
		for _, e := range p.EndpointDetails {
			ring.HostDC[e.Host] = e.Datacenter
		}

		// Update DC token mertics
		dcs := strset.New()
		for _, e := range p.EndpointDetails {
			if !dcs.Has(e.Datacenter) {
				dcTokens[e.Datacenter]++
				dcs.Add(e.Datacenter)
			}
		}
	}

	// Detect replication strategy
	if len(ring.HostDC) == 1 {
		ring.Replication = LocalStrategy
	} else {
		ring.Replication = NetworkTopologyStrategy
		for _, tokens := range dcTokens {
			if tokens != len(ring.Tokens) {
				ring.Replication = SimpleStrategy
				break
			}
		}
	}

	return ring, nil
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
	p := operations.StorageServiceRepairAsyncByKeyspacePostParams{
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

	resp, err := c.scyllaOpts.StorageServiceRepairAsyncByKeyspacePost(&p)
	if err != nil {
		return 0, err
	}

	return resp.Payload, nil
}

// RepairStatus returns current status of a repair command.
func (c *Client) RepairStatus(ctx context.Context, host, keyspace string, id int32) (CommandStatus, error) {
	resp, err := c.scyllaOpts.StorageServiceRepairAsyncByKeyspaceGet(&operations.StorageServiceRepairAsyncByKeyspaceGetParams{
		Context:  forceHost(ctx, host),
		Keyspace: keyspace,
		ID:       id,
	})
	if err != nil {
		return "", err
	}

	return CommandStatus(resp.Payload), nil
}

// ActiveRepairs returns a subset of hosts that are coordinators of a repair.
func (c *Client) ActiveRepairs(ctx context.Context, hosts []string) ([]string, error) {
	type hostError struct {
		host   string
		active bool
		err    error
	}
	out := make(chan hostError, runtime.NumCPU()+1)

	for _, h := range hosts {
		h := h
		go func() {
			a, err := c.hasActiveRepair(ctx, h)
			out <- hostError{
				host:   h,
				active: a,
				err:    errors.Wrapf(err, "host %s", h),
			}
		}()
	}

	var (
		active []string
		errs   error
	)
	for range hosts {
		v := <-out
		if v.err != nil {
			errs = multierr.Append(errs, v.err)
		}
		if v.active {
			active = append(active, v.host)
		}
	}
	return active, errs
}

func (c *Client) hasActiveRepair(ctx context.Context, host string) (bool, error) {
	const wait = 50 * time.Millisecond
	for i := 0; i < 10; i++ {
		resp, err := c.scyllaOpts.StorageServiceActiveRepairGet(&operations.StorageServiceActiveRepairGetParams{
			Context: forceHost(ctx, host),
		})
		if err != nil {
			return false, err
		}
		if len(resp.Payload) > 0 {
			return true, nil
		}
		// wait before trying again
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return false, ctx.Err()
		case <-t.C:
		}
	}
	return false, nil
}

// KillAllRepairs forces a termination of all repairs running on a host, the
// operation is not retried to avoid side effects of a deferred kill.
func (c *Client) KillAllRepairs(ctx context.Context, host string) error {
	_, err := c.scyllaOpts.StorageServiceForceTerminateRepairPost(&operations.StorageServiceForceTerminateRepairPostParams{ // nolint: errcheck
		Context: noRetry(forceHost(ctx, host)),
	})
	return err
}

// Snapshots lists available snapshots.
func (c *Client) Snapshots(ctx context.Context, host string) ([]string, error) {
	resp, err := c.scyllaOpts.StorageServiceSnapshotsGet(&operations.StorageServiceSnapshotsGetParams{
		Context: forceHost(ctx, host),
	})
	if err != nil {
		return nil, err
	}

	var tags []string
	for _, p := range resp.Payload {
		tags = append(tags, p.Key)
	}

	return tags, err
}

// TakeSnapshot takes a snapshot of a keyspace, multiple keyspaces may have
// the same tag.
func (c *Client) TakeSnapshot(ctx context.Context, host, tag, keyspace string, tables ...string) error {
	params := &operations.StorageServiceSnapshotsPostParams{
		Context: forceHost(ctx, host),
		Tag:     &tag,
		Kn:      &keyspace,
	}
	if len(tables) > 0 {
		cf := strings.Join(tables, ",")
		params.Cf = &cf
	}

	_, err := c.scyllaOpts.StorageServiceSnapshotsPost(params) // nolint: errcheck
	return err
}

// DeleteSnapshot removes a snapshot with a given tag.
func (c *Client) DeleteSnapshot(ctx context.Context, host, tag string) error {
	_, err := c.scyllaOpts.StorageServiceSnapshotsDelete(&operations.StorageServiceSnapshotsDeleteParams{ // nolint: errcheck
		Context: forceHost(ctx, host),
		Tag:     &tag,
	})
	return err
}
