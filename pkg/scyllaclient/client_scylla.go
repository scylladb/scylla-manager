// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/scyllaclient/internal/scylla/client/operations"
	"github.com/scylladb/mermaid/pkg/util/prom"
	"go.uber.org/multierr"
)

// ClusterName returns cluster name.
func (c *Client) ClusterName(ctx context.Context) (string, error) {
	resp, err := c.scyllaOps.StorageServiceClusterNameGet(&operations.StorageServiceClusterNameGetParams{Context: ctx})
	if err != nil {
		return "", err
	}

	return resp.Payload, nil
}

// Status returns nodetool status alike information, items are sorted by
// Datacenter and Address.
func (c *Client) Status(ctx context.Context) (NodeStatusInfoSlice, error) {
	// Get all hosts
	resp, err := c.scyllaOps.StorageServiceHostIDGet(&operations.StorageServiceHostIDGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}

	all := make([]NodeStatusInfo, len(resp.Payload))
	for i, p := range resp.Payload {
		all[i].Addr = p.Key
		all[i].HostID = p.Value
	}

	// Get host datacenter (hopefully cached)
	for i := range all {
		all[i].Datacenter, err = c.HostDatacenter(ctx, all[i].Addr)
		if err != nil {
			return nil, err
		}
	}

	// Get live nodes
	live, err := c.scyllaOps.GossiperEndpointLiveGet(&operations.GossiperEndpointLiveGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}
	setNodeStatus(all, NodeStatusUp, live.Payload)

	// Get joining nodes
	joining, err := c.scyllaOps.StorageServiceNodesJoiningGet(&operations.StorageServiceNodesJoiningGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}
	setNodeState(all, NodeStateJoining, joining.Payload)

	// Get leaving nodes
	leaving, err := c.scyllaOps.StorageServiceNodesLeavingGet(&operations.StorageServiceNodesLeavingGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}
	setNodeState(all, NodeStateLeaving, leaving.Payload)

	// Get moving nodes
	moving, err := c.scyllaOps.StorageServiceNodesMovingGet(&operations.StorageServiceNodesMovingGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}
	setNodeState(all, NodeStateMoving, moving.Payload)

	// Sort by Datacenter and Address
	sort.Slice(all, func(i, j int) bool {
		if all[i].Datacenter != all[j].Datacenter {
			return all[i].Datacenter < all[j].Datacenter
		}
		return all[i].Addr < all[j].Addr
	})

	return all, nil
}

func setNodeStatus(all []NodeStatusInfo, status NodeStatus, addrs []string) {
	if len(addrs) == 0 {
		return
	}
	m := strset.New(addrs...)

	for i := range all {
		if m.Has(all[i].Addr) {
			all[i].Status = status
		}
	}
}

func setNodeState(all []NodeStatusInfo, state NodeState, addrs []string) {
	if len(addrs) == 0 {
		return
	}
	m := strset.New(addrs...)

	for i := range all {
		if m.Has(all[i].Addr) {
			all[i].State = state
		}
	}
}

// Datacenters returns the available datacenters in this cluster.
func (c *Client) Datacenters(ctx context.Context) (map[string][]string, error) {
	resp, err := c.scyllaOps.StorageServiceHostIDGet(&operations.StorageServiceHostIDGetParams{Context: ctx})
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
func (c *Client) HostDatacenter(ctx context.Context, host string) (dc string, err error) {
	// Try reading from cache
	c.mu.RLock()
	dc = c.dcCache[host]
	c.mu.RUnlock()
	if dc != "" {
		return
	}

	resp, err := c.scyllaOps.SnitchDatacenterGet(&operations.SnitchDatacenterGetParams{
		Context: ctx,
		Host:    &host,
	})
	if err != nil {
		return "", err
	}
	dc = resp.Payload

	// Update cache
	c.mu.Lock()
	c.dcCache[host] = dc
	c.mu.Unlock()

	return
}

// HostIDs returns a mapping from host IP to UUID.
func (c *Client) HostIDs(ctx context.Context) (map[string]string, error) {
	resp, err := c.scyllaOps.StorageServiceHostIDGet(&operations.StorageServiceHostIDGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}

	v := make(map[string]string, len(resp.Payload))
	for i := 0; i < len(resp.Payload); i++ {
		v[resp.Payload[i].Key] = resp.Payload[i].Value
	}
	return v, nil
}

// CheckHostsChanged returns true iff a host was added or removed from cluster.
// In such a case the client should be discarded.
func (c *Client) CheckHostsChanged(ctx context.Context) (bool, error) {
	cur, err := c.hosts(ctx)
	if err != nil {
		return false, err
	}
	if len(cur) != len(c.config.Hosts) {
		return true, err
	}
	return !strset.New(c.config.Hosts...).Has(cur...), nil
}

// hosts returns a list of all hosts in a cluster.
func (c *Client) hosts(ctx context.Context) ([]string, error) {
	resp, err := c.scyllaOps.StorageServiceHostIDGet(&operations.StorageServiceHostIDGetParams{Context: ctx})
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
	resp, err := c.scyllaOps.StorageServiceKeyspacesGet(&operations.StorageServiceKeyspacesGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// Tables returns a slice of table names in a given keyspace.
func (c *Client) Tables(ctx context.Context, keyspace string) ([]string, error) {
	resp, err := c.scyllaOps.ColumnFamilyNameGet(&operations.ColumnFamilyNameGetParams{Context: ctx})
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
	resp, err := c.scyllaOps.StorageServiceTokensEndpointGet(&operations.StorageServiceTokensEndpointGetParams{Context: ctx})
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
	resp, err := c.scyllaOps.StorageServicePartitionerNameGet(&operations.StorageServicePartitionerNameGetParams{Context: ctx})
	if err != nil {
		return "", err
	}

	return resp.Payload, nil
}

// ShardCount returns number of shards in a node.
func (c *Client) ShardCount(ctx context.Context, host string) (uint, error) {
	const (
		queryMetricName = "database_total_writes"
		metricName      = "scylla_" + queryMetricName
	)

	metrics, err := c.metrics(ctx, host, queryMetricName)
	if err != nil {
		return 0, err
	}

	if _, ok := metrics[metricName]; !ok {
		return 0, errors.Errorf("scylla doest not expose %s metric", metricName)
	}

	shards := len(metrics[metricName].Metric)
	if shards == 0 {
		return 0, errors.New("failed to get shard count")
	}

	return uint(shards), nil
}

// metrics returns Scylla Prometheus metrics, `name` pattern be used to filter
// out only subset of metrics.
func (c *Client) metrics(ctx context.Context, host, name string) (map[string]*prom.MetricFamily, error) {
	u := c.newURL(host, "/metrics")
	r, err := http.NewRequestWithContext(forceHost(ctx, host), http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	if name != "" {
		q := r.URL.Query()
		q.Add("name", name)
		r.URL.RawQuery = q.Encode()
	}

	resp, err := c.transport.RoundTrip(r)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	return prom.ParseText(resp.Body)
}

// DescribeRing returns a description of token range of a given keyspace.
func (c *Client) DescribeRing(ctx context.Context, keyspace string) (Ring, error) {
	resp, err := c.scyllaOps.StorageServiceDescribeRingByKeyspaceGet(&operations.StorageServiceDescribeRingByKeyspaceGetParams{
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
			return Ring{}, errors.Wrap(err, "parse StartToken")
		}
		ring.Tokens[i].EndToken, err = strconv.ParseInt(p.EndToken, 10, 64)
		if err != nil {
			return Ring{}, errors.Wrap(err, "parse EndToken")
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
func (c *Client) Repair(ctx context.Context, host string, config RepairConfig) (int32, error) {
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
	if len(config.Hosts) > 1 {
		hosts := strings.Join(config.Hosts, ",") + "," + host
		p.Hosts = &hosts
	}

	resp, err := c.scyllaOps.StorageServiceRepairAsyncByKeyspacePost(&p)
	if err != nil {
		return 0, err
	}

	return resp.Payload, nil
}

// RepairStatus returns current status of a repair command.
func (c *Client) RepairStatus(ctx context.Context, host, keyspace string, id int32) (CommandStatus, error) {
	resp, err := c.scyllaOps.StorageServiceRepairAsyncByKeyspaceGet(&operations.StorageServiceRepairAsyncByKeyspaceGetParams{
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
		resp, err := c.scyllaOps.StorageServiceActiveRepairGet(&operations.StorageServiceActiveRepairGetParams{
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
	ctx = noRetry(ctx)

	_, err := c.scyllaOps.StorageServiceForceTerminateRepairPost(&operations.StorageServiceForceTerminateRepairPostParams{ // nolint: errcheck
		Context: forceHost(ctx, host),
	})
	return err
}

const snapshotTimeout = 5 * time.Minute

// Snapshots lists available snapshots.
func (c *Client) Snapshots(ctx context.Context, host string) ([]string, error) {
	ctx = customTimeout(ctx, snapshotTimeout)

	resp, err := c.scyllaOps.StorageServiceSnapshotsGet(&operations.StorageServiceSnapshotsGetParams{
		Context: forceHost(ctx, host),
	})
	if err != nil {
		return nil, err
	}

	var tags []string
	for _, p := range resp.Payload {
		tags = append(tags, p.Key)
	}

	return tags, nil
}

// SnapshotDetails returns an index of keyspaces and tables present in the given
// snapshot.
func (c *Client) SnapshotDetails(ctx context.Context, host, tag string) ([]Unit, error) {
	ctx = customTimeout(ctx, snapshotTimeout)

	resp, err := c.scyllaOps.StorageServiceSnapshotsGet(&operations.StorageServiceSnapshotsGetParams{
		Context: forceHost(ctx, host),
	})
	if err != nil {
		return nil, err
	}

	m := make(map[string]Unit)
	for _, p := range resp.Payload {
		if p.Key != tag {
			continue
		}
		for _, v := range p.Value {
			k, ok := m[v.Ks]
			if !ok {
				k = Unit{
					Keyspace: v.Ks,
				}
			}
			k.Tables = append(k.Tables, v.Cf)
			m[v.Ks] = k
		}
	}

	var s []Unit
	for _, v := range m {
		s = append(s, v)
	}
	sort.Slice(s, func(i, j int) bool {
		return s[i].Keyspace < s[j].Keyspace
	})

	return s, nil
}

// TakeSnapshot flushes and takes a snapshot of a keyspace.
// Multiple keyspaces may have the same tag.
func (c *Client) TakeSnapshot(ctx context.Context, host, tag, keyspace string, tables ...string) error {
	ctx = customTimeout(ctx, snapshotTimeout)

	var cfPtr *string

	if len(tables) > 0 {
		v := strings.Join(tables, ",")
		cfPtr = &v
	}

	if _, err := c.scyllaOps.StorageServiceKeyspaceFlushByKeyspacePost(&operations.StorageServiceKeyspaceFlushByKeyspacePostParams{ // nolint: errcheck
		Context:  forceHost(ctx, host),
		Keyspace: keyspace,
		Cf:       cfPtr,
	}); err != nil {
		return err
	}

	if _, err := c.scyllaOps.StorageServiceSnapshotsPost(&operations.StorageServiceSnapshotsPostParams{ // nolint: errcheck
		Context: forceHost(ctx, host),
		Tag:     &tag,
		Kn:      &keyspace,
		Cf:      cfPtr,
	}); err != nil {
		return err
	}

	return nil
}

// DeleteSnapshot removes a snapshot with a given tag.
func (c *Client) DeleteSnapshot(ctx context.Context, host, tag string) error {
	ctx = customTimeout(ctx, snapshotTimeout)

	_, err := c.scyllaOps.StorageServiceSnapshotsDelete(&operations.StorageServiceSnapshotsDeleteParams{ // nolint: errcheck
		Context: forceHost(ctx, host),
		Tag:     &tag,
	})
	return err
}

// TableDiskSize returns total on disk size of the table in bytes.
func (c *Client) TableDiskSize(ctx context.Context, host, keyspace, table string) (int64, error) {
	resp, err := c.scyllaOps.ColumnFamilyMetricsTotalDiskSpaceUsedByNameGet(&operations.ColumnFamilyMetricsTotalDiskSpaceUsedByNameGetParams{
		Context: forceHost(ctx, host),
		Name:    keyspace + ":" + table,
	})
	if err != nil {
		return 0, err
	}
	return resp.Payload, nil
}

// ScyllaFeatures returns features supported by the current Scylla release.
func (c *Client) ScyllaFeatures(ctx context.Context, host string) (ScyllaFeatures, error) {
	resp, err := c.scyllaOps.StorageServiceScyllaReleaseVersionGet(&operations.StorageServiceScyllaReleaseVersionGetParams{
		Context: forceHost(ctx, host),
	})
	if err != nil {
		return ScyllaFeatures{}, err
	}
	return makeScyllaFeatures(resp.Payload)
}

// TotalMemory returns Scylla total memory from particular host.
func (c *Client) TotalMemory(ctx context.Context, host string) (int64, error) {
	const (
		queryMetricName = "memory_total_memory"
		metricName      = "scylla_" + queryMetricName
	)

	metrics, err := c.metrics(ctx, host, queryMetricName)
	if err != nil {
		return 0, err
	}

	if _, ok := metrics[metricName]; !ok {
		return 0, errors.New("scylla doest not expose total memory metric")
	}

	var totalMemory int64 = 0
	for _, m := range metrics[metricName].Metric {
		totalMemory += int64(*m.Counter.Value)
	}

	return totalMemory, nil
}
