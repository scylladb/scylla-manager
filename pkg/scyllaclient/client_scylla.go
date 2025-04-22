// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"bytes"
	"context"
	stdErrors "errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/dht"
	"github.com/scylladb/scylla-manager/v3/pkg/util/maputil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/pointer"
	"github.com/scylladb/scylla-manager/v3/pkg/util/prom"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	slices2 "github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/client/operations"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
	"go.uber.org/multierr"
)

// ErrHostInvalidResponse is to indicate that one of the root-causes is the invalid response from scylla-server.
var ErrHostInvalidResponse = errors.New("invalid response from host")

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

	// Sort by Datacenter and Address
	sort.Slice(all, func(i, j int) bool {
		if all[i].Datacenter != all[j].Datacenter {
			return all[i].Datacenter < all[j].Datacenter
		}
		return all[i].Addr < all[j].Addr
	})

	return all, nil
}

// VerifyNodesAvailability checks if all nodes passed connectivity check and are in the UN state.
func (c *Client) VerifyNodesAvailability(ctx context.Context) error {
	status, err := c.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get status")
	}

	available, err := c.GetLiveNodes(ctx, status)
	if err != nil {
		return errors.Wrap(err, "get live nodes")
	}

	availableUN := available.Live()
	if len(status) == len(availableUN) {
		return nil
	}

	checked := strset.New()
	for _, n := range availableUN {
		checked.Add(n.HostID)
	}

	var unavailable []string
	for _, n := range status {
		if !checked.Has(n.HostID) {
			unavailable = append(unavailable, n.Addr)
		}
	}

	return fmt.Errorf("unavailable nodes: %v", unavailable)
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

// GossiperEndpointLiveGet finds live nodes (according to gossiper).
func (c *Client) GossiperEndpointLiveGet(ctx context.Context) ([]string, error) {
	live, err := c.scyllaOps.GossiperEndpointLiveGet(&operations.GossiperEndpointLiveGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}
	return live.GetPayload(), nil
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

// HostRack looks up the rack that the given host belongs to.
func (c *Client) HostRack(ctx context.Context, host string) (string, error) {
	resp, err := c.scyllaOps.SnitchRackGet(&operations.SnitchRackGetParams{
		Context: ctx,
		Host:    &host,
	})
	if err != nil {
		return "", err
	}
	return resp.Payload, nil
}

// HostIDs returns a mapping from host IP to UUID.
func (c *Client) HostIDs(ctx context.Context) (map[string]string, error) {
	resp, err := c.scyllaOps.StorageServiceHostIDGet(&operations.StorageServiceHostIDGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}

	v := make(map[string]string, len(resp.Payload))
	for i := range len(resp.Payload) {
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
	s := strset.New(c.config.Hosts...)
	return s.Size() != len(cur) || !s.Has(cur...), nil
}

// hosts returns a list of all hosts in a cluster.
func (c *Client) hosts(ctx context.Context) ([]string, error) {
	resp, err := c.scyllaOps.StorageServiceHostIDGet(&operations.StorageServiceHostIDGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}

	v := make([]string, len(resp.Payload))
	for i := range len(resp.Payload) {
		v[i] = resp.Payload[i].Key
	}
	return v, nil
}

// KeyspaceReplication describes keyspace replication.
type KeyspaceReplication string

// KeyspaceReplication enum.
const (
	ReplicationAll    KeyspaceReplication = "all"
	ReplicationVnode  KeyspaceReplication = "vnodes"
	ReplicationTablet KeyspaceReplication = "tablets"
)

// KeyspaceType describes keyspace type.
type KeyspaceType string

// KeyspaceType enum.
const (
	KeyspaceTypeAll      KeyspaceType = "all"
	KeyspaceTypeUser     KeyspaceType = "user"
	KeyspaceTypeNonLocal KeyspaceType = "non_local_strategy"
)

// AllKeyspaceTypes contains all possible KeyspaceType.
var AllKeyspaceTypes = []KeyspaceType{
	KeyspaceTypeAll,
	KeyspaceTypeUser,
	KeyspaceTypeNonLocal,
}

// FilteredKeyspaces return a list of keyspaces with given replication.
func (c *Client) FilteredKeyspaces(ctx context.Context, ksType KeyspaceType, replication KeyspaceReplication) ([]string, error) {
	resp, err := c.scyllaOps.StorageServiceKeyspacesGet(&operations.StorageServiceKeyspacesGetParams{
		Context:     ctx,
		Type:        pointer.StringPtr(string(ksType)),
		Replication: pointer.StringPtr(string(replication)),
	})
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// Keyspaces return a list of all the keyspaces.
func (c *Client) Keyspaces(ctx context.Context) ([]string, error) {
	return c.FilteredKeyspaces(ctx, KeyspaceTypeAll, ReplicationAll)
}

// KeyspacesByType returns a list of keyspaces aggregated by KeyspaceType.
func (c *Client) KeyspacesByType(ctx context.Context) (map[KeyspaceType][]string, error) {
	out := make(map[KeyspaceType][]string, len(AllKeyspaceTypes))
	for _, t := range AllKeyspaceTypes {
		ks, err := c.FilteredKeyspaces(ctx, t, ReplicationAll)
		if err != nil {
			return nil, errors.Wrapf(err, "query keyspaces with type %q", t)
		}
		out[t] = ks
	}
	return out, nil
}

// AllTables returns all tables grouped by keyspace.
func (c *Client) AllTables(ctx context.Context) (map[string][]string, error) {
	resp, err := c.scyllaOps.ColumnFamilyNameGet(&operations.ColumnFamilyNameGetParams{Context: ctx})
	if err != nil {
		return nil, err
	}
	out := make(map[string][]string)
	for _, kst := range resp.Payload {
		parts := strings.Split(kst, ":")
		if len(parts) != 2 {
			return nil, errors.Errorf("GET /column_family/name: expected exactly 1 colon in '<keyspace>:<table>', got %d", len(parts)-1)
		}
		ks := parts[0]
		t := parts[1]
		out[ks] = append(out[ks], t)
	}
	return out, nil
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

// Tokens returns list of tokens for a node.
func (c *Client) Tokens(ctx context.Context, host string) ([]int64, error) {
	resp, err := c.scyllaOps.StorageServiceTokensByEndpointGet(&operations.StorageServiceTokensByEndpointGetParams{
		Endpoint: host,
		Context:  ctx,
	})
	if err != nil {
		return nil, err
	}

	tokens := make([]int64, len(resp.Payload))
	for i, s := range resp.Payload {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return tokens, errors.Wrapf(err, "parsing error at pos %d", i)
		}
		tokens[i] = v
	}
	return tokens, nil
}

// ShardCount returns number of shards in a node.
// If host is empty it will pick one from the pool.
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
		return 0, errors.Errorf("scylla does not expose %s metric", metricName)
	}

	shards := len(metrics[metricName].GetMetric())
	if shards == 0 {
		return 0, errors.New("missing shard count")
	}

	return uint(shards), nil
}

// HostsShardCount runs ShardCount for many hosts.
func (c *Client) HostsShardCount(ctx context.Context, hosts []string) (map[string]uint, error) {
	shards := make([]uint, len(hosts))

	f := func(i int) error {
		sh, err := c.ShardCount(ctx, hosts[i])
		if err != nil {
			return parallel.Abort(errors.Wrapf(err, "%s: get shard count", hosts[i]))
		}
		shards[i] = sh
		return nil
	}
	if err := parallel.Run(len(hosts), parallel.NoLimit, f, parallel.NopNotify); err != nil {
		return nil, err
	}

	out := make(map[string]uint)
	for i, h := range hosts {
		if shards[i] == 0 {
			return nil, errors.Errorf("host %s reported 0 shard count", h)
		}
		out[h] = shards[i]
	}
	return out, nil
}

// metrics returns Scylla Prometheus metrics, `name` pattern be used to filter
// out only subset of metrics.
// If host is empty it will pick one from the pool.
func (c *Client) metrics(ctx context.Context, host, name string) (map[string]*prom.MetricFamily, error) {
	u := c.newURL(host, "/metrics")

	// In case host is not set select a host from a pool.
	if host != "" {
		ctx = forceHost(ctx, host)
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, err
	}

	if name != "" {
		q := r.URL.Query()
		q.Add("name", name)
		r.URL.RawQuery = q.Encode()
	}

	resp, err := c.client.Do("Metrics", r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return prom.ParseText(resp.Body)
}

// DescribeTabletRing returns a description of token range of a given tablet table.
func (c *Client) DescribeTabletRing(ctx context.Context, keyspace, table string) (Ring, error) {
	return c.describeRing(&operations.StorageServiceDescribeRingByKeyspaceGetParams{
		Context:  ctx,
		Keyspace: keyspace,
		Table:    &table,
	})
}

// DescribeVnodeRing returns a description of token range of a given vnode keyspace.
func (c *Client) DescribeVnodeRing(ctx context.Context, keyspace string) (Ring, error) {
	return c.describeRing(&operations.StorageServiceDescribeRingByKeyspaceGetParams{
		Context:  ctx,
		Keyspace: keyspace,
	})
}

func (c *Client) describeRing(params *operations.StorageServiceDescribeRingByKeyspaceGetParams) (Ring, error) {
	resp, err := c.scyllaOps.StorageServiceDescribeRingByKeyspaceGet(params)
	if err != nil {
		return Ring{}, err
	}
	if len(resp.Payload) == 0 {
		return Ring{}, errors.New("received empty token range list")
	}

	ring := Ring{
		ReplicaTokens: make([]ReplicaTokenRanges, 0),
		HostDC:        map[netip.Addr]string{},
	}
	dcTokens := make(map[string]int)

	replicaTokens := make(map[uint64][]TokenRange)
	replicaHash := make(map[uint64][]netip.Addr)

	isNetworkTopologyStrategy := true
	rf := len(resp.Payload[0].Endpoints)
	var dcRF map[string]int
	for _, p := range resp.Payload {
		// Parse tokens
		startToken, err := strconv.ParseInt(p.StartToken, 10, 64)
		if err != nil {
			return Ring{}, errors.Wrap(err, "parse StartToken")
		}
		endToken, err := strconv.ParseInt(p.EndToken, 10, 64)
		if err != nil {
			return Ring{}, errors.Wrap(err, "parse EndToken")
		}

		replicaSet, err := slices2.MapWithError(p.Endpoints, netip.ParseAddr)
		if err != nil {
			return Ring{}, err
		}
		// Ensure deterministic order of nodes in replica set
		slices.SortFunc(replicaSet, func(a, b netip.Addr) int {
			return a.Compare(b)
		})

		// Aggregate replica set token ranges
		hash := ReplicaHash(replicaSet)
		replicaHash[hash] = replicaSet
		replicaTokens[hash] = append(replicaTokens[hash], TokenRange{
			StartToken: startToken,
			EndToken:   endToken,
		})

		// Update replication factors
		if rf != len(p.Endpoints) {
			return Ring{}, errors.Errorf("ifferent token ranges have different rf (%d/%d). Repair is not safe for now", rf, len(p.Endpoints))
		}
		tokenDCrf := make(map[string]int)
		for _, e := range p.EndpointDetails {
			tokenDCrf[e.Datacenter]++
		}
		// NetworkTopologyStrategy -> all token ranges have the same dc to rf mapping
		if dcRF == nil || maputil.Equal(dcRF, tokenDCrf) {
			dcRF = tokenDCrf
		} else {
			isNetworkTopologyStrategy = false
		}

		// Update host to DC mapping
		for _, e := range p.EndpointDetails {
			ip, err := netip.ParseAddr(e.Host)
			if err != nil {
				return Ring{}, err
			}
			ring.HostDC[ip] = e.Datacenter
		}

		// Update DC token metrics
		dcs := strset.New()
		for _, e := range p.EndpointDetails {
			if !dcs.Has(e.Datacenter) {
				dcTokens[e.Datacenter]++
				dcs.Add(e.Datacenter)
			}
		}
	}

	for hash, tokens := range replicaTokens {
		// Ensure deterministic order of tokens
		sort.Slice(tokens, func(i, j int) bool {
			return tokens[i].StartToken < tokens[j].StartToken
		})

		ring.ReplicaTokens = append(ring.ReplicaTokens, ReplicaTokenRanges{
			ReplicaSet: replicaHash[hash],
			Ranges:     tokens,
		})
	}

	// Detect replication strategy
	ring.RF = rf
	switch {
	case len(ring.HostDC) == 1:
		ring.Replication = LocalStrategy
	case isNetworkTopologyStrategy:
		ring.Replication = NetworkTopologyStrategy
		ring.DCrf = dcRF
	default:
		ring.Replication = SimpleStrategy
	}

	return ring, nil
}

// ReplicaHash hashes replicas so that it can be used as a map key.
func ReplicaHash(replicaSet []netip.Addr) uint64 {
	hash := xxhash.New()
	for _, r := range replicaSet {
		_, _ = hash.WriteString(r.String()) // nolint: errcheck
		_, _ = hash.WriteString(",")        // nolint: errcheck
	}
	return hash.Sum64()
}

// TabletRepair schedules Scylla repair tablet table task and returns its ID.
// All tablets will be repaired with just a single task. It repairs all hosts
// by default, but it's possible to filter them by DC or host ID. The master is
// only needed so that we know which node should be queried for the task status.
func (c *Client) TabletRepair(ctx context.Context, keyspace, table, master string, dcs, hostIDs []string) (string, error) {
	const allTablets = "all"
	dontAwaitCompletion := "false"
	p := operations.StorageServiceTabletsRepairPostParams{
		Context:         forceHost(ctx, master),
		Ks:              keyspace,
		Table:           table,
		Tokens:          allTablets,
		AwaitCompletion: &dontAwaitCompletion,
	}
	if len(dcs) > 0 {
		merged := strings.Join(dcs, ",")
		p.SetDcsFilter(&merged)
	}
	if len(hostIDs) > 0 {
		merged := strings.Join(hostIDs, ",")
		p.SetHostsFilter(&merged)
	}
	resp, err := c.scyllaOps.StorageServiceTabletsRepairPost(&p)
	if err != nil {
		return "", err
	}
	return resp.GetPayload().TabletTaskID, nil
}

// Repair invokes async repair and returns the repair command ID.
func (c *Client) Repair(ctx context.Context, keyspace, table, master string, replicaSet []string, ranges []TokenRange, intensity int, smallTableOpt bool) (int32, error) {
	dr := dumpRanges(ranges)
	p := operations.StorageServiceRepairAsyncByKeyspacePostParams{
		Context:        forceHost(ctx, master),
		Keyspace:       keyspace,
		ColumnFamilies: &table,
		Ranges:         &dr,
	}
	if smallTableOpt {
		p.SmallTableOptimization = pointer.StringPtr("true")
	} else {
		p.RangesParallelism = pointer.StringPtr(strconv.Itoa(intensity))
	}
	// Single node cluster repair fails with hosts param
	if len(replicaSet) > 1 {
		hosts := strings.Join(replicaSet, ",")
		p.Hosts = &hosts
	}

	resp, err := c.scyllaOps.StorageServiceRepairAsyncByKeyspacePost(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload, nil
}

func dumpRanges(ranges []TokenRange) string {
	var buf bytes.Buffer
	for i, ttr := range ranges {
		if i > 0 {
			_ = buf.WriteByte(',')
		}
		if ttr.StartToken > ttr.EndToken {
			_, _ = fmt.Fprintf(&buf, "%d:%d,%d:%d", ttr.StartToken, dht.Murmur3MaxToken, dht.Murmur3MinToken, ttr.EndToken)
		} else {
			_, _ = fmt.Fprintf(&buf, "%d:%d", ttr.StartToken, ttr.EndToken)
		}
	}
	return buf.String()
}

func repairStatusShouldRetryHandler(err error) *bool {
	s, m := StatusCodeAndMessageOf(err)
	if s == http.StatusInternalServerError && strings.Contains(m, "unknown repair id") {
		return pointer.BoolPtr(false)
	}
	return nil
}

const repairStatusTimeout = 30 * time.Minute

// RepairStatus waits for repair job to finish and returns its status.
func (c *Client) RepairStatus(ctx context.Context, host string, id int32) (CommandStatus, error) {
	ctx = forceHost(ctx, host)
	ctx = customTimeout(ctx, repairStatusTimeout)
	ctx = withShouldRetryHandler(ctx, repairStatusShouldRetryHandler)
	var (
		resp interface {
			GetPayload() models.RepairAsyncStatusResponse
		}
		err error
	)

	resp, err = c.scyllaOps.StorageServiceRepairStatus(&operations.StorageServiceRepairStatusParams{
		Context: ctx,
		ID:      id,
	})
	if err != nil {
		return "", err
	}
	return CommandStatus(resp.GetPayload()), nil
}

// When using long polling, wait duration starts only when node receives the
// request.
// longPollingTimeout is calculating timeout duration needed for request to
// reach node so context is not canceled before response is received.
func (c *Client) longPollingTimeout(waitSeconds int) time.Duration {
	return time.Second*time.Duration(waitSeconds) + c.config.Timeout
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
	for range 10 {
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
func (c *Client) KillAllRepairs(ctx context.Context, hosts ...string) error {
	ctx = noRetry(ctx)

	f := func(i int) error {
		host := hosts[i]
		_, err := c.scyllaOps.StorageServiceForceTerminateRepairPost(&operations.StorageServiceForceTerminateRepairPostParams{
			Context: forceHost(ctx, host),
		})
		return err
	}

	notify := func(i int, err error) {
		host := hosts[i]
		c.logger.Error(ctx, "Failed to terminate repair",
			"host", host,
			"error", err,
		)
	}

	return parallel.Run(len(hosts), parallel.NoLimit, f, notify)
}

const snapshotTimeout = 30 * time.Minute

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
// Flush is taken care of by Scylla, see table::snapshot for details.
// If snapshot already exists no error is returned.
func (c *Client) TakeSnapshot(ctx context.Context, host, tag, keyspace string, tables ...string) error {
	ctx = customTimeout(ctx, snapshotTimeout)
	ctx = withShouldRetryHandler(ctx, takeSnapshotShouldRetryHandler)

	var cf *string
	if len(tables) > 0 {
		cf = pointer.StringPtr(strings.Join(tables, ","))
	}

	p := operations.StorageServiceSnapshotsPostParams{
		Context: forceHost(ctx, host),
		Tag:     &tag,
		Kn:      &keyspace,
		Cf:      cf,
	}
	_, err := c.scyllaOps.StorageServiceSnapshotsPost(&p)

	// Ignore SnapshotAlreadyExists error
	if err != nil && isSnapshotAlreadyExists(err) {
		err = nil
	}

	return err
}

var snapshotAlreadyExistsRegex = regexp.MustCompile(`snapshot \w+ already exists`)

func isSnapshotAlreadyExists(err error) bool {
	_, msg := StatusCodeAndMessageOf(err)
	return snapshotAlreadyExistsRegex.MatchString(msg)
}

func takeSnapshotShouldRetryHandler(err error) *bool {
	if isSnapshotAlreadyExists(err) {
		return pointer.BoolPtr(false)
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

// DeleteTableSnapshot removes a snapshot with a given tag.
// Removed data is restricted to the provided keyspace and table.
func (c *Client) DeleteTableSnapshot(ctx context.Context, host, tag, keyspace, table string) error {
	ctx = customTimeout(ctx, snapshotTimeout)

	_, err := c.scyllaOps.StorageServiceSnapshotsDelete(&operations.StorageServiceSnapshotsDeleteParams{ // nolint: errcheck
		Context: forceHost(ctx, host),
		Tag:     &tag,
		Kn:      pointer.StringPtr(keyspace),
		Cf:      pointer.StringPtr(table),
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

// TableExists returns true iff table exists.
func (c *Client) TableExists(ctx context.Context, host, keyspace, table string) (bool, error) {
	if host != "" {
		ctx = forceHost(ctx, host)
	}
	resp, err := c.scyllaOps.ColumnFamilyNameGet(&operations.ColumnFamilyNameGetParams{Context: ctx})
	if err != nil {
		return false, err
	}
	return slice.ContainsString(resp.Payload, keyspace+":"+table), nil
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
		return 0, errors.New("scylla does not expose total memory metric")
	}

	var totalMemory int64
	for _, m := range metrics[metricName].GetMetric() {
		switch {
		case m.GetCounter() != nil:
			totalMemory += int64(m.GetCounter().GetValue())
		case m.GetGauge() != nil:
			totalMemory += int64(m.GetGauge().GetValue())
		}
	}

	return totalMemory, nil
}

// HostsTotalMemory runs TotalMemory for many hosts.
func (c *Client) HostsTotalMemory(ctx context.Context, hosts []string) (map[string]int64, error) {
	memory := make([]int64, len(hosts))

	f := func(i int) error {
		mem, err := c.TotalMemory(ctx, hosts[i])
		if err != nil {
			return parallel.Abort(errors.Wrapf(err, "%s: get total memory", hosts[i]))
		}
		memory[i] = mem
		return nil
	}
	if err := parallel.Run(len(hosts), parallel.NoLimit, f, parallel.NopNotify); err != nil {
		return nil, err
	}

	out := make(map[string]int64)
	for i, h := range hosts {
		out[h] = memory[i]
	}
	return out, nil
}

// HostKeyspaceTable is a tuple of Host and Keyspace and Table names.
type HostKeyspaceTable struct {
	Host     string
	Keyspace string
	Table    string
}

// HostKeyspaceTables is a slice of HostKeyspaceTable.
type HostKeyspaceTables []HostKeyspaceTable

// Hosts returns slice of unique hosts.
func (t HostKeyspaceTables) Hosts() []string {
	s := strset.New()
	for _, v := range t {
		s.Add(v.Host)
	}
	return s.List()
}

// SizeReport extends HostKeyspaceTable with Size information.
type SizeReport struct {
	HostKeyspaceTable
	Size int64
}

// TableDiskSizeReport returns total on disk size of tables in bytes.
func (c *Client) TableDiskSizeReport(ctx context.Context, hostKeyspaceTables HostKeyspaceTables) ([]SizeReport, error) {
	// Get shard count of a first node to estimate parallelism limit
	shards, err := c.ShardCount(ctx, "")
	if err != nil {
		return nil, errors.Wrapf(err, "shard count")
	}

	var (
		limit  = len(hostKeyspaceTables.Hosts()) * int(shards)
		report = make([]SizeReport, len(hostKeyspaceTables))
	)

	f := func(i int) error {
		v := hostKeyspaceTables[i]

		size, err := c.TableDiskSize(ctx, v.Host, v.Keyspace, v.Table)
		if err != nil {
			return parallel.Abort(fmt.Errorf("%s: %w", v.Host, stdErrors.Join(err, ErrHostInvalidResponse)))
		}
		c.logger.Debug(ctx, "Table disk size",
			"host", v.Host,
			"keyspace", v.Keyspace,
			"table", v.Table,
			"size", size,
		)

		report[i] = SizeReport{
			HostKeyspaceTable: v,
			Size:              size,
		}
		return nil
	}

	notify := func(i int, err error) {
		v := hostKeyspaceTables[i]
		c.logger.Error(ctx, "Failed to get table disk size",
			"host", v.Host,
			"keyspace", v.Keyspace,
			"table", v.Table,
			"error", err,
		)
	}

	err = parallel.Run(len(hostKeyspaceTables), limit, f, notify)
	return report, err
}

// AwaitLoadSSTables loads sstables that are already downloaded to host's table upload directory.
func (c *Client) AwaitLoadSSTables(ctx context.Context, host, keyspace, table string, loadAndStream, primaryReplicaOnly bool) error {
	c.logger.Info(ctx, "First try on loading sstables",
		"host", host,
		"keyspace", keyspace,
		"table", table,
		"load&stream", loadAndStream,
		"primary replica only", primaryReplicaOnly,
	)

	isAlreadyLoadingSSTables := func(err error) bool {
		const alreadyLoadingSSTablesErrMsg = "Already loading SSTables"
		return err != nil && strings.Contains(err.Error(), alreadyLoadingSSTablesErrMsg)
	}

	// The first call is synchronous and might time out.
	// We also need to handle situation where SM task
	// was interrupted and retried immediately.
	// Then it might also happen, that we get the
	// already loading error, and we should await its completion.
	const firstCallTimeout = time.Hour
	firstCallCtx := ctx
	firstCallCtx = customTimeout(firstCallCtx, firstCallTimeout)
	firstCallCtx = noRetry(firstCallCtx)
	err := c.loadSSTables(firstCallCtx, host, keyspace, table, loadAndStream, primaryReplicaOnly)
	if err == nil {
		return nil // Return on success
	}
	if !isAlreadyLoadingSSTables(err) && !errors.Is(err, context.DeadlineExceeded) {
		return err // Return on not already loading nor timeout related error
	}

	dontRetryOnAlreadyLoadingSSTablesRetryHandler := func(err error) *bool {
		if isAlreadyLoadingSSTables(err) {
			return pointer.BoolPtr(false)
		}
		return nil
	}

	// Retry calls are not blocking as they return an error
	// if the sstables are still being loaded.
	const retryInterval = 10 * time.Second
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}

		c.logger.Info(ctx, "Retry on loading sstables",
			"host", host,
			"keyspace", keyspace,
			"table", table,
			"load&stream", loadAndStream,
			"primary replica only", primaryReplicaOnly,
		)

		retryCallCtx := ctx
		retryCallCtx = withShouldRetryHandler(retryCallCtx, dontRetryOnAlreadyLoadingSSTablesRetryHandler)
		err = c.loadSSTables(retryCallCtx, host, keyspace, table, loadAndStream, primaryReplicaOnly)
		if err == nil {
			return nil // Return on success
		}
		if !isAlreadyLoadingSSTables(err) {
			return err // Return on not already loading sstables error
		}
	}
}

// loadSSTables that are already downloaded to host's table upload directory.
// Used API endpoint has the following properties:
// - It is synchronous - response is received only after the loading has finished
// - It immediately returns an error if called while loading is still happening
// - It returns nil when called on an empty upload dir
// loadSSTables does not perform any special error, timeout or retry handling.
// See AwaitLoadSSTables for a wrapper with those features.
func (c *Client) loadSSTables(ctx context.Context, host, keyspace, table string, loadAndStream, primaryReplicaOnly bool) error {
	_, err := c.scyllaOps.StorageServiceSstablesByKeyspacePost(&operations.StorageServiceSstablesByKeyspacePostParams{
		Context:            forceHost(ctx, host),
		Keyspace:           keyspace,
		Cf:                 table,
		LoadAndStream:      &loadAndStream,
		PrimaryReplicaOnly: &primaryReplicaOnly,
	})
	return err
}

// IsAutoCompactionEnabled checks if auto compaction of given table is enabled on the host.
func (c *Client) IsAutoCompactionEnabled(ctx context.Context, host, keyspace, table string) (bool, error) {
	resp, err := c.scyllaOps.ColumnFamilyAutocompactionByNameGet(&operations.ColumnFamilyAutocompactionByNameGetParams{
		Context: forceHost(ctx, host),
		Name:    keyspace + ":" + table,
	})
	if err != nil {
		return false, err
	}
	return resp.Payload, nil
}

// EnableAutoCompaction enables auto compaction on the host.
func (c *Client) EnableAutoCompaction(ctx context.Context, host, keyspace, table string) error {
	_, err := c.scyllaOps.ColumnFamilyAutocompactionByNamePost(&operations.ColumnFamilyAutocompactionByNamePostParams{
		Context: forceHost(ctx, host),
		Name:    keyspace + ":" + table,
	})
	return err
}

// DisableAutoCompaction disables auto compaction on the host.
func (c *Client) DisableAutoCompaction(ctx context.Context, host, keyspace, table string) error {
	_, err := c.scyllaOps.ColumnFamilyAutocompactionByNameDelete(&operations.ColumnFamilyAutocompactionByNameDeleteParams{
		Context: forceHost(ctx, host),
		Name:    keyspace + ":" + table,
	})
	return err
}

// FlushTable flushes writes stored in MemTable into SSTables stored on disk.
func (c *Client) FlushTable(ctx context.Context, host, keyspace, table string) error {
	_, err := c.scyllaOps.StorageServiceKeyspaceFlushByKeyspacePost(&operations.StorageServiceKeyspaceFlushByKeyspacePostParams{
		Cf:       &table,
		Keyspace: keyspace,
		Context:  forceHost(ctx, host),
	})
	return err
}

// ViewBuildStatus returns the earliest (among all nodes) build status for given view.
func (c *Client) ViewBuildStatus(ctx context.Context, keyspace, view string) (ViewBuildStatus, error) {
	resp, err := c.scyllaOps.StorageServiceViewBuildStatusesByKeyspaceAndViewGet(&operations.StorageServiceViewBuildStatusesByKeyspaceAndViewGetParams{
		Context:  ctx,
		Keyspace: keyspace,
		View:     view,
	})
	if err != nil {
		return "", err
	}

	if len(resp.Payload) == 0 {
		return StatusUnknown, nil
	}

	minStatus := StatusSuccess
	for _, v := range resp.Payload {
		status := ViewBuildStatus(v.Value)
		if status.Index() < minStatus.Index() {
			minStatus = status
		}
	}
	return minStatus, nil
}

// ControlTabletLoadBalancing disables or enables tablet load balancing in cluster.
func (c *Client) ControlTabletLoadBalancing(ctx context.Context, enabled bool) error {
	// Disabling tablet load balancing might take a lot of time,
	// because it waits for all currently running migrations to finish.
	// Because of that, we need to increase the timeout.
	// In case of timeout, retries don't make sense, because it would
	// be better to increase the timeout instead, so that we ensure
	// that no new tablet migrations are started during retry interval.
	// On the other hand, we should still retry in other scenarios.
	const tabletsBalancingTimeout = 30 * time.Minute
	ctx = customTimeout(ctx, tabletsBalancingTimeout)
	ctx = withShouldRetryHandler(ctx, func(err error) *bool {
		// It's fine not to retry even is the context error
		// is propagated from parent - it wouldn't work anyway.
		if errors.Is(err, context.DeadlineExceeded) {
			return pointer.BoolPtr(false)
		}
		return nil
	})
	_, err := c.scyllaOps.StorageServiceTabletsBalancingPost(&operations.StorageServiceTabletsBalancingPostParams{
		Context: ctx,
		Enabled: enabled,
	})
	return err
}

// raftReadBarrierTimeout reflects a default timeout defined on the scylla side
// so we can align scyllclient timeout with it.
const raftReadBarrierTimeout = 60 * time.Second

// RaftReadBarrier triggers read barrier for the given Raft group to wait for previously committed commands in this group to be applied locally.
// For example, can be used on group 0 (default, when group is not provided) to wait for the node to obtain latest schema changes.
func (c *Client) RaftReadBarrier(ctx context.Context, host, groupID string) error {
	ctx = forceHost(ctx, host)
	ctx = customTimeout(ctx, raftReadBarrierTimeout)

	params := operations.RaftReadBarrierPostParams{
		Context: ctx,
	}

	if groupID != "" {
		params.SetGroupID(&groupID)
	}

	_, err := c.scyllaOps.RaftReadBarrierPost(&params)
	if err != nil {
		return fmt.Errorf("RaftReadBarrierPost: %w", err)
	}
	return nil
}

// ScyllaTaskState describes Scylla task state.
type ScyllaTaskState string

// Possible ScyllaTaskState.
const (
	ScyllaTaskStateCreated ScyllaTaskState = "created"
	ScyllaTaskStateRunning ScyllaTaskState = "running"
	ScyllaTaskStateDone    ScyllaTaskState = "done"
	ScyllaTaskStateFailed  ScyllaTaskState = "failed"
)

func isScyllaTaskRunning(err error) bool {
	// Scylla API call might return earlier due to timeout (see swagger definition)
	status, _ := StatusCodeAndMessageOf(err)
	return status == http.StatusRequestTimeout
}

func scyllaWaitTaskShouldRetryHandler(err error) *bool {
	if isScyllaTaskRunning(err) {
		return pointer.BoolPtr(false)
	}
	return nil
}

// ScyllaWaitTask long polls Scylla task status.
func (c *Client) ScyllaWaitTask(ctx context.Context, host, id string, longPollingSeconds int64) (*models.TaskStatus, error) {
	ctx = withShouldRetryHandler(ctx, scyllaWaitTaskShouldRetryHandler)
	ctx = forceHost(ctx, host)
	ctx = noTimeout(ctx)
	p := &operations.TaskManagerWaitTaskTaskIDGetParams{
		Context: ctx,
		TaskID:  id,
	}
	if longPollingSeconds > 0 {
		p.SetTimeout(&longPollingSeconds)
	}

	resp, err := c.scyllaOps.TaskManagerWaitTaskTaskIDGet(p)
	if err != nil {
		if isScyllaTaskRunning(err) {
			return c.ScyllaTaskProgress(ctx, host, id)
		}
		return nil, err
	}
	return resp.GetPayload(), nil
}

// ScyllaTaskProgress returns provided Scylla task status.
func (c *Client) ScyllaTaskProgress(ctx context.Context, host, id string) (*models.TaskStatus, error) {
	resp, err := c.scyllaOps.TaskManagerTaskStatusTaskIDGet(&operations.TaskManagerTaskStatusTaskIDGetParams{
		Context: forceHost(ctx, host),
		TaskID:  id,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetPayload(), nil
}

// ScyllaAbortTask aborts provided Scylla task.
// Note that not all Scylla tasks can be aborted - see models.TaskStatus to check that.
func (c *Client) ScyllaAbortTask(ctx context.Context, host, id string) error {
	_, err := c.scyllaOps.TaskManagerAbortTaskTaskIDPost(&operations.TaskManagerAbortTaskTaskIDPostParams{
		Context: forceHost(ctx, host),
		TaskID:  id,
	})
	return err
}

// ToCanonicalIP replaces ":0:0" in IPv6 addresses with "::"
// ToCanonicalIP("192.168.0.1") -> "192.168.0.1"
// ToCanonicalIP("100:200:0:0:0:0:0:1") -> "100:200::1".
func ToCanonicalIP(host string) string {
	val := net.ParseIP(host)
	if val == nil {
		return host
	}
	return val.String()
}
