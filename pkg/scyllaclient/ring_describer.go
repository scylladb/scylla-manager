// Copyright (C) 2024 ScyllaDB

package scyllaclient

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
)

// RingDescriber describes token rings on table basis for bot vnode and tablet tables.
type RingDescriber interface {
	DescribeRing(ctx context.Context, keyspace, table string) (Ring, error)
}

type ringCache struct {
	Keyspace string
	Table    string
	Ring     Ring
}

func (rc *ringCache) tryGetRing(keyspace, table string) (Ring, bool) {
	if rc.Keyspace == keyspace && rc.Table == table {
		return rc.Ring, true
	}
	return Ring{}, false
}

func (rc *ringCache) setRing(keyspace, table string, ring Ring) {
	rc.Keyspace = keyspace
	rc.Table = table
	rc.Ring = ring
}

type ringDescriber struct {
	client   *Client
	tabletKs *strset.Set
	cache    *ringCache
}

func (rd *ringDescriber) DescribeRing(ctx context.Context, keyspace, table string) (Ring, error) {
	if ring, ok := rd.cache.tryGetRing(keyspace, table); ok {
		return ring, nil
	}

	var (
		ring Ring
		err  error
	)
	if rd.tabletKs.Has(keyspace) {
		ring, err = rd.client.DescribeTabletRing(ctx, keyspace, table)
	} else {
		ring, err = rd.client.DescribeVnodeRing(ctx, keyspace)
	}
	if err != nil {
		return Ring{}, errors.Wrap(err, "describe ring")
	}

	rd.cache.setRing(keyspace, table, ring)
	return ring, nil
}

func NewRingDescriber(ctx context.Context, client *Client) RingDescriber {
	return &ringDescriber{
		client:   client,
		tabletKs: getTabletKs(ctx, client),
		cache:    new(ringCache),
	}
}

// getTabletKs returns set of tablet replicated keyspaces.
func getTabletKs(ctx context.Context, client *Client) *strset.Set {
	out := strset.New()
	// Assume that errors indicate that endpoints rejected 'replication' param,
	// which means that given Scylla version does not support tablet API.
	// Other errors will be handled on other API calls.
	tablets, err := client.ReplicationKeyspaces(ctx, ReplicationTablet)
	if err != nil {
		return out
	}
	vnodes, err := client.ReplicationKeyspaces(ctx, ReplicationVnode)
	if err != nil {
		return out
	}
	// Even when both API calls succeeded, we need to validate
	// that the 'replication' param wasn't silently ignored.
	out.Add(tablets...)
	if out.HasAny(vnodes...) {
		return strset.New()
	}
	return out
}
