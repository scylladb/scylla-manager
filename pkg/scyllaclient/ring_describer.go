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

type ringDescriber struct {
	client   *Client
	tabletKs *strset.Set
	cache    ringCache
}

type ringCache struct {
	Keyspace string
	Table    string
	Ring     Ring
}

func NewRingDescriber(ctx context.Context, client *Client) RingDescriber {
	return &ringDescriber{
		client:   client,
		tabletKs: getTabletKs(ctx, client),
	}
}

func (rd *ringDescriber) DescribeRing(ctx context.Context, keyspace, table string) (Ring, error) {
	if ring, ok := rd.tryGetRing(keyspace, table); ok {
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

	rd.setRing(keyspace, table, ring)
	return ring, nil
}

func (rd *ringDescriber) tryGetRing(keyspace, table string) (Ring, bool) {
	if rd.cache.Keyspace == keyspace {
		if rd.cache.Table == table || !rd.tabletKs.Has(keyspace) {
			return rd.cache.Ring, true
		}
	}
	return Ring{}, false
}

func (rd *ringDescriber) setRing(keyspace, table string, ring Ring) {
	rd.cache.Keyspace = keyspace
	rd.cache.Table = table
	rd.cache.Ring = ring
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
