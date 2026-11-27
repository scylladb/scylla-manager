// Copyright (C) 2026 ScyllaDB

package scyllaclient

import (
	"context"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/client/operations"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// SSTableInfo returns the keyspace's SSTables on the host, mapped by table name.
func (c *Client) SSTableInfo(ctx context.Context, host, keyspace string) (map[string][]*models.Sstable, error) {
	resp, err := c.scyllaOps.StorageServiceSstableInfoGet(&operations.StorageServiceSstableInfoGetParams{
		Context:  forceHost(ctx, host),
		Keyspace: &keyspace,
	})
	if err != nil {
		return nil, err
	}

	ret := make(map[string][]*models.Sstable)
	for _, t := range resp.Payload {
		ret[t.Table] = append(ret[t.Table], t.Sstables...)
	}
	return ret, nil
}

// SSTableExtendedProperties returns the extended properties map of a given group, if present.
func SSTableExtendedProperties(s *models.Sstable, group string) map[string]string {
	for _, g := range s.ExtendedProperties {
		if g == nil || g.Group != group {
			continue
		}
		ret := make(map[string]string, len(g.Attributes))
		for _, a := range g.Attributes {
			if a != nil {
				ret[a.Key] = a.Value
			}
		}
		return ret
	}
	return nil
}
