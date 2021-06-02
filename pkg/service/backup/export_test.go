// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func SnapshotTagFromManifestPath(t *testing.T, s string) string {
	var m RemoteManifest
	if err := m.ParsePath(s); err != nil {
		t.Fatal(t)
	}
	return m.SnapshotTag
}

type (
	FileInfo = fileInfo
)

const (
	ScyllaManifest = scyllaManifest
)

func (p *RunProgress) Files() []FileInfo {
	return p.files
}

func (s *Service) InitTarget(ctx context.Context, clusterID uuid.UUID, target *Target) error {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrapf(err, "get client")
	}

	// Collect ring information
	rings := make(map[string]scyllaclient.Ring, len(target.Units))
	for _, u := range target.Units {
		ring, err := client.DescribeRing(ctx, u.Keyspace)
		if err != nil {
			return errors.Wrap(err, "initialize: describe keyspace ring")
		}
		rings[u.Keyspace] = ring
	}

	// Get live nodes
	target.liveNodes, err = s.getLiveNodes(ctx, client, *target, rings)
	return err
}

func (t Target) Hosts() []string {
	return t.liveNodes.Hosts()
}
