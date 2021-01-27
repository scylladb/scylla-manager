// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func NewSnapshotTag() string {
	return newSnapshotTag()
}

func SnapshotTagFromManifestPath(t *testing.T, s string) string {
	var m remoteManifest
	if err := m.ParsePartialPath(s); err != nil {
		t.Fatal(t)
	}
	return m.SnapshotTag
}

func ParsePartialPath(s string) error {
	var m remoteManifest
	return m.ParsePartialPath(s)
}

type RemoteManifest = remoteManifest
type ManifestContent = manifestContent
type FileInfo = fileInfo
type ModelFilesInfo = filesInfo

func RemoteManifestDir(clusterID uuid.UUID, dc, nodeID string) string {
	return remoteManifestDir(clusterID, dc, nodeID)
}

const (
	ScyllaManifest  = scyllaManifest
	MetadataVersion = metadataVersion
	TempFileExt = tempFileExt
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
