// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func SnapshotTagFromManifestPath(t *testing.T, s string) string {
	var m ManifestInfo
	if err := m.ParsePath(s); err != nil {
		t.Fatal(t)
	}
	return m.SnapshotTag
}

type (
	FileInfo = fileInfo
)

func (p *RunProgress) Files() []FileInfo {
	return p.files
}

func (s *Service) InitTarget(ctx context.Context, clusterID uuid.UUID, target *Target) error {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrapf(err, "get client")
	}

	// Get live nodes
	target.liveNodes, err = s.getLiveNodes(ctx, client, target.DC)
	return err
}

func (t Target) Hosts() []string {
	return t.liveNodes.Hosts()
}
