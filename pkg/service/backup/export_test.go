// Copyright (C) 2017 ScyllaDB

package backup

import (
	"testing"

	"github.com/scylladb/mermaid/pkg/util/uuid"
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
type LegacyManifest = manifestV1

func RemoteManifestDir(clusterID uuid.UUID, dc, nodeID string) string {
	return remoteManifestDir(clusterID, dc, nodeID)
}

const (
	ScyllaManifest  = scyllaManifest
	MetadataVersion = metadataVersion
)
