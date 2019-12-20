// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
	"path"
	"strings"

	"github.com/scylladb/mermaid/pkg/util/uuid"
)

const dataDir = "data:"

func keyspaceDir(keyspace string) string {
	return dataDir + keyspace
}

const (
	manifest = "manifest.json"
	sep      = string(os.PathSeparator)
)

func remoteMetaClusterDCDir(clusterID uuid.UUID) string {
	return path.Join(
		"backup",
		string(metaDirKind),
		"cluster",
		clusterID.String(),
		"dc",
	)
}

func remoteMetaKeyspaceLevel(baseDir string) int {
	a := len(strings.Split(remoteBaseDir(metaDirKind, uuid.Nil, "a", "b", "c", "d"), sep))
	b := len(strings.Split(baseDir, sep))
	return a - b - 2
}

func remoteManifestFile(clusterID, taskID uuid.UUID, snapshotTag, dc, nodeID, keyspace, table, version string) string {
	return path.Join(
		remoteBaseDir(metaDirKind, clusterID, dc, nodeID, keyspace, table),
		"task",
		taskID.String(),
		"tag",
		snapshotTag,
		version,
		manifest,
	)
}

func remoteTagDir(clusterID, taskID uuid.UUID, snapshotTag, dc, nodeID, keyspace, table string) string {
	return path.Join(
		remoteBaseDir(metaDirKind, clusterID, dc, nodeID, keyspace, table),
		"task",
		taskID.String(),
		"tag",
		snapshotTag,
	)
}

func remoteTagsDir(clusterID, taskID uuid.UUID, dc, nodeID, keyspace, table string) string {
	return path.Join(
		remoteBaseDir(metaDirKind, clusterID, dc, nodeID, keyspace, table),
		"task",
		taskID.String(),
		"tag",
	)
}

func remoteTasksDir(clusterID uuid.UUID, dc, nodeID, keyspace, table string) string {
	return path.Join(
		remoteBaseDir(metaDirKind, clusterID, dc, nodeID, keyspace, table),
		"task",
	)
}

func remoteSSTableVersionDir(clusterID uuid.UUID, dc, nodeID, keyspace, table, version string) string {
	return path.Join(
		remoteBaseDir(sstDirKind, clusterID, dc, nodeID, keyspace, table),
		version,
	)
}

func remoteSSTableDir(clusterID uuid.UUID, dc, nodeID, keyspace, table string) string {
	return remoteBaseDir(sstDirKind, clusterID, dc, nodeID, keyspace, table)
}

type dirKind string

const (
	sstDirKind  = dirKind("sst")
	metaDirKind = dirKind("meta")
)

func remoteBaseDir(kind dirKind, clusterID uuid.UUID, dc, nodeID, keyspace, table string) string {
	return path.Join(
		"backup",
		string(kind),
		"cluster",
		clusterID.String(),
		"dc",
		dc,
		"node",
		nodeID,
		"keyspace",
		keyspace,
		"table",
		table,
	)
}
