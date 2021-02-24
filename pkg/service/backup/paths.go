// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
	"path"
	"strings"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

const dataDir = "data:"

func keyspaceDir(keyspace string) string {
	return dataDir + keyspace
}

const (
	scyllaManifest  = "manifest.json"
	scyllaSchema    = "schema.cql"
	manifest        = "manifest.json.gz"
	schema          = "schema.tar.gz"
	metadataVersion = ".version"
	sep             = string(os.PathSeparator)
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

func remoteManifestLevel(baseDir string) int {
	a := len(strings.Split(remoteManifestDir(uuid.Nil, "a", "b"), sep))
	b := len(strings.Split(baseDir, sep))
	return a - b
}

func remoteManifestFile(clusterID, taskID uuid.UUID, snapshotTag, dc, nodeID string) string {
	manifestName := strings.Join([]string{
		"task",
		taskID.String(),
		"tag",
		snapshotTag,
		manifest,
	}, "_")

	return path.Join(
		remoteManifestDir(clusterID, dc, nodeID),
		manifestName,
	)
}

func remoteSchemaFile(clusterID, taskID uuid.UUID, snapshotTag string) string {
	manifestName := strings.Join([]string{
		"task",
		taskID.String(),
		"tag",
		snapshotTag,
		schema,
	}, "_")

	return path.Join(
		remoteSchemaDir(clusterID),
		manifestName,
	)
}

func remoteSSTableVersionDir(clusterID uuid.UUID, dc, nodeID, keyspace, table, version string) string {
	return path.Join(
		remoteSSTableDir(clusterID, dc, nodeID, keyspace, table),
		version,
	)
}

type dirKind string

const (
	sstDirKind    = dirKind("sst")
	metaDirKind   = dirKind("meta")
	schemaDirKind = dirKind("schema")
)

func remoteSSTableBaseDir(clusterID uuid.UUID, dc, nodeID string) string {
	return path.Join(
		"backup",
		string(sstDirKind),
		"cluster",
		clusterID.String(),
		"dc",
		dc,
		"node",
		nodeID,
	)
}

func remoteSSTableDir(clusterID uuid.UUID, dc, nodeID, keyspace, table string) string {
	return path.Join(
		remoteSSTableBaseDir(clusterID, dc, nodeID),
		"keyspace",
		keyspace,
		"table",
		table,
	)
}

func remoteManifestDir(clusterID uuid.UUID, dc, nodeID string) string {
	return path.Join(
		"backup",
		string(metaDirKind),
		"cluster",
		clusterID.String(),
		"dc",
		dc,
		"node",
		nodeID,
	)
}

func remoteSchemaDir(clusterID uuid.UUID) string {
	return path.Join(
		"backup",
		string(schemaDirKind),
		"cluster",
		clusterID.String(),
	)
}

func remoteMetaVersionFile(clusterID uuid.UUID, dc, nodeID string) string {
	return path.Join(
		remoteManifestDir(clusterID, dc, nodeID),
		metadataVersion,
	)
}

func ssTablePathWithKeyspacePrefix(keyspace, table, version, name string) string {
	return path.Join(
		"keyspace", keyspace,
		"table", table,
		version,
		name,
	)
}
