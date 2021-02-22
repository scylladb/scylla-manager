// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
	"path"
	"strings"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

const (
	// MetadataVersion is the suffix for version file.
	MetadataVersion = ".version"
	// Manifest is name of the manifest file.
	Manifest = "manifest.json.gz"
	// Schema is the name of the schema file.
	Schema = "schema.tar.gz"
	// TempFileExt is suffix for the temporary files.
	TempFileExt = ".tmp"

	sep = string(os.PathSeparator)
)

// RemoteManifestFile returns path to the manifest file.
func RemoteManifestFile(clusterID, taskID uuid.UUID, snapshotTag, dc, nodeID string) string {
	manifestName := strings.Join([]string{
		"task",
		taskID.String(),
		"tag",
		snapshotTag,
		Manifest,
	}, "_")

	return path.Join(
		RemoteManifestDir(clusterID, dc, nodeID),
		manifestName,
	)
}

// RemoteSchemaFile returns path to the schema file.
func RemoteSchemaFile(clusterID, taskID uuid.UUID, snapshotTag string) string {
	manifestName := strings.Join([]string{
		"task",
		taskID.String(),
		"tag",
		snapshotTag,
		Schema,
	}, "_")

	return path.Join(
		remoteSchemaDir(clusterID),
		manifestName,
	)
}

// RemoteSSTableVersionDir returns path to the sstable version directory.
func RemoteSSTableVersionDir(clusterID uuid.UUID, dc, nodeID, keyspace, table, version string) string {
	return path.Join(
		remoteSSTableDir(clusterID, dc, nodeID, keyspace, table),
		version,
	)
}

type dirKind string

// Enumeration of dirKinds.
const (
	SchemaDirKind = dirKind("schema")
	SSTDirKind    = dirKind("sst")
	MetaDirKind   = dirKind("meta")
)

// RemoteSSTableBaseDir returns path to the sstable base directory.
func RemoteSSTableBaseDir(clusterID uuid.UUID, dc, nodeID string) string {
	return path.Join(
		"backup",
		string(SSTDirKind),
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
		RemoteSSTableBaseDir(clusterID, dc, nodeID),
		"keyspace",
		keyspace,
		"table",
		table,
	)
}

// RemoteManifestDir returns path to the manifest directory.
func RemoteManifestDir(clusterID uuid.UUID, dc, nodeID string) string {
	return path.Join(
		"backup",
		string(MetaDirKind),
		"cluster",
		clusterID.String(),
		"dc",
		dc,
		"node",
		nodeID,
	)
}

// RemoteMetaClusterDCDir returns path to DC dir for the provided cluster.
func RemoteMetaClusterDCDir(clusterID uuid.UUID) string {
	return path.Join(
		"backup",
		string(MetaDirKind),
		"cluster",
		clusterID.String(),
		"dc",
	)
}

func remoteSchemaDir(clusterID uuid.UUID) string {
	return path.Join(
		"backup",
		string(SchemaDirKind),
		"cluster",
		clusterID.String(),
	)
}

// RemoteMetaVersionFile returns path to the manifest version file.
func RemoteMetaVersionFile(clusterID uuid.UUID, dc, nodeID string) string {
	return path.Join(
		RemoteManifestDir(clusterID, dc, nodeID),
		MetadataVersion,
	)
}

// TempFile returns temporary path for the provided file.
func TempFile(f string) string {
	return f + TempFileExt
}
