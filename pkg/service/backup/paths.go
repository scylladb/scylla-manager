// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/util/uuid"
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

// Adapted from Scylla's sstable detection code
// https://github.com/scylladb/scylla/blob/bb2e04cc8b8152bbe11749d79f0f136335c77602/sstables/sstables.cc#L2724
var (
	laMcFileNameRe                = `(?:la|mc)-\d+-\w+(-.*)`
	kaFileNameRe                  = `\w+-\w+-ka-\d+(-.*)`
	keyspaceTableNameRe           = `[a-zA-Z0-9_]+`
	tableVersionRe                = `[a-f0-9]{32}`
	keyspaceTableVersionPatternRe = `^keyspace/` + keyspaceTableNameRe + `/table/` + keyspaceTableNameRe + `/` + tableVersionRe + `/`

	keyspaceTableVersionLaMcRe     = regexp.MustCompile(keyspaceTableVersionPatternRe + laMcFileNameRe)
	keyspaceTableVersionKaRe       = regexp.MustCompile(keyspaceTableVersionPatternRe + kaFileNameRe)
	keyspaceTableVersionManifestRe = regexp.MustCompile(keyspaceTableVersionPatternRe + scyllaManifest)
)

// groupingKey returns key which can be used for grouping SSTable files.
// SSTable representation in snapshot consists of few files sharing same prefix,
// this key allows to group these files.
func groupingKey(path string) (string, error) {
	m := keyspaceTableVersionLaMcRe.FindStringSubmatch(path)
	if m != nil {
		return strings.TrimSuffix(path, m[1]), nil
	}
	m = keyspaceTableVersionKaRe.FindStringSubmatch(path)
	if m != nil {
		return strings.TrimSuffix(path, m[1]), nil
	}
	m = keyspaceTableVersionManifestRe.FindStringSubmatch(path)
	if m != nil {
		return path, nil
	}

	return "", errors.New("file path does not match sstable patterns")
}

func ssTablePathWithKeyspacePrefix(keyspace, table, version, name string) string {
	return path.Join(
		"keyspace", keyspace,
		"table", table,
		version,
		name,
	)
}
