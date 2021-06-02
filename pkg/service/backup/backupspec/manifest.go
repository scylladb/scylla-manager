// Copyright (C) 2017 ScyllaDB

package backupspec

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"path"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/pkg/util/pathparser"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// ManifestInfo represents manifest on remote location.
type ManifestInfo struct {
	Location    Location
	DC          string
	ClusterID   uuid.UUID
	NodeID      string
	TaskID      uuid.UUID
	SnapshotTag string
	Temporary   bool
}

// Path returns path to the file that manifest points to.
func (m *ManifestInfo) Path() string {
	f := RemoteManifestFile(m.ClusterID, m.TaskID, m.SnapshotTag, m.DC, m.NodeID)
	if m.Temporary {
		f = TempFile(f)
	}
	return f
}

// SchemaPath returns path to the schema file that manifest points to.
func (m *ManifestInfo) SchemaPath() string {
	return RemoteSchemaFile(m.ClusterID, m.TaskID, m.SnapshotTag)
}

// SSTableVersionDir returns path to the sstable version directory.
func (m *ManifestInfo) SSTableVersionDir(keyspace, table, version string) string {
	return RemoteSSTableVersionDir(m.ClusterID, m.DC, m.NodeID, keyspace, table, version)
}

// ParsePath extracts properties from full remote path to manifest.
func (m *ManifestInfo) ParsePath(s string) error {
	// Clear values
	*m = ManifestInfo{}

	// Clean path for usage with strings.Split
	s = strings.TrimPrefix(path.Clean(s), sep)

	parsers := []pathparser.Parser{
		pathparser.Static("backup"),
		pathparser.Static(string(MetaDirKind)),
		pathparser.Static("cluster"),
		pathparser.ID(&m.ClusterID),
		pathparser.Static("dc"),
		pathparser.String(&m.DC),
		pathparser.Static("node"),
		pathparser.String(&m.NodeID),
		m.fileNameParser,
	}
	n, err := pathparser.New(s, sep).Parse(parsers...)
	if err != nil {
		return err
	}
	if n < len(parsers) {
		return errors.Errorf("no input at position %d", n)
	}

	m.Temporary = strings.HasSuffix(s, TempFileExt)

	return nil
}

func (m *ManifestInfo) fileNameParser(v string) error {
	parsers := []pathparser.Parser{
		pathparser.Static("task"),
		pathparser.ID(&m.TaskID),
		pathparser.Static("tag"),
		pathparser.Static("sm"),
		func(v string) error {
			tag := "sm_" + v
			if !IsSnapshotTag(tag) {
				return errors.Errorf("invalid snapshot tag %s", tag)
			}
			m.SnapshotTag = tag
			return nil
		},
		pathparser.Static(Manifest, TempFile(Manifest)),
	}

	n, err := pathparser.New(v, "_").Parse(parsers...)
	if err != nil {
		return err
	}
	if n < len(parsers) {
		return errors.Errorf("input too short")
	}
	return nil
}

// ManifestContent is structure containing information about the backup.
type ManifestContent struct {
	Version     string      `json:"version"`
	ClusterName string      `json:"cluster_name"`
	IP          string      `json:"ip"`
	Index       []FilesMeta `json:"index"`
	Size        int64       `json:"size"`
	Tokens      []int64     `json:"tokens"`
	Schema      string      `json:"schema"`
}

func (m *ManifestContent) Read(r io.Reader) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}

	if err := json.NewDecoder(gr).Decode(m); err != nil {
		return err
	}
	return gr.Close()
}

func (m *ManifestContent) Write(w io.Writer) error {
	gw := gzip.NewWriter(w)

	if err := json.NewEncoder(gw).Encode(m); err != nil {
		return err
	}

	return gw.Close()
}

// ManifestInfoWithContent is intended for passing manifest with its content.
type ManifestInfoWithContent struct {
	*ManifestInfo
	*ManifestContent
}

func NewManifestInfoWithContent() ManifestInfoWithContent {
	return ManifestInfoWithContent{
		ManifestInfo:    new(ManifestInfo),
		ManifestContent: new(ManifestContent),
	}
}

// FilesInfo specifies paths to files backed up for a table (and node) within
// a location.
// Note that a backup for a table usually consists of multiple instances of
// FilesInfo since data is replicated across many nodes.
type FilesInfo struct {
	Location Location    `json:"location"`
	Schema   string      `json:"schema"`
	Files    []FilesMeta `json:"files"`
}

// FilesMeta contains information about SST files of particular keyspace/table.
type FilesMeta struct {
	Keyspace string   `json:"keyspace"`
	Table    string   `json:"table"`
	Version  string   `json:"version"`
	Files    []string `json:"files"`
	Size     int64    `json:"size"`

	Path string `json:"path,omitempty"`
}

// MakeFilesInfo creates new files info from the provided manifest with applied
// filter.
func MakeFilesInfo(m ManifestInfoWithContent, filter *ksfilter.Filter) FilesInfo {
	// Clear DC from location. DC part litters files listing and makes it
	// incompatible with other tools like AWS cli.
	l := m.Location
	l.DC = ""

	fi := FilesInfo{
		Location: l,
		Schema:   m.Schema,
	}

	for _, idx := range m.Index {
		if !filter.Check(idx.Keyspace, idx.Table) {
			continue
		}
		idx.Path = m.SSTableVersionDir(idx.Keyspace, idx.Table, idx.Version)
		fi.Files = append(fi.Files, idx)
	}

	return fi
}
