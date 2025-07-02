// Copyright (C) 2025 ScyllaDB

package backup

import (
	"compress/gzip"
	"context"
	"encoding/json"
	stdErr "errors"
	"io"
	"path"
	"regexp"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// DescribeSchemaFilter allows for faster and more robust schema file lookup.
type DescribeSchemaFilter struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
}

// GetDescribeSchema fetches the backed up schema.
func (s *Service) GetDescribeSchema(ctx context.Context, clusterID uuid.UUID, snapshotTag string, location backupspec.Location, filter DescribeSchemaFilter,
) (query.DescribedSchema, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "create client")
	}

	host, err := getHostForLocation(ctx, client, location)
	if err != nil {
		return nil, errors.Wrap(err, "get host for location")
	}
	s.logger.Info(ctx, "Found host for fetching schema", "host", host)

	schemaFilePath, err := getSchemaFilePath(ctx, client, host, location, snapshotTag, filter, s.logger)
	if err != nil {
		return nil, errors.Wrap(err, "get schema file path")
	}
	s.logger.Info(ctx, "Found schema file path", "path", schemaFilePath)

	schema, err := readSchemaFile(ctx, client, host, schemaFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "read schema file")
	}

	return schema, nil
}

func getHostForLocation(ctx context.Context, client *scyllaclient.Client, loc backupspec.Location) (string, error) {
	status, err := client.Status(ctx)
	if err != nil {
		return "", errors.Wrap(err, "get status")
	}
	if len(status) == 0 {
		return "", errors.New("no nodes in cluster")
	}
	if loc.DC != "" {
		status = status.Datacenter([]string{loc.DC})
	}
	if len(status) == 0 {
		return "", errors.New("no nodes in DC " + loc.DC)
	}
	return status[0].Addr, nil
}

func getSchemaFilePath(ctx context.Context, client *scyllaclient.Client, host string, loc backupspec.Location, tag string, f DescribeSchemaFilter, log log.Logger) (string, error) {
	if f.ClusterID != uuid.Nil && f.TaskID != uuid.Nil {
		return loc.RemotePath(backupspec.RemoteSchemaFile(f.ClusterID, f.TaskID, tag)), nil
	}

	baseDir := path.Join("backup", string(backupspec.SchemaDirKind), "cluster")
	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}
	if f.ClusterID != uuid.Nil {
		baseDir = path.Join(baseDir, f.ClusterID.String())
		opts.Recurse = false
	}

	entries, err := client.RcloneListDir(ctx, host, loc.RemotePath(baseDir), &opts)
	if err != nil {
		return "", errors.Wrapf(err, "list directory %s on host %s", loc.RemotePath(baseDir), host)
	}

	var schemaFilePath string
	var parseErr error
	for _, entry := range entries {
		entryTaskID, entryTag, err := ParseSchemaFileName(entry.Name)
		if err != nil {
			log.Info(ctx, "Couldn't parse schema file name", "name", entry.Name, "error", err)
			parseErr = stdErr.Join(parseErr, errors.Wrap(err, entry.Name))
			continue
		}
		if f.TaskID != uuid.Nil && f.TaskID != entryTaskID {
			continue
		}
		if tag != entryTag {
			continue
		}

		if schemaFilePath != "" {
			return "", errors.Errorf("multiple schema files found (%s, %s)", schemaFilePath, entry.Path)
		}
		schemaFilePath = entry.Path
	}
	if schemaFilePath == "" {
		if parseErr != nil {
			return "", errors.Wrap(parseErr, "parse schema file name")
		}
		return "", errors.New("no schema file found")
	}

	return loc.RemotePath(path.Join(baseDir, schemaFilePath)), nil
}

var schemaFileNameRegex = regexp.MustCompile(`task_(.*)_tag_(.*)_schema_with_internals\.json\.gz`)

const (
	taskIDSubmatchIdx      = 1
	snapshotTagSubmatchIdx = 2
	expectedSubmatchCount  = 3
)

// ParseSchemaFileName parses backed up schema file name into task ID and snapshot tag.
func ParseSchemaFileName(name string) (taskID uuid.UUID, tag string, err error) {
	m := schemaFileNameRegex.FindStringSubmatch(name)
	if len(m) != expectedSubmatchCount {
		return uuid.Nil, "", errors.New("unexpected schema file name format " + name)
	}

	taskID, err = uuid.Parse(m[taskIDSubmatchIdx])
	if err != nil {
		return uuid.Nil, "", errors.Wrapf(err, "parse task ID")
	}

	tag = m[snapshotTagSubmatchIdx]
	if !backupspec.IsSnapshotTag(tag) {
		return uuid.Nil, "", errors.New("unexpected snapshot tag format " + tag)
	}

	return taskID, tag, nil
}

func readSchemaFile(ctx context.Context, client *scyllaclient.Client, host, schemaFilePath string) (schema query.DescribedSchema, err error) {
	r, err := client.RcloneOpen(ctx, host, schemaFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "open schema file")
	}
	defer func() {
		err = stdErr.Join(err, errors.Wrap(r.Close(), "close schema file reader"))
	}()

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "create gzip reader")
	}
	defer func() {
		err = stdErr.Join(err, errors.Wrap(gzr.Close(), "close gzip reader"))
	}()

	rawSchema, err := io.ReadAll(gzr)
	if err != nil {
		return nil, errors.Wrap(err, "decompress schema")
	}

	if err := json.Unmarshal(rawSchema, &schema); err != nil {
		return nil, errors.Wrap(err, "unmarshal schema")
	}

	return schema, nil
}
