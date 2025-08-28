// Copyright (C) 2025 ScyllaDB

package backup

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
)

func (w *worker) DumpAlternatorSchema(ctx context.Context, his []hostInfo, alternatorFunc cluster.AlternatorClientFunc) error {
	hi, ok := getAlternatorHost(his)
	if !ok {
		// Nothing to do if alternator is not enabled
		return nil
	}
	w.Logger.Info(ctx, "Chose node for alternator schema dump", "node", hi.IP)

	if ok, err := hi.NodeConfig.SupportsAlternatorSchemaBackupFromAPI(); err != nil {
		return errors.Wrap(err, "check alternator schema backup support")
	} else if !ok {
		// Nothing to do if alternator schema should be restored from sstables
		return nil
	}

	// All supported versions which support alternator schema
	// backup from API also support raft read barrier API.
	if err := w.Client.RaftReadBarrier(ctx, hi.IP, ""); err != nil {
		return errors.Wrap(err, "make raft read barrier")
	}

	client, err := alternatorFunc(ctx, w.ClusterID, hi.IP)
	if err != nil {
		return errors.Wrapf(err, "create alternator client to node")
	}

	schema, err := w.getAlternatorSchema(ctx, client)
	if err != nil {
		return errors.Wrap(err, "get alternator schema")
	}

	archive, err := marshalAndCompressArchive(schema)
	if err != nil {
		return errors.Wrap(err, "create alternator schema archive")
	}

	w.Logger.Info(ctx, "Created alternator schema archive")
	w.AlternatorSchema = archive
	return nil
}

func (w *worker) UploadAlternatorSchema(ctx context.Context, his []hostInfo) error {
	if w.AlternatorSchema.Len() == 0 {
		w.Logger.Info(ctx, "No alternator schema to upload")
		return nil
	}

	visited := map[backupspec.Location]struct{}{}
	for i := range his {
		if _, ok := visited[his[i].Location]; ok {
			continue
		}
		visited[his[i].Location] = struct{}{}

		remoteAlternatorSchemaPath := his[i].Location.RemotePath(backupspec.AlternatorSchemaPath(w.ClusterID, w.TaskID, w.SnapshotTag))
		if err := w.Client.RclonePut(ctx, his[i].IP, remoteAlternatorSchemaPath, &w.AlternatorSchema); err != nil {
			return errors.Wrapf(err, "upload alternator schema from node (%q) to location (%q)", his[i].IP, remoteAlternatorSchemaPath)
		}
	}

	return nil
}

// getAlternatorHost returns IP of host (if any) with alternator enabled.
func getAlternatorHost(his []hostInfo) (hostInfo, bool) {
	for i := range his {
		if his[i].NodeConfig.AlternatorEnabled() {
			return his[i], true
		}
	}
	return hostInfo{}, false
}

// getAlternatorSchema queries alternator API in order to get alternator schema.
func (w *worker) getAlternatorSchema(ctx context.Context, client *dynamodb.Client) (backupspec.AlternatorSchema, error) {
	tableNames, err := listAllTables(ctx, client)
	if err != nil {
		return backupspec.AlternatorSchema{}, errors.Wrap(err, "list all alternator tables")
	}

	var tableSchema []backupspec.AlternatorTableSchema
	for _, name := range tableNames {
		describeOut, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: &name,
		})
		if err != nil {
			return backupspec.AlternatorSchema{}, errors.Wrapf(err, "describe alternator table: %q", name)
		}

		tags, err := listAllTags(ctx, client, *describeOut.Table.TableArn)
		if err != nil {
			return backupspec.AlternatorSchema{}, errors.Wrapf(err, "list tags of alternator table: %q", name)
		}

		TTLOut, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
			TableName: &name,
		})
		if err != nil {
			return backupspec.AlternatorSchema{}, errors.Wrapf(err, "describe TTL of alternator table: %q", name)
		}

		tableSchema = append(tableSchema, backupspec.AlternatorTableSchema{
			Describe: describeOut.Table,
			Tags:     tags,
			TTL:      TTLOut.TimeToLiveDescription,
		})
	}
	return backupspec.AlternatorSchema{
		Tables: tableSchema,
	}, nil
}

// listAllTables is a wrapper for ListTables which returns all tables.
func listAllTables(ctx context.Context, client *dynamodb.Client) ([]string, error) {
	var tableNames []string
	var exclusiveStartTableName *string
	for {
		listOut, err := client.ListTables(ctx, &dynamodb.ListTablesInput{
			ExclusiveStartTableName: exclusiveStartTableName,
		})
		if err != nil {
			return nil, errors.Wrap(err, "list tables")
		}
		tableNames = append(tableNames, listOut.TableNames...)
		exclusiveStartTableName = listOut.LastEvaluatedTableName
		if exclusiveStartTableName == nil {
			return tableNames, nil
		}
	}
}

// listAllTags is a wrapper for ListTagsOfResource which returns all tags.
func listAllTags(ctx context.Context, client *dynamodb.Client, resourceArn string) ([]types.Tag, error) {
	var tags []types.Tag
	var nextToken *string
	for {
		listTagsOut, err := client.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{
			ResourceArn: &resourceArn,
			NextToken:   nextToken,
		})
		if err != nil {
			return nil, errors.Wrap(err, "list tags of resource")
		}
		tags = append(tags, listTagsOut.Tags...)
		nextToken = listTagsOut.NextToken
		if nextToken == nil {
			return tags, nil
		}
	}
}
