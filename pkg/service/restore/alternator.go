// Copyright (C) 2025 ScyllaDB

package restore

import (
	"context"
	stdErr "errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	slices2 "github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
)

// alternatorSchemaWorker contains tools needed for restoring alternator schema.
// It basis its knowledge of alternator schema on the alternator schema backup file.
type alternatorSchemaWorker struct {
	alternatorWorker
	// In alternator, each table lives in its own keyspace named 'alternator_<table>'.
	// Table schema is stored under the keyspace name for easier lookup.
	ksSchema map[string]backupspec.AlternatorTableSchema
	client   *dynamodb.Client
}

// newAlternatorSchemaWorker creates new alternatorSchemaWorker.
func newAlternatorSchemaWorker(client *dynamodb.Client, schema backupspec.AlternatorSchema) (*alternatorSchemaWorker, error) {
	if client == nil && len(schema.Tables) > 0 {
		return nil, errors.New("uninitialized alternator client with non-empty alternator schema")
	}
	w := alternatorWorker{}
	ksSchema := make(map[string]backupspec.AlternatorTableSchema, len(schema.Tables))
	for _, t := range schema.Tables {
		if t.Describe == nil || t.Describe.TableName == nil {
			continue
		}
		ksSchema[w.alternatorKeyspace(*t.Describe.TableName)] = t
	}
	return &alternatorSchemaWorker{
		ksSchema: ksSchema,
		client:   client,
	}, nil
}

// isAlternatorKeyspace checks if given query.DescribedSchemaRow describes alternator schema.
func (sw *alternatorSchemaWorker) isAlternatorSchemaRow(cql query.DescribedSchemaRow) bool {
	_, ok := sw.ksSchema[sw.sanitizeCQLKeyspace(cql)]
	return ok
}

func (sw *alternatorSchemaWorker) restore(ctx context.Context) (err error) {
	var createdTables []string
	defer func() {
		if err != nil {
			for _, t := range createdTables {
				_, delErr := sw.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
					TableName: aws.String(t),
				})
				if delErr != nil {
					err = stdErr.Join(err, errors.Wrapf(delErr, "rollback creation of alternator table "+t))
					return
				}
			}
		}
	}()

	for _, schema := range sw.ksSchema {
		createIn := sw.tableSchemaToCreate(schema)
		_, err = sw.client.CreateTable(ctx, &createIn)
		if err != nil {
			return errors.Wrapf(err, "create alternator table %q", *createIn.TableName)
		}
		createdTables = append(createdTables, *createIn.TableName)
	}
	return nil
}

// tableSchemaToCreate converts backupspec.AlternatorTableSchema to dynamodb.CreateTableInput.
func (sw *alternatorSchemaWorker) tableSchemaToCreate(schema backupspec.AlternatorTableSchema) dynamodb.CreateTableInput {
	if schema.Describe == nil {
		return dynamodb.CreateTableInput{}
	}
	return dynamodb.CreateTableInput{
		AttributeDefinitions:      schema.Describe.AttributeDefinitions,
		KeySchema:                 schema.Describe.KeySchema,
		TableName:                 schema.Describe.TableName,
		BillingMode:               sw.billingModeSummToCreate(schema.Describe.BillingModeSummary),
		DeletionProtectionEnabled: schema.Describe.DeletionProtectionEnabled,
		GlobalSecondaryIndexes:    slices2.Map(schema.Describe.GlobalSecondaryIndexes, sw.gsiDescToCreate),
		LocalSecondaryIndexes:     slices2.Map(schema.Describe.LocalSecondaryIndexes, sw.lsiDescToCreate),
		OnDemandThroughput:        schema.Describe.OnDemandThroughput,
		ProvisionedThroughput:     sw.tableProvisionedThroughputDescToCreate(schema.Describe),
		// ResourcePolicy: not backed up nor restored because RBAC is handled via CQL.
		// See https://docs.scylladb.com/manual/stable/alternator/compatibility.html#authentication-and-authorization.
		SSESpecification:    sw.sseDescToCreate(schema.Describe.SSEDescription),
		StreamSpecification: schema.Describe.StreamSpecification,
		TableClass:          sw.tableClassSummToCreate(schema.Describe.TableClassSummary),
		Tags:                schema.Tags,
		WarmThroughput:      sw.tableWarmThroughputDescToCreate(schema.Describe.WarmThroughput),
	}
}

// sanitizeCQLKeyspace removes quotes from keyspace name in query.DescribedSchemaRow.
func (sw *alternatorSchemaWorker) sanitizeCQLKeyspace(cql query.DescribedSchemaRow) string {
	return strings.TrimPrefix(strings.TrimSuffix(cql.Keyspace, "\""), "\"")
}

// alternatorWorker contains basic tools for handling alternator schema.
type alternatorWorker struct{}

func (w alternatorWorker) alternatorKeyspace(table string) string {
	return "alternator_" + table
}

func (w alternatorWorker) gsiDescToCreate(desc types.GlobalSecondaryIndexDescription) types.GlobalSecondaryIndex {
	return types.GlobalSecondaryIndex{
		IndexName:             desc.IndexName,
		KeySchema:             desc.KeySchema,
		Projection:            desc.Projection,
		OnDemandThroughput:    desc.OnDemandThroughput,
		ProvisionedThroughput: w.gsiProvisionedThroughputDescToCreate(desc.ProvisionedThroughput),
		WarmThroughput:        w.gsiWarmThroughputDescToCreate(desc.WarmThroughput),
	}
}

func (w alternatorWorker) lsiDescToCreate(desc types.LocalSecondaryIndexDescription) types.LocalSecondaryIndex {
	return types.LocalSecondaryIndex{
		IndexName:  desc.IndexName,
		KeySchema:  desc.KeySchema,
		Projection: desc.Projection,
	}
}

func (w alternatorWorker) tableProvisionedThroughputDescToCreate(desc *types.TableDescription) *types.ProvisionedThroughput {
	// For some reason, alternator API returns non-nil TableDescription.ProvisionedThroughput even for types.BillingModePayPerRequest.
	// We need to check for it in TableDescription.BillingModeSummary and remove it if not needed.
	// Otherwise, dynamodb client request will fail at validation stage.
	if desc.ProvisionedThroughput == nil || desc.BillingModeSummary == nil || desc.BillingModeSummary.BillingMode != types.BillingModeProvisioned {
		return nil
	}
	return &types.ProvisionedThroughput{
		ReadCapacityUnits:  desc.ProvisionedThroughput.ReadCapacityUnits,
		WriteCapacityUnits: desc.ProvisionedThroughput.WriteCapacityUnits,
	}
}

func (w alternatorWorker) gsiProvisionedThroughputDescToCreate(desc *types.ProvisionedThroughputDescription) *types.ProvisionedThroughput {
	if desc == nil {
		return nil
	}
	return &types.ProvisionedThroughput{
		ReadCapacityUnits:  desc.ReadCapacityUnits,
		WriteCapacityUnits: desc.WriteCapacityUnits,
	}
}

func (w alternatorWorker) gsiWarmThroughputDescToCreate(desc *types.GlobalSecondaryIndexWarmThroughputDescription) *types.WarmThroughput {
	if desc == nil {
		return nil
	}
	return &types.WarmThroughput{
		ReadUnitsPerSecond:  desc.ReadUnitsPerSecond,
		WriteUnitsPerSecond: desc.WriteUnitsPerSecond,
	}
}

func (w alternatorWorker) tableWarmThroughputDescToCreate(desc *types.TableWarmThroughputDescription) *types.WarmThroughput {
	if desc == nil {
		return nil
	}
	return &types.WarmThroughput{
		ReadUnitsPerSecond:  desc.ReadUnitsPerSecond,
		WriteUnitsPerSecond: desc.WriteUnitsPerSecond,
	}
}

func (w alternatorWorker) sseDescToCreate(desc *types.SSEDescription) *types.SSESpecification {
	if desc == nil {
		return nil
	}
	return &types.SSESpecification{
		Enabled:        aws.Bool(desc.Status == types.SSEStatusEnabled),
		KMSMasterKeyId: desc.KMSMasterKeyArn,
		SSEType:        desc.SSEType,
	}
}

func (w alternatorWorker) billingModeSummToCreate(desc *types.BillingModeSummary) types.BillingMode {
	if desc == nil {
		return ""
	}
	return desc.BillingMode
}

func (w alternatorWorker) tableClassSummToCreate(desc *types.TableClassSummary) types.TableClass {
	if desc == nil {
		return ""
	}
	return desc.TableClass
}
