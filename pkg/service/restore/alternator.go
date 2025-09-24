// Copyright (C) 2025 ScyllaDB

package restore

import (
	"context"
	"encoding/json"
	stdErr "errors"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
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
// It is safe to pass nil client if alternator schema is empty.
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
		ksSchema[w.cqlKeyspaceName(*t.Describe.TableName)] = t
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

// alternatorInitViewsWorker contains tools needed for initializing restored alternator views.
// It basis its knowledge of alternator schema on the alternator schema described from the cluster.
type alternatorInitViewsWorker struct {
	alternatorWorker
	// In alternator, each table lives in its own keyspace named 'alternator_<table>'.
	// Table schema is stored under the keyspace name for easier lookup.
	ksSchema map[string]backupspec.AlternatorTableSchema
	client   *dynamodb.Client
}

// newAlternatorInitViewsWorker creates new alternatorInitViewsWorker.
// Units are used for filtering alternator schema according to the --keyspace flag.
// Passing nil client results in no initialized alternator views.
func newAlternatorInitViewsWorker(ctx context.Context, client *dynamodb.Client, units []Unit) (*alternatorInitViewsWorker, error) {
	ksSchema := make(map[string]backupspec.AlternatorTableSchema)
	if client == nil {
		return &alternatorInitViewsWorker{
			ksSchema: ksSchema,
			client:   client,
		}, nil
	}

	type tableKey struct {
		ks string
		t  string
	}
	filteredCQLTables := make(map[tableKey]struct{})
	for _, u := range units {
		for _, t := range u.Tables {
			filteredCQLTables[tableKey{ks: u.Keyspace, t: t.Table}] = struct{}{}
		}
	}

	schema, err := backup.GetAlternatorSchema(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "get alternator schema")
	}
	w := alternatorWorker{}
	for _, t := range schema.Tables {
		if t.Describe == nil || t.Describe.TableName == nil {
			continue
		}
		altT := *t.Describe.TableName
		altKs := w.cqlKeyspaceName(altT)
		_, ok := filteredCQLTables[tableKey{ks: altKs, t: altT}]
		if !ok {
			continue
		}
		ksSchema[altKs] = t
	}
	return &alternatorInitViewsWorker{
		ksSchema: ksSchema,
		client:   client,
	}, nil
}

// isAlternatorKeyspace checks if given keyspace is an alternator one.
func (iw *alternatorInitViewsWorker) isAlternatorKeyspace(ks string) bool {
	_, ok := iw.ksSchema[ks]
	return ok
}

// initViews initializes alternator views translating them to Views.
func (iw *alternatorInitViewsWorker) initViews() ([]RestoredView, error) {
	var views []RestoredView
	for _, schema := range iw.ksSchema {
		if schema.Describe == nil || schema.Describe.TableName == nil {
			continue
		}
		t := *schema.Describe.TableName
		for _, gsi := range schema.Describe.GlobalSecondaryIndexes {
			if gsi.IndexName == nil {
				continue
			}
			stmt := dynamodb.UpdateTableInput{
				TableName:                   aws.String(t),
				AttributeDefinitions:        iw.filterGSIAttr(*schema.Describe, gsi),
				GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{iw.gsiDescToCreateUpdate(gsi)},
			}
			rawCreateStmt, err := json.Marshal(stmt)
			if err != nil {
				return nil, err
			}
			views = append(views, RestoredView{
				View: View{
					Keyspace:  iw.cqlKeyspaceName(t),
					Name:      iw.cqlGSIName(t, *gsi.IndexName),
					Type:      AlternatorGlobalSecondaryIndex,
					BaseTable: t,
				},
				CreateStmt: string(rawCreateStmt),
			})
		}
		for _, lsi := range schema.Describe.LocalSecondaryIndexes {
			if lsi.IndexName == nil {
				continue
			}
			views = append(views, RestoredView{
				View: View{
					Keyspace:  iw.cqlKeyspaceName(t),
					Name:      iw.cqlLSIName(t, *lsi.IndexName),
					Type:      AlternatorLocalSecondaryIndex,
					BaseTable: t,
				},
			})
		}
	}
	return views, nil
}

// filterGSIAttr filters table AttributeDefinition to only those used in GSI KeySchema.
func (iw *alternatorInitViewsWorker) filterGSIAttr(desc types.TableDescription, gsiDesc types.GlobalSecondaryIndexDescription) []types.AttributeDefinition {
	gsiAttrs := make(map[string]struct{})
	for _, gsiAttr := range gsiDesc.KeySchema {
		if gsiAttr.AttributeName == nil {
			continue
		}
		gsiAttrs[*gsiAttr.AttributeName] = struct{}{}
	}
	var filtered []types.AttributeDefinition
	for _, attr := range desc.AttributeDefinitions {
		if attr.AttributeName == nil {
			continue
		}
		if _, ok := gsiAttrs[*attr.AttributeName]; ok {
			filtered = append(filtered, attr)
		}
	}
	return filtered
}

// alternatorDropViewsWorker contains tools needed for dropping restored alternator views.
// It basis its knowledge of alternator schema on the initialized Views.
type alternatorDropViewsWorker struct {
	alternatorWorker
	views  []RestoredView
	client *dynamodb.Client
}

// newAlternatorDropViewsWorker creates new alternatorDropViewsWorker.
func newAlternatorDropViewsWorker(ctx context.Context, client *dynamodb.Client, views []RestoredView) (*alternatorDropViewsWorker, error) {
	// Only existing views should be dropped
	filteredViews, err := filterAlternatorViews(ctx, client, views, true)
	if err != nil {
		return nil, errors.Wrap(err, "filter alternator views")
	}
	return &alternatorDropViewsWorker{
		views:  filteredViews,
		client: client,
	}, nil
}

// isAlternatorView checks if given view is an alternator one.
func (dw *alternatorDropViewsWorker) isAlternatorView(view RestoredView) bool {
	return view.Type == AlternatorGlobalSecondaryIndex || view.Type == AlternatorLocalSecondaryIndex
}

// dropViews drops all alternator views that should later be re-created.
func (dw *alternatorDropViewsWorker) dropViews(ctx context.Context) error {
	for _, v := range dw.views {
		update, err := dw.viewToDeleteUpdate(v)
		if err != nil {
			return errors.Wrap(err, "prepare alternator view delete update")
		}
		_, err = dw.client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName:                   aws.String(v.BaseTable),
			GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{update},
		})
		if err != nil {
			return errors.Wrap(err, "drop alternator view")
		}
	}
	return nil
}

func (dw *alternatorDropViewsWorker) viewToDeleteUpdate(view RestoredView) (types.GlobalSecondaryIndexUpdate, error) {
	switch view.Type {
	case AlternatorGlobalSecondaryIndex:
		altView, err := dw.alternatorGSIName(view.BaseTable, view.Name)
		if err != nil {
			return types.GlobalSecondaryIndexUpdate{}, err
		}
		return types.GlobalSecondaryIndexUpdate{
			Delete: &types.DeleteGlobalSecondaryIndexAction{
				IndexName: aws.String(altView),
			},
		}, nil
	default:
		return types.GlobalSecondaryIndexUpdate{}, errors.New("unsupported view type: " + string(view.Type))
	}
}

// alternatorCreateViewsWorker contains tools needed for creating restored alternator views.
// It basis its knowledge of alternator schema on the initialized Views.
type alternatorCreateViewsWorker struct {
	views  []RestoredView
	client *dynamodb.Client
}

// newAlternatorCreateViewsWorker creates new alternatorCreateViewsWorker.
func newAlternatorCreateViewsWorker(ctx context.Context, client *dynamodb.Client, views []RestoredView) (*alternatorCreateViewsWorker, error) {
	// Only non-existing views should be created
	filteredViews, err := filterAlternatorViews(ctx, client, views, false)
	if err != nil {
		return nil, errors.Wrap(err, "filter alternator views")
	}
	return &alternatorCreateViewsWorker{
		views:  filteredViews,
		client: client,
	}, nil
}

// isAlternatorView checks if given view is an alternator one.
func (cw *alternatorCreateViewsWorker) isAlternatorView(view RestoredView) bool {
	return view.Type == AlternatorGlobalSecondaryIndex || view.Type == AlternatorLocalSecondaryIndex
}

// createViews creates all alternator views that were previously dropped.
func (cw *alternatorCreateViewsWorker) createViews(ctx context.Context) error {
	for _, v := range cw.views {
		update, err := cw.viewToCreateStmt(v)
		if err != nil {
			return errors.Wrap(err, "prepare alternator view create update")
		}
		_, err = cw.client.UpdateTable(ctx, update)
		if err != nil {
			return errors.Wrap(err, "create alternator view")
		}
	}
	return nil
}

func (cw *alternatorCreateViewsWorker) viewToCreateStmt(view RestoredView) (*dynamodb.UpdateTableInput, error) {
	switch view.Type {
	case AlternatorGlobalSecondaryIndex:
		var update dynamodb.UpdateTableInput
		if err := json.Unmarshal([]byte(view.CreateStmt), &update); err != nil {
			return nil, errors.Wrap(err, "unmarshal initialized alternator GSI create update")
		}
		return &update, nil
	default:
		return nil, errors.New("unsupported view type: " + string(view.Type))
	}
}

// filterAlternatorViews is a helper function used for initialization of alternatorDropViewsWorker and alternatorCreateViewsWorker.
// The exist parameter specifies if we want to filter for existing or non-existing views in the current cluster schema.
// Since we don't drop and re-create alternator LSIs, we only need to filter for GSIs.
func filterAlternatorViews(ctx context.Context, client *dynamodb.Client, views []RestoredView, exist bool) ([]RestoredView, error) {
	if client == nil {
		if ok := slices.ContainsFunc(views, func(v RestoredView) bool { return v.Type == AlternatorGlobalSecondaryIndex }); ok {
			return nil, errors.New("uninitialized alternator client with non-empty alternator schema")
		}
		return []RestoredView{}, nil
	}

	schema, err := backup.GetAlternatorSchema(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "get alternator schema")
	}

	type viewKey struct {
		t string // CQL table name
		v string // CQL view name
	}
	existingViews := make(map[viewKey]struct{})
	w := alternatorWorker{}
	for _, t := range schema.Tables {
		if t.Describe == nil || t.Describe.TableName == nil {
			continue
		}
		for _, gsi := range t.Describe.GlobalSecondaryIndexes {
			if gsi.IndexName == nil {
				continue
			}
			altT := *t.Describe.TableName
			altV := *gsi.IndexName
			existingViews[viewKey{t: altT, v: w.cqlGSIName(altT, altV)}] = struct{}{}
		}
	}

	var filteredViews []RestoredView
	for _, v := range views {
		if v.Type != AlternatorGlobalSecondaryIndex {
			continue
		}
		if _, ok := existingViews[viewKey{t: v.BaseTable, v: v.Name}]; ok == exist {
			filteredViews = append(filteredViews, v)
		}
	}
	return filteredViews, nil
}

// alternatorWorker contains basic tools for handling alternator schema.
type alternatorWorker struct{}

func (w alternatorWorker) cqlKeyspaceName(table string) string {
	return "alternator_" + table
}

func (w alternatorWorker) cqlGSIName(table, gsi string) string {
	return table + ":" + gsi
}

func (w alternatorWorker) cqlLSIName(table, lsi string) string {
	return table + "!:" + lsi
}

func (w alternatorWorker) alternatorGSIName(table, cqlGSI string) (string, error) {
	gsi, ok := strings.CutPrefix(cqlGSI, table+":")
	if !ok {
		return "", errors.Errorf("%q is not a valid alternator GSI name for table %q", cqlGSI, table)
	}
	return gsi, nil
}

func (w alternatorWorker) alternatorLSIName(table, cqlLSI string) (string, error) { //nolint: unused
	lsi, ok := strings.CutPrefix(cqlLSI, table+"!:")
	if !ok {
		return "", errors.Errorf("%q is not a valid alternator LSI name for table %q", cqlLSI, table)
	}
	return lsi, nil
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

func (w alternatorWorker) gsiDescToCreateUpdate(desc types.GlobalSecondaryIndexDescription) types.GlobalSecondaryIndexUpdate {
	return types.GlobalSecondaryIndexUpdate{
		Create: &types.CreateGlobalSecondaryIndexAction{
			IndexName:             desc.IndexName,
			KeySchema:             desc.KeySchema,
			Projection:            desc.Projection,
			OnDemandThroughput:    desc.OnDemandThroughput,
			ProvisionedThroughput: w.gsiProvisionedThroughputDescToCreate(desc.ProvisionedThroughput),
			WarmThroughput:        w.gsiWarmThroughputDescToCreate(desc.WarmThroughput),
		},
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
