// Copyright (C) 2026 ScyllaDB

package restore

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
)

func TestGSIProvisionedThroughputDescToCreate(t *testing.T) {
	table := func(mode types.BillingMode) *types.ProvisionedThroughput {
		desc := &types.TableDescription{
			BillingModeSummary: &types.BillingModeSummary{BillingMode: mode},
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(7),
			},
		}
		return alternatorWorker{}.tableProvisionedThroughputDescToCreate(desc)
	}
	gsi := func(rcu, wcu int64) *types.ProvisionedThroughputDescription {
		return &types.ProvisionedThroughputDescription{
			ReadCapacityUnits:  aws.Int64(rcu),
			WriteCapacityUnits: aws.Int64(wcu),
		}
	}
	throughput := func(rcu, wcu int64) *types.ProvisionedThroughput {
		return &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(rcu),
			WriteCapacityUnits: aws.Int64(wcu),
		}
	}

	testCases := []struct {
		name     string
		table    *types.ProvisionedThroughput
		gsi      *types.ProvisionedThroughputDescription
		expected *types.ProvisionedThroughput
	}{
		{name: "pay per request, zero gsi", table: table(types.BillingModePayPerRequest), gsi: gsi(0, 0)},
		{name: "pay per request, no gsi", table: table(types.BillingModePayPerRequest)},
		{name: "no billing mode", gsi: gsi(3, 4)},
		{name: "provisioned", table: table(types.BillingModeProvisioned), gsi: gsi(3, 4), expected: throughput(3, 4)},
		{name: "provisioned, zero gsi", table: table(types.BillingModeProvisioned), gsi: gsi(0, 0), expected: throughput(5, 7)},
		{name: "provisioned, partial gsi", table: table(types.BillingModeProvisioned), gsi: gsi(3, 0), expected: throughput(3, 7)},
		{name: "provisioned, no gsi", table: table(types.BillingModeProvisioned), expected: throughput(5, 7)},
		{name: "provisioned, zero table and gsi", table: throughput(0, 0), gsi: gsi(0, 0), expected: throughput(1, 1)},
		{name: "provisioned, nil table values", table: &types.ProvisionedThroughput{}, gsi: gsi(3, 0), expected: throughput(3, 1)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := alternatorWorker{}.gsiProvisionedThroughputDescToCreate(tc.table, tc.gsi)
			if diff := cmp.Diff(tc.expected, got, cmp.AllowUnexported(types.ProvisionedThroughput{})); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestAlternatorGSIThroughputWiring(t *testing.T) {
	describe := func(name string, mode types.BillingMode, rcu, wcu int64) *types.TableDescription {
		return &types.TableDescription{
			TableName:          aws.String(name),
			BillingModeSummary: &types.BillingModeSummary{BillingMode: mode},
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  aws.Int64(rcu),
				WriteCapacityUnits: aws.Int64(wcu),
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{{
				IndexName: aws.String("gsi"),
				ProvisionedThroughput: &types.ProvisionedThroughputDescription{
					ReadCapacityUnits:  aws.Int64(rcu),
					WriteCapacityUnits: aws.Int64(wcu),
				},
			}},
		}
	}
	expected := &types.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(7)}

	testCases := []struct {
		name     string
		desc     *types.TableDescription
		expected *types.ProvisionedThroughput
	}{
		{name: "provisioned", desc: describe("prov", types.BillingModeProvisioned, 5, 7), expected: expected},
		{name: "pay per request", desc: describe("ppr", types.BillingModePayPerRequest, 0, 0)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema := backupspec.AlternatorTableSchema{Describe: tc.desc}
			opt := cmp.AllowUnexported(types.ProvisionedThroughput{})

			create := (&alternatorSchemaWorker{}).tableSchemaToCreate(schema)
			if diff := cmp.Diff(tc.expected, create.ProvisionedThroughput, opt); diff != "" {
				t.Fatal("table:", diff)
			}
			if diff := cmp.Diff(tc.expected, create.GlobalSecondaryIndexes[0].ProvisionedThroughput, opt); diff != "" {
				t.Fatal("create table gsi:", diff)
			}

			iw := &alternatorInitViewsWorker{ksSchema: map[string]backupspec.AlternatorTableSchema{"ks": schema}}
			views, err := iw.initViews()
			if err != nil {
				t.Fatal(err)
			}
			update, err := (&alternatorCreateViewsWorker{}).viewToCreateStmt(views[0])
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.expected, update.GlobalSecondaryIndexUpdates[0].Create.ProvisionedThroughput, opt); diff != "" {
				t.Fatal("update table gsi:", diff)
			}
		})
	}
}

func TestViewToCreateStmtDropsStaleGSIThroughput(t *testing.T) {
	// Older SM stored the 0/0 GSI throughput of PAY_PER_REQUEST tables in the run's views.
	stmt, err := json.Marshal(dynamodb.UpdateTableInput{
		TableName: aws.String("t"),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{
			Create: &types.CreateGlobalSecondaryIndexAction{
				IndexName: aws.String("gsi"),
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(0),
					WriteCapacityUnits: aws.Int64(0),
				},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	update, err := (&alternatorCreateViewsWorker{}).viewToCreateStmt(View{Type: AlternatorGlobalSecondaryIndex, CreateStmt: string(stmt)})
	if err != nil {
		t.Fatal(err)
	}
	if pt := update.GlobalSecondaryIndexUpdates[0].Create.ProvisionedThroughput; pt != nil {
		t.Fatalf("expected nil throughput, got %+v", pt)
	}
}
