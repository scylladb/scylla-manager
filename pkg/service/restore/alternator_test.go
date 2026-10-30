// Copyright (C) 2026 ScyllaDB

package restore

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
)

func TestProvisionedThroughputDescToCreate(t *testing.T) {
	desc := func(rcu, wcu int64) *types.ProvisionedThroughputDescription {
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
		name        string
		desc        *types.ProvisionedThroughputDescription
		billingMode types.BillingMode
		expected    *types.ProvisionedThroughput
	}{
		{name: "pay per request, zero", billingMode: types.BillingModePayPerRequest, desc: desc(0, 0)},
		{name: "pay per request, positive", billingMode: types.BillingModePayPerRequest, desc: desc(3, 4)},
		{name: "pay per request, nil", billingMode: types.BillingModePayPerRequest},
		{name: "no billing mode", desc: desc(3, 4)},
		{name: "provisioned", billingMode: types.BillingModeProvisioned, desc: desc(3, 4), expected: throughput(3, 4)},
		{name: "provisioned, zero", billingMode: types.BillingModeProvisioned, desc: desc(0, 0), expected: throughput(1, 1)},
		{name: "provisioned, partial", billingMode: types.BillingModeProvisioned, desc: desc(3, 0), expected: throughput(3, 1)},
		{name: "provisioned, nil", billingMode: types.BillingModeProvisioned, expected: throughput(1, 1)},
		{name: "provisioned, nil values", billingMode: types.BillingModeProvisioned, desc: &types.ProvisionedThroughputDescription{}, expected: throughput(1, 1)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := alternatorWorker{}.provisionedThroughputDescToCreate(tc.desc, tc.billingMode)
			if diff := cmp.Diff(tc.expected, got, cmp.AllowUnexported(types.ProvisionedThroughput{})); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestAlternatorThroughputWiring(t *testing.T) {
	describe := func(name string, mode types.BillingMode, tableRCU, tableWCU, gsiRCU, gsiWCU int64) *types.TableDescription {
		return &types.TableDescription{
			TableName:          aws.String(name),
			BillingModeSummary: &types.BillingModeSummary{BillingMode: mode},
			ProvisionedThroughput: &types.ProvisionedThroughputDescription{
				ReadCapacityUnits:  aws.Int64(tableRCU),
				WriteCapacityUnits: aws.Int64(tableWCU),
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{{
				IndexName: aws.String("gsi"),
				ProvisionedThroughput: &types.ProvisionedThroughputDescription{
					ReadCapacityUnits:  aws.Int64(gsiRCU),
					WriteCapacityUnits: aws.Int64(gsiWCU),
				},
			}},
		}
	}
	throughput := func(rcu, wcu int64) *types.ProvisionedThroughput {
		return &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(rcu),
			WriteCapacityUnits: aws.Int64(wcu),
		}
	}

	testCases := []struct {
		name          string
		desc          *types.TableDescription
		expectedTable *types.ProvisionedThroughput
		expectedGSI   *types.ProvisionedThroughput
	}{
		{
			name:          "provisioned",
			desc:          describe("prov", types.BillingModeProvisioned, 5, 7, 3, 4),
			expectedTable: throughput(5, 7),
			expectedGSI:   throughput(3, 4),
		},
		{
			name:          "provisioned, zero",
			desc:          describe("prov_zero", types.BillingModeProvisioned, 0, 0, 0, 0),
			expectedTable: throughput(1, 1),
			expectedGSI:   throughput(1, 1),
		},
		{
			name: "pay per request",
			desc: describe("ppr", types.BillingModePayPerRequest, 0, 0, 0, 0),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema := backupspec.AlternatorTableSchema{Describe: tc.desc}
			opt := cmp.AllowUnexported(types.ProvisionedThroughput{})

			create := (&alternatorSchemaWorker{}).tableSchemaToCreate(schema)
			if diff := cmp.Diff(tc.expectedTable, create.ProvisionedThroughput, opt); diff != "" {
				t.Fatal("table:", diff)
			}
			if diff := cmp.Diff(tc.expectedGSI, create.GlobalSecondaryIndexes[0].ProvisionedThroughput, opt); diff != "" {
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
			if diff := cmp.Diff(tc.expectedGSI, update.GlobalSecondaryIndexUpdates[0].Create.ProvisionedThroughput, opt); diff != "" {
				t.Fatal("update table gsi:", diff)
			}
		})
	}
}
