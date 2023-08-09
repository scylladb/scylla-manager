// Copyright (C) 2017 ScyllaDB

package backup

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestDCLimitMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name    string
		DCLimit DCLimit
	}{
		{
			Name: "with dc",
			DCLimit: DCLimit{
				DC:    "dc",
				Limit: 100,
			},
		},
		{
			Name: "without dc",
			DCLimit: DCLimit{
				Limit: 100,
			},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			golden := test.DCLimit
			b, err := golden.MarshalText()
			if err != nil {
				t.Error(golden, err)
			}
			var r DCLimit
			if err := r.UnmarshalText(b); err != nil {
				t.Error(err)
			}
			if golden != r {
				t.Errorf("Got %s, expected %s", r, golden)
			}
		})
	}
}

func TestExtractLocations(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		Json     string
		Location []Location
	}{
		{
			Name: "Empty",
			Json: "{}",
		},
		{
			Name: "Invalid properties",
			Json: "",
		},
		{
			Name: "Duplicates",
			Json: `{"location": ["dc:s3:foo", "s3:foo", "s3:bar"]}`,
			Location: []Location{
				{DC: "dc", Provider: S3, Path: "foo"},
				{Provider: S3, Path: "bar"},
			},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			l, err := extractLocations([]json.RawMessage{[]byte(test.Json)})
			if err != nil {
				t.Log("extractLocations() error", err)
			}
			if diff := cmp.Diff(l, test.Location); diff != "" {
				t.Errorf("extractLocations() = %s, expected %s", l, test.Location)
			}
		})
	}
}

func TestListItems(t *testing.T) {
	t.Parallel()

	t.Run("GetOrAppend", func(t *testing.T) {
		t.Parallel()
		var items [][2]uuid.UUID
		l := ListItems{}
		for x := 1; x < 10; x++ {
			clusterID1, err := uuid.NewRandom()
			if err != nil {
				t.Fatal(err)
			}
			taskID1, err := uuid.NewRandom()
			if err != nil {
				t.Fatal(err)
			}
			items = append(items, [2]uuid.UUID{clusterID1, taskID1})
			l.GetOrAppend(clusterID1, taskID1)
			if len(l) != x {
				t.Fatalf("list len %d != %d", len(l), x)
			}
		}

	outer:
		for _, item := range items {
			for _, info := range l {
				if info.ClusterID == item[0] && info.TaskID == item[1] {
					continue outer
				}
			}
			t.Fatalf("can't find record with clusterID=%s and taskID=%s in the list", item[0].String(), item[1].String())
		}
	})
}

func TestSnapshotInfoList(t *testing.T) {
	t.Parallel()

	t.Run("GetOrAppend", func(t *testing.T) {
		t.Parallel()
		l := SnapshotInfoList{}
		var snapshots []string
		for x := 1; x < 10; x++ {
			snapshotInfo, err := uuid.NewRandom()
			if err != nil {
				t.Fatal(err)
			}
			snapshots = append(snapshots, snapshotInfo.String())
			l.GetOrAppend(snapshotInfo.String())
			if len(l) != x {
				t.Fatalf("list len %d != %d", len(l), x)
			}
		}
	outer:
		for _, snapshot := range snapshots {
			for _, info := range l {
				if info.SnapshotTag == snapshot {
					continue outer
				}
			}
			t.Fatalf("can't find snapshot %s in the list", snapshot)
		}
	})
}
