// Copyright (C) 2021 ScyllaDB

//go:build all || integration
// +build all integration

package migrate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestRewriteHealthCheck30(t *testing.T) {
	type Task struct {
		ClusterID  uuid.UUID
		Type       string
		ID         uuid.UUID
		Name       string
		Properties json.RawMessage
	}

	healthCheckModeProperties := func(mode string) json.RawMessage {
		return json.RawMessage(`{"mode": "` + mode + `"}`)
	}

	clusterID := uuid.MustRandom()
	input := []*Task{
		{
			ClusterID: clusterID,
			Type:      "healthcheck",
			ID:        uuid.MustParse("6D77D539-1D10-4A40-98F2-4DE0865DE183"),
		},
		{
			ClusterID: clusterID,
			Type:      "healthcheck_rest",
			ID:        uuid.MustParse("8042889C-6226-4FBA-BEAD-13C768FC2FC8"),
		},
		{
			ClusterID: clusterID,
			Type:      "healthcheck_alternator",
			ID:        uuid.MustParse("95961BAC-BEF6-4B09-BD8C-95F05F12089F"),
		},
		{
			ClusterID: clusterID,
			Type:      "repair",
			ID:        uuid.MustParse("A3B85B67-7DA8-4A14-A964-2EE933434E61"),
		},
	}
	golden := []*Task{
		{
			ClusterID:  clusterID,
			Type:       "healthcheck",
			ID:         input[0].ID,
			Name:       "cql",
			Properties: healthCheckModeProperties("cql"),
		},
		{
			ClusterID:  clusterID,
			Type:       "healthcheck",
			ID:         input[1].ID,
			Name:       "rest",
			Properties: healthCheckModeProperties("rest"),
		},
		{
			ClusterID:  clusterID,
			Type:       "healthcheck",
			ID:         input[2].ID,
			Name:       "alternator",
			Properties: healthCheckModeProperties("alternator"),
		},
		{
			ClusterID: clusterID,
			Type:      "repair",
			ID:        input[3].ID,
		},
	}

	prepare := func(session gocqlx.Session) error {
		q := qb.Insert("scheduler_task").Columns("cluster_id", "type", "id").Query(session)
		for _, t := range input {
			if err := q.BindStruct(t).Exec(); err != nil {
				return err
			}
		}
		q.Release()
		return nil
	}
	validate := func(session gocqlx.Session) error {
		q := qb.Select("scheduler_task").Query(session)
		defer q.Release()

		var all []*Task
		if err := q.Iter().Unsafe().Select(&all); err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()
		for i, v := range all {
			Logger.Debug(ctx, "Tasks", "pos", i, "task", v)
		}

		if diff := cmp.Diff(all, golden, testutils.UUIDComparer()); diff != "" {
			t.Fatal(diff)
		}

		return nil
	}

	testCallback(t, migrate.CallComment, "rewriteHealthCheck30", prepare, validate)
}
