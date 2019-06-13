// Copyright (C) 2017 ScyllaDB

// +build all integration

package backup_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/service/backup"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap/zapcore"
)

type backupTestHelper struct {
	session *gocql.Session
	client  *scyllaclient.Client
	service *backup.Service

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newBackupTestHelper(t *testing.T, session *gocql.Session, config backup.Config) *backupTestHelper {
	t.Helper()

	client := newTestClient(t, log.Logger{})
	service := newTestService(t, session, client, config, log.NewDevelopmentWithLevel(zapcore.DebugLevel))

	return &backupTestHelper{
		session: session,
		client:  client,
		service: service,

		clusterID: uuid.MustRandom(),
		taskID:    uuid.MustRandom(),
		runID:     uuid.NewTime(),

		t: t,
	}
}

func newTestClient(t *testing.T, logger log.Logger) *scyllaclient.Client {
	t.Helper()

	c, err := scyllaclient.NewClient(scyllaclient.DefaultConfigWithHosts(ManagedClusterHosts), logger.Named("scylla"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newTestService(t *testing.T, session *gocql.Session, client *scyllaclient.Client, c backup.Config, logger log.Logger) *backup.Service {
	t.Helper()

	s, err := backup.NewService(
		session,
		c,
		func(_ context.Context, id uuid.UUID) (string, error) {
			return "test_cluster", nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		logger.Named("backup"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestServiceGetTargetIntegration(t *testing.T) {
	golden := backup.Target{
		Units: []backup.Unit{
			{Keyspace: "system_auth"},
			{Keyspace: "system_distributed"},
			{Keyspace: "system_traces"},
			{Keyspace: "test_keyspace_dc1_rf2"},
			{Keyspace: "test_keyspace_dc1_rf3"},
			{Keyspace: "test_keyspace_dc2_rf2"},
			{Keyspace: "test_keyspace_dc2_rf3"},
			{Keyspace: "test_keyspace_rf2"},
			{Keyspace: "test_keyspace_rf3"},
		},
		DC:        []string{"dc1", "dc2"},
		Location:  []backup.Location{{Provider: "s3", Path: "foo"}},
		Retention: 3,
	}

	decorate := func(f func(*backup.Target)) backup.Target {
		v := golden
		f(&v)
		return v
	}

	table := []struct {
		Name   string
		JSON   string
		Target backup.Target
	}{
		{
			Name:   "everything",
			JSON:   `{"location": ["s3:foo"]}`,
			Target: decorate(func(v *backup.Target) {}),
		},
		{
			Name: "filter keyspaces",
			JSON: `{"keyspace": ["system_auth.*"], "location": ["s3:foo"]}`,
			Target: decorate(func(v *backup.Target) {
				v.Units = []backup.Unit{{Keyspace: "system_auth"}}
			}),
		},
		{
			Name: "filter dc",
			JSON: `{"dc": ["dc1"], "location": ["s3:foo"]}`,
			Target: decorate(func(v *backup.Target) {
				v.DC = []string{"dc1"}
			}),
		},
		{
			Name: "dc locations",
			JSON: `{"location": ["s3:foo", "dc1:s3:bar"]}`,
			Target: decorate(func(v *backup.Target) {
				v.Location = []backup.Location{{Provider: "s3", Path: "foo"}, {DC: "dc1", Provider: "s3", Path: "bar"}}
			}),
		},
		{
			Name: "dc rate limit",
			JSON: `{"rate_limit": ["1000", "dc1:100"], "location": ["s3:foo"]}`,
			Target: decorate(func(v *backup.Target) {
				v.RateLimit = []backup.RateLimit{{Limit: 1000}, {DC: "dc1", Limit: 100}}
			}),
		},
	}

	var (
		session = CreateSession(t)
		h       = newBackupTestHelper(t, session, backup.DefaultConfig())
		ctx     = context.Background()
	)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			golden := test.Target
			v, err := h.service.GetTarget(ctx, h.clusterID, json.RawMessage(test.JSON), false)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(v, golden); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestServiceGetTargetErrorIntegration(t *testing.T) {
	table := []struct {
		Name   string
		JSON   string
		Errror string
	}{
		{
			Name:   "empty",
			JSON:   `{}`,
			Errror: "missing location",
		},
		{
			Name:   "invalid keyspace filter",
			JSON:   `{"keyspace": ["foobar"], "location": ["s3:foo"]}`,
			Errror: "no matching keyspaces found",
		},
		{
			Name:   "invalid dc filter",
			JSON:   `{"dc": ["foobar"], "location": ["s3:foo"]}`,
			Errror: "no matching DCs found",
		},
		{
			Name:   "invalid location dc",
			JSON:   `{"location": ["foobar:s3:foo"]}`,
			Errror: "invalid location: no such datacenter",
		},
		{
			Name:   "no location for dc",
			JSON:   `{"location": ["dc1:s3:foo"]}`,
			Errror: "invalid location: missing configurations for datacenters",
		},
		{
			Name:   "invalid rate limit dc",
			JSON:   `{"rate_limit": ["foobar:100"], "location": ["s3:foo"]}`,
			Errror: "invalid rate-limit: no such datacenter",
		},
	}

	var (
		session = CreateSession(t)
		h       = newBackupTestHelper(t, session, backup.DefaultConfig())
		ctx     = context.Background()
	)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			_, err := h.service.GetTarget(ctx, h.clusterID, json.RawMessage(test.JSON), false)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), test.Errror) {
				t.Fatalf("expected %s got %s", test.Errror, err)
			}
		})
	}
}

func TestBackupIntegration(t *testing.T) {
	const bucket = "backuptest"

	S3InitBucket(t, bucket)

	config := backup.DefaultConfig()
	config.TestEndpoint = S3TestEndpoint()

	var (
		session = CreateSession(t)
		h       = newBackupTestHelper(t, session, config)
		ctx     = context.Background()
	)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: "system_auth",
			},
		},
		DC: []string{"dc1"},
		Location: []backup.Location{
			{
				Provider: backup.S3,
				Path:     bucket,
			},
		},
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}
	Print("Then: data is uploaded")
	d, err := h.client.RcloneListDir(ctx, ManagedClusterHosts[0], target.Location[0].RemotePath(""), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(d) == 0 {
		t.Fatal("expected data")
	}
}
