// Copyright (C) 2017 ScyllaDB

// +build all integration

package backup_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/mermaid"
	. "github.com/scylladb/mermaid/mermaidtest"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/service/backup"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap/zapcore"
)

type backupTestHelper struct {
	session *gocql.Session
	client  *scyllaclient.Client
	service *backup.Service
	runner  backup.Runner

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newBackupTestHelper(t *testing.T, session *gocql.Session, config backup.Config) *backupTestHelper {
	t.Helper()

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	client := newTestClient(t, logger.Named("client"))
	service := newTestService(t, session, client, config, logger)

	return &backupTestHelper{
		session: session,
		client:  client,
		service: service,
		runner:  service.Runner(),

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
		},
		DC:        []string{"dc1", "dc2"},
		Location:  []backup.Location{{Provider: "s3", Path: "foo"}},
		Retention: 3,
		Continue:  true,
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
		{
			Name: "continue setting",
			JSON: `{"continue": false, "location": ["s3:foo"]}`,
			Target: decorate(func(v *backup.Target) {
				v.Continue = false
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
	config.TestS3Endpoint = S3TestEndpoint()

	var (
		session = CreateSession(t)
		h       = newBackupTestHelper(t, session, config)
		ctx     = context.Background()
	)

	defer func() {
		for _, ip := range ManagedClusterHosts {
			if err := h.client.RcloneStatsReset(context.Background(), ip, ""); err != nil {
				t.Error("Couldn't reset stats", ip, err)
			}
		}
	}()

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
	if len(d) < 9 {
		t.Fatal("expected data")
	}
}

func TestServiceGetLastResumableRunIntegration(t *testing.T) {
	const bucket = "get_last_resumable"

	S3InitBucket(t, bucket)

	config := backup.DefaultConfig()
	config.TestS3Endpoint = S3TestEndpoint()
	config.PollInterval = time.Second

	var (
		session = CreateSession(t)
		h       = newBackupTestHelper(t, session, config)
		ctx     = context.Background()
	)

	putRun := func(t *testing.T, r *backup.Run) {
		t.Helper()
		stmt, names := schema.BackupRun.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(r).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}
	putRunProgress := func(t *testing.T, r *backup.Run, size, uploaded int64) {
		t.Helper()
		p := backup.RunProgress{
			ClusterID: r.ClusterID,
			TaskID:    r.TaskID,
			RunID:     r.ID,
			Host:      ManagedClusterHosts[0],
			Size:      size,
			Uploaded:  uploaded,
		}
		stmt, names := schema.BackupRunProgress.Insert()
		if err := gocqlx.Query(session.Query(stmt), names).BindStruct(&p).ExecRelease(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("no started runs", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test"}},
		}
		putRun(t, r0)

		r1 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test"}},
		}
		putRun(t, r1)

		_, err := h.service.GetLastResumableRun(ctx, clusterID, taskID)
		if err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})

	t.Run("started run before done run", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test"}},
		}
		putRun(t, r0)
		putRunProgress(t, r0, 10, 0)

		r1 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test"}},
		}
		putRun(t, r1)
		putRunProgress(t, r0, 10, 10)

		_, err := h.service.GetLastResumableRun(ctx, clusterID, taskID)
		if err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})

	t.Run("started run before not started run", func(t *testing.T) {
		t.Parallel()

		clusterID := uuid.MustRandom()
		taskID := uuid.MustRandom()

		r0 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test1"}},
		}
		putRun(t, r0)
		putRunProgress(t, r0, 10, 0)

		r1 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test2"}},
		}
		putRun(t, r1)

		r, err := h.service.GetLastResumableRun(ctx, clusterID, taskID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(r, r0, UUIDComparer(), cmp.AllowUnexported(backup.Run{}), cmp.AllowUnexported(backup.Unit{})); diff != "" {
			t.Fatal(diff)
		}
	})
}

var runnerTimeout = 10 * time.Second

// Tests resuming stopped backup.
func TestRunnerResumeIntegration(t *testing.T) {
	const bucket = "runnertest"
	const ks_name = "backup_runner"

	S3InitBucket(t, bucket)

	config := backup.DefaultConfig()
	config.TestS3Endpoint = S3TestEndpoint()
	config.PollInterval = time.Second

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSession(t)
		dropKeyspace   = createBigTable(t, clusterSession, ks_name, 3*1024*1024)
	)

	defer dropKeyspace()

	properties := json.RawMessage(fmt.Sprintf(`{
		"keyspace": [
			"%s"
		],
		"dc": ["dc1"],
		"location": ["%s:%s"],
		"retention": 1,
		"rate_limit": ["dc1:1"],
		"continue": true
	}`, ks_name, backup.S3, bucket))

	t.Run("resume after stop", func(t *testing.T) {
		h := newBackupTestHelper(t, session, config)
		defer func() {
			for _, ip := range ManagedClusterHosts {
				if err := h.client.RcloneStatsReset(context.Background(), ip, ""); err != nil {
					t.Error("Couldn't reset stats", ip, err)
				}
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			Print("When: runner is running")
			err := h.runner.Run(ctx, h.clusterID, h.taskID, h.runID, properties)
			if err == nil {
				t.Error("Expected error on run but got nil")
			} else {
				if !strings.Contains(err.Error(), "context") {
					t.Errorf("Expected context error but got: %+v", err)
				}
			}
		}()

		waitForTransfersToStart(t, h)

		Print("And: context is canceled")
		cancel()
		<-ctx.Done()

		select {
		case <-time.After(runnerTimeout):
			t.Fatalf("Runner didn't complete in under %s", runnerTimeout)
		case <-done:
			Print("Then: runner completed execution")
		}

		Print("and: nothing is transferring")
		if !nothingIsRunning(t, h) {
			t.Fatal("There are still transfers underway on the hosts")
		}

		Print("When: runner is resumed with new RunID")
		resumeID := uuid.NewTime()
		err := h.runner.Run(context.Background(), h.clusterID, h.taskID, resumeID, properties)
		if err != nil {
			t.Error("Didn't expect error", err)
		}

		Print("Then: data is uploaded")
		d, err := h.client.RcloneListDir(context.Background(), ManagedClusterHosts[0], fmt.Sprintf("%s:%s", backup.S3, bucket), true)
		if err != nil {
			t.Fatal(err)
		}
		if len(d) < 9 {
			t.Fatal("expected data")
		}

		Print("And: directory with new runID is not created")
		for i := range d {
			if d[i].Name == resumeID.String() {
				t.Errorf("Run is not resumed, new structure on remote: %+v", d[i])
			}
		}

		Print("And: nothing is transferring")
		if !nothingIsRunning(t, h) {
			t.Fatal("There are still transfers on the hosts")
		}
	})

	t.Run("resume after agent restart", func(t *testing.T) {
		h := newBackupTestHelper(t, session, config)
		defer func() {
			for _, ip := range ManagedClusterHosts {
				if err := h.client.RcloneStatsReset(context.Background(), ip, ""); err != nil {
					t.Error("Couldn't reset stats", ip, err)
				}
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			Print("When: runner is running")
			err := h.runner.Run(ctx, h.clusterID, h.taskID, h.runID, properties)
			if err == nil {
				t.Error("Expected error on run but got nil")
			}
			close(done)
		}()

		waitForTransfersToStart(t, h)

		Print("And: we restart the agents")
		restartAllAgents(t)

		select {
		case <-time.After(runnerTimeout * 3):
			t.Fatalf("Runner didn't complete in under %s", runnerTimeout*3)
		case <-done:
			Print("Then: runner completed execution")
		}

		Print("and: nothing is transferring")
		if !nothingIsRunning(t, h) {
			t.Fatal("There are still transfers underway on the hosts")
		}

		Print("When: runner is resumed with new RunID")
		resumeID := uuid.NewTime()
		err := h.runner.Run(context.Background(), h.clusterID, h.taskID, resumeID, properties)
		if err != nil {
			t.Error("Didn't expect error", err)
		}

		Print("Then: data is uploaded")
		d, err := h.client.RcloneListDir(context.Background(), ManagedClusterHosts[0], fmt.Sprintf("%s:%s", backup.S3, bucket), true)
		if err != nil {
			t.Fatal(err)
		}
		if len(d) < 9 {
			t.Fatal("expected data")
		}

		Print("And: directory with new runID is not created")
		for i := range d {
			if d[i].Name == resumeID.String() {
				t.Errorf("Run is not resumed, new structure on remote: %+v", d[i])
			}
		}

		Print("And: nothing is transferring")
		if !nothingIsRunning(t, h) {
			t.Fatal("There are still transfers on the hosts")
		}
	})

	t.Run("continue false", func(t *testing.T) {
		h := newBackupTestHelper(t, session, config)
		defer func() {
			for _, ip := range ManagedClusterHosts {
				if err := h.client.RcloneStatsReset(context.Background(), ip, ""); err != nil {
					t.Error("Couldn't reset stats", ip, err)
				}
			}
		}()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		properties := json.RawMessage(fmt.Sprintf(`{
			"keyspace": [
				"%s"
			],
			"dc": ["dc1"],
			"location": ["%s:%s"],
			"retention": 1,
			"rate_limit": ["dc1:1"],
			"continue": false
		}`, ks_name, backup.S3, bucket))

		go func() {
			defer close(done)
			Print("When: runner is running")
			err := h.runner.Run(ctx, h.clusterID, h.taskID, h.runID, properties)
			if err == nil {
				t.Error("Expected error on run but got nil")
			} else {
				if !strings.Contains(err.Error(), "context") {
					t.Errorf("Expected context error but got: %+v", err)
				}
			}
		}()

		waitForTransfersToStart(t, h)

		Print("And: context is canceled")
		cancel()
		<-ctx.Done()

		select {
		case <-time.After(runnerTimeout):
			t.Fatalf("Runner didn't complete in under %s", runnerTimeout)
		case <-done:
			Print("Then: runner completed execution")
		}

		Print("and: nothing is transferring")
		if !nothingIsRunning(t, h) {
			t.Fatal("There are still transfers underway on the hosts")
		}

		Print("When: runner is resumed with new RunID")
		resumeID := uuid.NewTime()
		err := h.runner.Run(context.Background(), h.clusterID, h.taskID, resumeID, properties)
		if err != nil {
			t.Error("Didn't expect error", err)
		}

		Print("Then: data is uploaded")
		d, err := h.client.RcloneListDir(context.Background(), ManagedClusterHosts[0], fmt.Sprintf("%s:%s", backup.S3, bucket), true)
		if err != nil {
			t.Fatal(err)
		}
		if len(d) < 9 {
			t.Fatal("expected data")
		}

		Print("And: directory with new runID is created")
		var created bool
		for i := range d {
			if d[i].Name == resumeID.String() {
				created = true
			}
		}
		if !created {
			t.Error("Run is resumed instead of starting new")
		}

		Print("And: nothing is transferring")
		if !nothingIsRunning(t, h) {
			t.Fatal("There are still transfers on the hosts")
		}
	})
}

// Check that any transfer have started to confirm backup actually
// got to the uploading.
func waitForTransfersToStart(t *testing.T, h *backupTestHelper) {
	t.Helper()
	WaitCond(t, func() bool {
		for _, host := range ManagedClusterHosts {
			s, err := h.client.RcloneStats(context.Background(), host, "")
			if err != nil {
				t.Fatal(err)
			}
			for _, tr := range s.Transferring {
				if tr.Name != "manifest.json" {
					Print("And: upload is underway")
					return true
				}
			}
		}
		return false
	}, 100*time.Millisecond, runnerTimeout)
}

// Returns true if no transfers are running on the hosts.
func nothingIsRunning(t *testing.T, h *backupTestHelper) bool {
	res := false
	for i := 0; i < 5; i++ {
		count := 0
		for _, host := range ManagedClusterHosts {
			s, err := h.client.RcloneStats(context.Background(), host, "")
			if err != nil {
				t.Fatal(err)
			}
			if len(s.Transferring) == 0 {
				count++
			}
		}
		if count == len(ManagedClusterHosts) {
			res = true
			break
		}
	}
	return res
}

func restartAllAgents(t *testing.T) {
	t.Helper()
	res := make(chan error)
	for _, host := range ManagedClusterHosts {
		go func(host string) {
			_, _, err := ExecOnHost(host, `supervisorctl restart scylla-manager-agent`)
			res <- err
		}(host)
	}
	for i := 0; i < len(ManagedClusterHosts); i++ {
		select {
		case <-time.After(5 * time.Second):
			t.Error("Expected agent to be restarted in 5s")
			return
		case err := <-res:
			if err != nil {
				t.Error(err)
			}
		}
	}
}

func createBigTable(t *testing.T, session *gocql.Session, ks string, size int) func() {
	createKeyspace(t, session, ks)
	ExecStmt(t, session, fmt.Sprintf("CREATE TABLE %s.big_table (id int PRIMARY KEY, data blob)", ks))

	query := fmt.Sprintf("INSERT INTO %s.big_table (id, data) VALUES (?, ?)", ks)
	data := make([]byte, size)
	rand.Read(data)
	err := session.Query(query, 1, data).Exec()
	if err != nil {
		t.Fatal(err)
	}

	return func() {
		dropKeyspace(t, session, ks)
	}
}

func createKeyspace(t *testing.T, session *gocql.Session, keyspace string) {
	ExecStmt(t, session, "CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
}

func dropKeyspace(t *testing.T, session *gocql.Session, keyspace string) {
	ExecStmt(t, session, "DROP KEYSPACE "+keyspace)
}
