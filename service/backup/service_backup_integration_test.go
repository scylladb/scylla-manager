// Copyright (C) 2017 ScyllaDB

// +build all integration

package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
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

		clusterID: uuid.MustRandom(),
		taskID:    uuid.MustRandom(),
		runID:     uuid.NewTime(),

		t: t,
	}
}

func newTestClient(t *testing.T, logger log.Logger) *scyllaclient.Client {
	t.Helper()

	c, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts, AgentAuthToken()), logger.Named("scylla"))
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
	// Clear keyspaces
	CreateManagedClusterSession(t)

	table := []struct {
		Name   string
		Input  string
		Golden string
	}{
		{
			Name:   "everything",
			Input:  "testdata/get_target/everything.input.json",
			Golden: "testdata/get_target/everything.golden.json",
		},
		{
			Name:   "filter keyspaces",
			Input:  "testdata/get_target/filter_keyspaces.input.json",
			Golden: "testdata/get_target/filter_keyspaces.golden.json",
		},
		{
			Name:   "filter dc",
			Input:  "testdata/get_target/filter_dc.input.json",
			Golden: "testdata/get_target/filter_dc.golden.json",
		},
		{
			Name:   "dc locations",
			Input:  "testdata/get_target/dc_locations.input.json",
			Golden: "testdata/get_target/dc_locations.golden.json",
		},
		{
			Name:   "dc rate limit",
			Input:  "testdata/get_target/dc_rate_limit.input.json",
			Golden: "testdata/get_target/dc_rate_limit.golden.json",
		},
		{
			Name:   "dc snapshot parallel",
			Input:  "testdata/get_target/dc_snapshot_parallel.input.json",
			Golden: "testdata/get_target/dc_snapshot_parallel.golden.json",
		},
		{
			Name:   "dc upload parallel",
			Input:  "testdata/get_target/dc_upload_parallel.input.json",
			Golden: "testdata/get_target/dc_upload_parallel.golden.json",
		},
		{
			Name:   "continue",
			Input:  "testdata/get_target/continue.input.json",
			Golden: "testdata/get_target/continue.golden.json",
		},
	}

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newBackupTestHelper(t, session, backup.DefaultConfig())
		ctx     = context.Background()
	)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			b, err := ioutil.ReadFile(test.Input)
			if err != nil {
				t.Fatal(err)
			}
			v, err := h.service.GetTarget(ctx, h.clusterID, b, false)

			if UpdateGoldenFiles() {
				b, _ := json.Marshal(v)
				var buf bytes.Buffer
				json.Indent(&buf, b, "", "  ")
				if err := ioutil.WriteFile(test.Golden, buf.Bytes(), 0666); err != nil {
					t.Error(err)
				}
			}

			b, err = ioutil.ReadFile(test.Golden)
			if err != nil {
				t.Fatal(err)
			}
			var golden backup.Target
			if err := json.Unmarshal(b, &golden); err != nil {
				t.Error(err)
			}

			if diff := cmp.Diff(v, golden); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestServiceGetTargetErrorIntegration(t *testing.T) {
	// Clear keyspaces
	CreateManagedClusterSession(t)

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
		{
			Name:   "invalid snapshot parallel dc",
			JSON:   `{"snapshot_parallel": ["foobar:100"], "location": ["s3:foo"]}`,
			Errror: "invalid snapshot-parallel: no such datacenter",
		},
		{
			Name:   "invalid upload parallel dc",
			JSON:   `{"upload_parallel": ["foobar:100"], "location": ["s3:foo"]}`,
			Errror: "invalid upload-parallel: no such datacenter",
		},
	}

	var (
		session = CreateSessionWithoutMigration(t)
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

func writeAndFlushData(t *testing.T, session *gocql.Session, keyspace string, size int) {
	ExecStmt(t, session, "CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
	ExecStmt(t, session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.big_table (id int PRIMARY KEY, data blob)", keyspace))

	cql := fmt.Sprintf("INSERT INTO %s.big_table (id, data) VALUES (?, ?)", keyspace)
	data := make([]byte, size)
	rand.Read(data)
	err := session.Query(cql, 1, data).Exec()
	if err != nil {
		t.Fatal(err)
	}

	flushData(t)
}

func restartAgents(t *testing.T) {
	execOnAllHosts(t, "supervisorctl restart scylla-manager-agent")
}

func flushData(t *testing.T) {
	execOnAllHosts(t, "nodetool flush")
}

func execOnAllHosts(t *testing.T, cmd string) {
	for _, host := range ManagedClusterHosts {
		stdout, stderr, err := ExecOnHost(host, cmd)
		if err != nil {
			t.Log("stdout", stdout)
			t.Log("stderr", stderr)
			t.Fatal("Command failed on host", host, err)
		}
	}
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

func TestBackupSmokeIntegration(t *testing.T) {
	const testBucket = "backuptest-smoke"

	S3InitBucket(t, testBucket)

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
				Path:     testBucket,
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

var runnerTimeout = 10 * time.Second

// Tests resuming a stopped backup.
func TestBackupResumeIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-resume"
		testKeyspace = "backuptest_resume"
	)

	S3InitBucket(t, testBucket)

	config := backup.DefaultConfig()
	config.TestS3Endpoint = S3TestEndpoint()
	config.PollInterval = time.Second

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSession(t)
	)

	writeAndFlushData(t, clusterSession, testKeyspace, 3*1024*1024)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC: []string{"dc1"},
		Location: []backup.Location{
			{
				Provider: backup.S3,
				Path:     testBucket,
			},
		},
		Retention: 1,
		RateLimit: []backup.DCLimit{
			{"dc1", 1},
		},
		Continue: true,
	}

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
			err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target)
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
		err := h.service.Backup(context.Background(), h.clusterID, h.taskID, resumeID, target)
		if err != nil {
			t.Error("Didn't expect error", err)
		}

		Print("Then: data is uploaded")
		d, err := h.client.RcloneListDir(context.Background(), ManagedClusterHosts[0], fmt.Sprintf("%s:%s", backup.S3, testBucket), true)
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
			err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target)
			if err == nil {
				t.Error("Expected error on run but got nil")
			}
			close(done)
		}()

		waitForTransfersToStart(t, h)

		Print("And: we restart the agents")
		restartAgents(t)

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
		err := h.service.Backup(context.Background(), h.clusterID, h.taskID, resumeID, target)
		if err != nil {
			t.Error("Didn't expect error", err)
		}

		Print("Then: data is uploaded")
		d, err := h.client.RcloneListDir(context.Background(), ManagedClusterHosts[0], fmt.Sprintf("%s:%s", backup.S3, testBucket), true)
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

		target := target
		target.Continue = false

		go func() {
			defer close(done)
			Print("When: runner is running")
			err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target)
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
		err := h.service.Backup(context.Background(), h.clusterID, h.taskID, resumeID, target)
		if err != nil {
			t.Error("Didn't expect error", err)
		}

		Print("Then: data is uploaded")
		d, err := h.client.RcloneListDir(context.Background(), ManagedClusterHosts[0], fmt.Sprintf("%s:%s", backup.S3, testBucket), true)
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

func TestPurgeIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-purge"
		testKeyspace = "backuptest_purge"
	)

	S3InitBucket(t, testBucket)

	config := backup.DefaultConfig()
	config.TestS3Endpoint = S3TestEndpoint()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSession(t)

		h   = newBackupTestHelper(t, session, config)
		ctx = context.Background()
	)

	defer func() {
		for _, ip := range ManagedClusterHosts {
			if err := h.client.RcloneStatsReset(context.Background(), ip, ""); err != nil {
				t.Error("Couldn't reset stats", ip, err)
			}
		}
	}()

	Print("Given: retention policy 1")

	location := backup.Location{
		Provider: backup.S3,
		Path:     testBucket,
	}

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []backup.Location{location},
		Retention: 1,
	}

	Print("When: run backup 3 times")
	var runID uuid.UUID
	for i := 0; i < 3; i++ {
		writeAndFlushData(t, clusterSession, testKeyspace, 3*1024*1024)
		runID = uuid.NewTime()
		if err := h.service.Backup(ctx, h.clusterID, h.taskID, runID, target); err != nil {
			t.Fatal(err)
		}
	}

	Print("Then: only the last task run is preserved")
	files, err := h.client.RcloneListDir(ctx, ManagedClusterHosts[0], location.RemotePath(""), true)
	if err != nil {
		t.Fatal(err)
	}
	var manifests []string
	for _, f := range files {
		if f.Name == "manifest.json" {
			if !strings.Contains(f.Path, h.taskID.String()) || !strings.Contains(f.Path, runID.String()) {
				t.Errorf("Manifest %s does not belong to task %s or run %s", f.Path, h.taskID, runID)
			}
			manifests = append(manifests, f.Path)
		}
	}
	if len(manifests) != 3 {
		t.Fatalf("Expected 3 manifests got %d", len(manifests))
	}

	Print("And: old sstable files are removed")
	var sstPfx []string
	for _, m := range manifests {
		b, err := h.client.RcloneCat(ctx, ManagedClusterHosts[0], location.RemotePath(m))
		if err != nil {
			t.Fatal(err)
		}
		var v struct {
			Files []string `json:"files"`
		}
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatal(err)
		}
		for _, f := range v.Files {
			sstPfx = append(sstPfx, strings.TrimSuffix(f, "-Data.db"))
		}
	}

	for _, f := range files {
		if !f.IsDir && f.Name != "manifest.json" {
			ok := false
			for _, pfx := range sstPfx {
				if strings.HasPrefix(f.Name, pfx) {
					ok = true
					break
				}
			}
			if !ok {
				t.Errorf("Unexpected file %s", f.Path)
			}
		}
	}
}
