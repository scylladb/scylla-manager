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
	"path"
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
	session  *gocql.Session
	client   *scyllaclient.Client
	service  *backup.Service
	location backup.Location

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newBackupTestHelper(t *testing.T, session *gocql.Session, config backup.Config, location backup.Location) *backupTestHelper {
	t.Helper()

	S3InitBucket(t, location.Path)

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	client := newTestClient(t, logger.Named("client"))
	service := newTestService(t, session, client, config, logger)

	for _, ip := range ManagedClusterHosts() {
		if err := client.RcloneStatsReset(context.Background(), ip, ""); err != nil {
			t.Error("Couldn't reset stats", ip, err)
		}
	}

	return &backupTestHelper{
		session:  session,
		client:   client,
		service:  service,
		location: location,

		clusterID: uuid.MustRandom(),
		taskID:    uuid.MustRandom(),
		runID:     uuid.NewTime(),

		t: t,
	}
}

func newTestClient(t *testing.T, logger log.Logger) *scyllaclient.Client {
	t.Helper()

	c, err := scyllaclient.NewClient(scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken()), logger.Named("scylla"))
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

func (h *backupTestHelper) listFiles() (manifests, files []string) {
	h.t.Helper()
	opts := &scyllaclient.RcloneListDirOpts{
		Recurse:   true,
		FilesOnly: true,
	}
	allFiles, err := h.client.RcloneListDir(context.Background(), ManagedClusterHost(), h.location.RemotePath(""), opts)
	if err != nil {
		h.t.Fatal(err)
	}
	for _, f := range allFiles {
		if strings.HasSuffix(f.Name, "manifest.json") {
			manifests = append(manifests, f.Path)
		} else {
			files = append(files, f.Path)
		}
	}
	return
}

func writeAndFlushData(t *testing.T, session *gocql.Session, keyspace string, size int) {
	t.Helper()

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

func flushData(t *testing.T) {
	execOnAllHosts(t, "nodetool flush")
}

func restartAgents(t *testing.T) {
	execOnAllHosts(t, "supervisorctl restart scylla-manager-agent")
}

func execOnAllHosts(t *testing.T, cmd string) {
	t.Helper()
	for _, host := range ManagedClusterHosts() {
		stdout, stderr, err := ExecOnHost(host, cmd)
		if err != nil {
			t.Log("stdout", stdout)
			t.Log("stderr", stderr)
			t.Fatal("Command failed on host", host, err)
		}
	}
}

const (
	maxWaitCond       = 5 * time.Second
	condCheckInterval = 100 * time.Millisecond
)

func (h *backupTestHelper) waitCond(f func() bool) {
	WaitCond(h.t, f, condCheckInterval, maxWaitCond)
}

func (h *backupTestHelper) waitTransfersStarted() {
	h.waitCond(func() bool {
		h.t.Helper()
		for _, host := range ManagedClusterHosts() {
			s, err := h.client.RcloneStats(context.Background(), host, "")
			if err != nil {
				h.t.Fatal(err)
			}
			for _, tr := range s.Transferring {
				if tr.Name != "manifest.json" {
					Print("And: upload is underway")
					return true
				}
			}
		}
		return false
	})
}

func (h *backupTestHelper) waitManifestUploaded() {
	h.waitCond(func() bool {
		h.t.Helper()
		m, _ := h.listFiles()
		return len(m) > 0
	})
}

func (h *backupTestHelper) waitNoTransfers() {
	h.waitCond(func() bool {
		h.t.Helper()
		for _, host := range ManagedClusterHosts() {
			s, err := h.client.RcloneStats(context.Background(), host, "")
			if err != nil {
				h.t.Fatal(err)
			}
			if len(s.Transferring) > 0 {
				return false
			}
		}
		return true
	})
}

func s3Location(bucket string) backup.Location {
	return backup.Location{
		Provider: backup.S3,
		Path:     bucket,
	}
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

	const testBucket = "backuptest-get-target"

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newBackupTestHelper(t, session, backup.DefaultConfig(), s3Location(testBucket))
		ctx     = context.Background()
	)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			b, err := ioutil.ReadFile(test.Input)
			if err != nil {
				t.Fatal(err)
			}
			v, err := h.service.GetTarget(ctx, h.clusterID, b, false)
			if err != nil {
				t.Fatal(err)
			}

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

			if diff := cmp.Diff(golden, v); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestServiceGetTargetErrorIntegration(t *testing.T) {
	// Clear keyspaces
	CreateManagedClusterSession(t)

	table := []struct {
		Name  string
		JSON  string
		Error string
	}{
		{
			Name:  "empty",
			JSON:  `{}`,
			Error: "missing location",
		},
		{
			Name:  "invalid keyspace filter",
			JSON:  `{"keyspace": ["foobar"], "location": ["s3:backuptest-get-target"]}`,
			Error: "no matching keyspaces found",
		},
		{
			Name:  "invalid dc filter",
			JSON:  `{"dc": ["foobar"], "location": ["s3:backuptest-get-target"]}`,
			Error: "no matching DCs found",
		},
		{
			Name:  "invalid location dc",
			JSON:  `{"location": ["foobar:s3:backuptest-get-target"]}`,
			Error: "invalid location: no such datacenter",
		},
		{
			Name:  "no location for dc",
			JSON:  `{"location": ["dc1:s3:backuptest-get-target"]}`,
			Error: "invalid location: missing location for datacenter",
		},
		{
			Name:  "no location dc filtered out",
			JSON:  `{"dc": ["dc2"], "location": ["dc1:s3:backuptest-get-target"]}`,
			Error: "invalid location: missing location for datacenter",
		},
		{
			Name:  "inaccessible location",
			JSON:  `{"location": ["s3:foo", "dc1:s3:bar"]}`,
			Error: "location not accessible",
		},
		{
			Name:  "invalid rate limit dc",
			JSON:  `{"rate_limit": ["foobar:100"], "location": ["s3:backuptest-get-target"]}`,
			Error: "invalid rate-limit: no such datacenter",
		},
		{
			Name:  "invalid snapshot parallel dc",
			JSON:  `{"snapshot_parallel": ["foobar:100"], "location": ["s3:backuptest-get-target"]}`,
			Error: "invalid snapshot-parallel: no such datacenter",
		},
		{
			Name:  "invalid upload parallel dc",
			JSON:  `{"upload_parallel": ["foobar:100"], "location": ["s3:backuptest-get-target"]}`,
			Error: "invalid upload-parallel: no such datacenter",
		},
	}

	const testBucket = "backupbackuptest-get-target"

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newBackupTestHelper(t, session, backup.DefaultConfig(), s3Location(testBucket))
		ctx     = context.Background()
	)

	S3InitBucket(t, testBucket)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			_, err := h.service.GetTarget(ctx, h.clusterID, json.RawMessage(test.JSON), false)
			if err == nil {
				t.Fatal("GetTarget() expected error")
			}

			t.Log("GetTarget() message:", err)
			if !strings.Contains(err.Error(), test.Error) {
				t.Fatalf("GetTarget() expected error message %q got %q", test.Error, err)
			}
		})
	}
}

func TestServiceGetLastResumableRunIntegration(t *testing.T) {
	const testBucket = "backuptest-void"

	config := backup.DefaultConfig()
	config.PollInterval = time.Second

	var (
		session = CreateSession(t)
		h       = newBackupTestHelper(t, session, config, s3Location(testBucket))
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
			Host:      ManagedClusterHost(),
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

func TestBackupSmokeIntegration(t *testing.T) {
	const testBucket = "backuptest-smoke"

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session = CreateSession(t)
		h       = newBackupTestHelper(t, session, config, location)
		ctx     = context.Background()
	)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: "system_auth",
			},
		},
		DC:        []string{"dc1"},
		Location:  []backup.Location{location},
		Retention: 3,
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}
	Print("And: run it again")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: there are two backups")
	items, err := h.service.List(ctx, h.clusterID, ManagedClusterHost(), []backup.Location{location}, backup.ListFilter{ClusterID: h.clusterID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("List() expected one item got %d %v", len(items), items)
	}
	i := items[0]
	if len(i.SnapshotTags) != 2 {
		t.Fatalf("List() expected two SnapshotTags %d %v", len(i.SnapshotTags), i)
	}
}

var backupTimeout = 10 * time.Second

// Tests resuming a stopped backup.
func TestBackupResumeIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-resume"
		testKeyspace = "backuptest_resume"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()
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
		DC:        []string{"dc1"},
		Location:  []backup.Location{location},
		Retention: 2,
		RateLimit: []backup.DCLimit{
			{"dc1", 1},
		},
		Continue: true,
	}

	t.Run("resume after stop", func(t *testing.T) {
		h := newBackupTestHelper(t, session, config, location)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		go func() {
			defer close(done)
			Print("When: backup is running")
			err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target)
			if err == nil {
				t.Error("Expected error on run but got nil")
			} else {
				if !strings.Contains(err.Error(), "context") {
					t.Errorf("Expected context error but got: %+v", err)
				}
			}
		}()

		h.waitTransfersStarted()

		Print("And: context is canceled")
		cancel()
		<-ctx.Done()

		select {
		case <-time.After(backupTimeout):
			t.Fatalf("Backup failed to complete in under %s", backupTimeout)
		case <-done:
			Print("Then: backup completed execution")
		}

		Print("And: nothing is transferring")
		h.waitNoTransfers()

		Print("When: backup is resumed with new RunID")
		err := h.service.Backup(context.Background(), h.clusterID, h.taskID, uuid.NewTime(), target)
		if err != nil {
			t.Error("Unexpected error", err)
		}

		Print("Then: data is uploaded")
		manifests, files := h.listFiles()
		if len(manifests) != 3 {
			t.Fatalf("Expected 3 manifests got %s", manifests)
		}
		if len(files) == 0 {
			t.Fatal("Expected data to be uploaded")
		}

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})

	t.Run("resume after agent restart", func(t *testing.T) {
		h := newBackupTestHelper(t, session, config, location)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() {
			Print("When: backup is running")
			err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target)
			if err == nil {
				t.Error("Expected error on run but got nil")
			}
			close(done)
		}()

		h.waitTransfersStarted()

		Print("And: we restart the agents")
		restartAgents(t)

		select {
		case <-time.After(backupTimeout * 3):
			t.Fatalf("Backup failed to complete in under %s", backupTimeout*3)
		case <-done:
			Print("Then: backup completed execution")
		}

		Print("And: nothing is transferring")
		h.waitNoTransfers()

		Print("When: backup is resumed with new RunID")
		err := h.service.Backup(context.Background(), h.clusterID, h.taskID, uuid.NewTime(), target)
		if err != nil {
			t.Error("Unexpected error", err)
		}

		Print("Then: data is uploaded")
		manifests, files := h.listFiles()
		if len(manifests) != 3 {
			t.Fatalf("Expected 3 manifests got %s", manifests)
		}
		if len(files) == 0 {
			t.Fatal("Expected data to be uploaded")
		}

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})

	t.Run("continue false", func(t *testing.T) {
		h := newBackupTestHelper(t, session, config, location)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		target := target
		target.Continue = false
		target.UploadParallel = []backup.DCLimit{{Limit: 1}} // Upload one by one

		go func() {
			defer close(done)
			Print("When: backup is running")
			err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target)
			if err == nil {
				t.Error("Expected error on run but got nil")
			} else {
				if !strings.Contains(err.Error(), "context") {
					t.Errorf("Expected context error but got: %+v", err)
				}
			}
		}()

		h.waitManifestUploaded()

		Print("And: context is canceled")
		cancel()
		<-ctx.Done()

		select {
		case <-time.After(backupTimeout):
			t.Fatalf("Backup failed to complete in under %s", backupTimeout)
		case <-done:
			Print("Then: backup completed execution")
		}

		Print("And: nothing is transferring")
		h.waitNoTransfers()

		Print("When: backup is resumed with new RunID")
		err := h.service.Backup(context.Background(), h.clusterID, h.taskID, uuid.NewTime(), target)
		if err != nil {
			t.Error("Unexpected error", err)
		}

		Print("Then: data is uploaded")
		manifests, files := h.listFiles()
		if len(manifests) <= 3 {
			t.Fatalf("Expected over 3 manifests got %s", manifests)
		}
		if len(files) == 0 {
			t.Fatal("Expected data to be uploaded")
		}

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})
}

func TestPurgeIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-purge"
		testKeyspace = "backuptest_purge"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSession(t)

		h   = newBackupTestHelper(t, session, config, location)
		ctx = context.Background()
	)

	Print("Given: retention policy 1")
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
	manifests, files := h.listFiles()
	for _, m := range manifests {
		if !strings.Contains(m, h.taskID.String()) {
			t.Errorf("Unexpected file %s manifest does not belong to task %s", m, h.taskID)
		}
	}
	if len(manifests) != 3 {
		t.Fatalf("Expected 3 manifests got %s", manifests)
	}

	Print("And: old sstable files are removed")
	var sstPfx []string
	for _, m := range manifests {
		b, err := h.client.RcloneCat(ctx, ManagedClusterHost(), location.RemotePath(m))
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
		ok := false
		for _, pfx := range sstPfx {
			if strings.HasPrefix(path.Base(f), pfx) {
				ok = true
				break
			}
		}
		if !ok {
			t.Errorf("Unexpected file %s", f)
		}
	}
}
