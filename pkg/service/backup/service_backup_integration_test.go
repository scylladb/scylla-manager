// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"
)

type backupTestHelper struct {
	session  gocqlx.Session
	hrt      *HackableRoundTripper
	client   *scyllaclient.Client
	service  *backup.Service
	location Location

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newBackupTestHelper(t *testing.T, session gocqlx.Session, config backup.Config, location Location, clientConf *scyllaclient.Config) *backupTestHelper {
	t.Helper()

	S3InitBucket(t, location.Path)

	clusterID := uuid.MustRandom()

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	client := newTestClient(t, hrt, logger.Named("client"), clientConf)
	service := newTestService(t, session, client, config, logger)

	for _, ip := range ManagedClusterHosts() {
		if err := client.RcloneResetStats(context.Background(), ip); err != nil {
			t.Error("Couldn't reset stats", ip, err)
		}
	}

	return &backupTestHelper{
		session:  session,
		hrt:      hrt,
		client:   client,
		service:  service,
		location: location,

		clusterID: clusterID,
		taskID:    uuid.MustRandom(),
		runID:     uuid.NewTime(),

		t: t,
	}
}

func newTestClient(t *testing.T, hrt *HackableRoundTripper, logger log.Logger, config *scyllaclient.Config) *scyllaclient.Client {
	t.Helper()

	if config == nil {
		c := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
		config = &c
	}
	config.Transport = hrt

	c, err := scyllaclient.NewClient(*config, logger)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newTestService(t *testing.T, session gocqlx.Session, client *scyllaclient.Client, c backup.Config, logger log.Logger) *backup.Service {
	t.Helper()

	s, err := backup.NewService(
		session,
		c,
		metrics.NewBackupMetrics(),
		func(_ context.Context, id uuid.UUID) (string, error) {
			return "test_cluster", nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID) (gocqlx.Session, error) {
			return CreateManagedClusterSession(t), nil
		},
		logger.Named("backup"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (h *backupTestHelper) listS3Files() (manifests, schemas, files []string) {
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
		if strings.HasPrefix(f.Path, "backup/meta") && strings.Contains(f.Path, "manifest") {
			manifests = append(manifests, f.Path)
		} else if strings.HasPrefix(f.Path, "backup/schema") {
			schemas = append(schemas, f.Path)
		} else {
			files = append(files, f.Path)
		}
	}
	return
}

func (h *backupTestHelper) progressFilesSet() *strset.Set {
	h.t.Helper()

	files := strset.New()

	iter := table.BackupRunProgress.SelectQuery(h.session).BindMap(qb.M{
		"cluster_id": h.clusterID,
		"task_id":    h.taskID,
		"run_id":     h.runID,
	}).Iter()
	defer iter.Close()

	pr := &backup.RunProgress{}
	for iter.StructScan(pr) {
		fs := pr.Files()
		for i := range fs {
			if strings.Contains(fs[i].Name, backup.ScyllaManifest) {
				continue
			}
			files.Add(fs[i].Name)
		}
	}

	return files
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
	maxWaitCond               = 5 * time.Second
	condCheckInterval         = 100 * time.Millisecond
	longPollingTimeoutSeconds = 2
)

func (h *backupTestHelper) waitCond(f func() bool) {
	WaitCond(h.t, f, condCheckInterval, maxWaitCond)
}

func (h *backupTestHelper) waitTransfersStarted() {
	h.waitCond(func() bool {
		h.t.Helper()
		for _, host := range ManagedClusterHosts() {
			job, err := h.client.RcloneJobInfo(context.Background(), host, scyllaclient.GlobalProgressID, longPollingTimeoutSeconds)
			if err != nil {
				h.t.Fatal(err)
			}
			for _, tr := range job.Stats.Transferring {
				if strings.HasSuffix(tr.Name, ".db") {
					Print("And: upload is underway with " + tr.Name)
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
		m, _, _ := h.listS3Files()
		return len(m) > 0
	})
}

func (h *backupTestHelper) waitNoTransfers() {
	h.waitCond(func() bool {
		h.t.Helper()
		for _, host := range ManagedClusterHosts() {
			job, err := h.client.RcloneJobInfo(context.Background(), host, scyllaclient.GlobalProgressID, longPollingTimeoutSeconds)
			if err != nil {
				h.t.Fatal(err)
			}
			if len(job.Stats.Transferring) > 0 {
				return false
			}
		}
		return true
	})
}

func (h *backupTestHelper) tamperWithManifest(ctx context.Context, manifestsPath string, f func(ManifestInfoWithContent) bool) {
	h.t.Helper()

	// Parse manifest path
	m := NewManifestInfoWithContent()
	if err := m.ParsePath(manifestsPath); err != nil {
		h.t.Fatal(err)
	}
	// Load manifest
	r, err := h.client.RcloneOpen(ctx, ManagedClusterHost(), h.location.RemotePath(manifestsPath))
	if err != nil {
		h.t.Fatal(err)
	}
	if err := m.Read(r); err != nil {
		h.t.Fatal(err)
	}
	r.Close()

	// Since we rewrite the manifest we need the index loaded into the struct.
	if err := m.LoadIndex(); err != nil {
		h.t.Fatal(err)
	}

	// Decorate, if not changed return early
	if !f(m) {
		return
	}
	// Save modified manifest
	buf := bytes.NewBuffer(nil)
	if err = m.Write(buf); err != nil {
		h.t.Fatal(err)
	}
	if err := h.client.RclonePut(ctx, ManagedClusterHost(), h.location.RemotePath(m.Path()), buf); err != nil {
		h.t.Fatal(err)
	}
}

func (h *backupTestHelper) touchFile(ctx context.Context, dir, file, content string) {
	h.t.Helper()
	buf := bytes.NewBufferString(content)
	if err := h.client.RclonePut(ctx, ManagedClusterHost(), h.location.RemotePath(path.Join(dir, file)), buf); err != nil {
		h.t.Fatal(err)
	}
}

func s3Location(bucket string) Location {
	return Location{
		Provider: S3,
		Path:     bucket,
	}
}

func TestGetTargetIntegration(t *testing.T) {
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
			Name:   "dc default rate limit",
			Input:  "testdata/get_target/dc_no_rate_limit.input.json",
			Golden: "testdata/get_target/dc_no_rate_limit.golden.json",
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
		h       = newBackupTestHelper(t, session, backup.DefaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateManagedClusterSessionAndDropAllKeyspaces(t).Close()
	S3InitBucket(t, testBucket)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			b, err := os.ReadFile(test.Input)
			if err != nil {
				t.Fatal(err)
			}
			v, err := h.service.GetTarget(ctx, h.clusterID, b)
			if err != nil {
				t.Fatal(err)
			}

			if UpdateGoldenFiles() {
				b, _ := json.Marshal(v)
				var buf bytes.Buffer
				json.Indent(&buf, b, "", "  ")
				if err := os.WriteFile(test.Golden, buf.Bytes(), 0666); err != nil {
					t.Error(err)
				}
			}

			b, err = os.ReadFile(test.Golden)
			if err != nil {
				t.Fatal(err)
			}
			var golden backup.Target
			if err := json.Unmarshal(b, &golden); err != nil {
				t.Error(err)
			}

			if diff := cmp.Diff(golden, v, cmpopts.SortSlices(func(a, b string) bool { return a < b }), cmpopts.IgnoreUnexported(backup.Target{})); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestGetTargetErrorIntegration(t *testing.T) {
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
			Error: "no keyspace matched criteria",
		},
		{
			Name:  "invalid dc filter",
			JSON:  `{"dc": ["foobar"], "location": ["s3:backuptest-get-target"]}`,
			Error: "no matching DCs found",
		},
		{
			Name:  "invalid location dc",
			JSON:  `{"location": ["foobar:s3:backuptest-get-target"]}`,
			Error: `invalid location: "foobar:s3:backuptest-get-target" no such datacenter foobar`,
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
			Error: "specified bucket does not exist",
		},
		{
			Name:  "invalid rate limit dc",
			JSON:  `{"rate_limit": ["foobar:100"], "location": ["s3:backuptest-get-target"]}`,
			Error: `invalid rate-limit: "foobar:100" no such datacenter foobar`,
		},
		{
			Name:  "invalid snapshot parallel dc",
			JSON:  `{"snapshot_parallel": ["foobar:100"], "location": ["s3:backuptest-get-target"]}`,
			Error: `invalid snapshot-parallel: "foobar:100" no such datacenter foobar`,
		},
		{
			Name:  "invalid upload parallel dc",
			JSON:  `{"upload_parallel": ["foobar:100"], "location": ["s3:backuptest-get-target"]}`,
			Error: `invalid upload-parallel: "foobar:100" no such datacenter foobar`,
		},
	}

	const testBucket = "backuptest-get-target-error"

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newBackupTestHelper(t, session, backup.DefaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateManagedClusterSessionAndDropAllKeyspaces(t).Close()
	S3InitBucket(t, testBucket)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			_, err := h.service.GetTarget(ctx, h.clusterID, json.RawMessage(test.JSON))
			if err == nil {
				t.Fatal("GetTarget() expected error")
			}

			if !strings.Contains(err.Error(), test.Error) {
				t.Fatalf("GetTarget() = %v, expected %v", err, test.Error)
			} else {
				t.Log("GetTarget():", err)
			}
		})
	}
}

func TestGetLastResumableRunIntegration(t *testing.T) {
	const testBucket = "backuptest-void"

	config := backup.DefaultConfig()

	var (
		session = CreateSession(t)
		h       = newBackupTestHelper(t, session, config, s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	putRun := func(t *testing.T, r *backup.Run) {
		t.Helper()
		if err := table.BackupRun.InsertQuery(session).BindStruct(r).ExecRelease(); err != nil {
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
			Stage:     StageInit,
		}
		putRun(t, r0)

		r1 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test"}},
			Stage:     StageInit,
		}
		putRun(t, r1)

		_, err := h.service.GetLastResumableRun(ctx, clusterID, taskID)
		if !errors.Is(err, service.ErrNotFound) {
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
			Stage:     StageUpload,
		}
		putRun(t, r0)

		r1 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test"}},
			Stage:     StageDone,
		}
		putRun(t, r1)

		_, err := h.service.GetLastResumableRun(ctx, clusterID, taskID)
		if !errors.Is(err, service.ErrNotFound) {
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
			Stage:     StageUpload,
		}
		putRun(t, r0)

		r1 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test2"}},
			Stage:     StageInit,
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
	const (
		testBucket   = "backuptest-smoke"
		testKeyspace = "backuptest_data"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []Location{location},
		Retention: 3,
	}
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}
	// Sleep to avoid tag collision.
	time.Sleep(time.Second)
	Print("And: run it again")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: there are two backups")
	items, err := h.service.List(ctx, h.clusterID, []Location{location}, backup.ListFilter{ClusterID: h.clusterID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("List() = %v, expected one item", items)
	}
	i := items[0]
	if len(i.SnapshotInfo) != 2 {
		t.Fatalf("List() = %v, expected two SnapshotTags", items)
	}

	Print("And: snapshots are removed from nodes")
	for _, host := range ManagedClusterHosts() {
		s, err := h.client.Snapshots(ctx, host)
		if err != nil {
			t.Fatal("Snapshots() error", err)
		}
		if len(s) > 0 {
			t.Fatalf("Found snapshots %s on host %s", s, host)
		}
	}

	Print("And: files")
	manifests, schemas, _ := h.listS3Files()
	// Manifest meta per host per snapshot
	expectedNumberOfManifests := 3 * 2
	if len(manifests) != expectedNumberOfManifests {
		t.Fatalf("expected %d manifests, got %d", expectedNumberOfManifests, len(manifests))
	}

	// Schema meta per snapshot
	const schemasCount = 2
	if len(schemas) != schemasCount {
		t.Fatalf("expected %d schemas, got %d", schemasCount, len(schemas))
	}

	filesInfo, err := h.service.ListFiles(ctx, h.clusterID, []Location{location}, backup.ListFilter{ClusterID: h.clusterID})
	if err != nil {
		t.Fatal("ListFiles() error", err)
	}
	if len(filesInfo) != 6 {
		t.Fatalf("len(ListFiles()) = %d, expected %d", len(filesInfo), len(manifests))
	}
	for _, fi := range filesInfo {
		for _, fs := range fi.Files {
			remoteFiles, err := h.client.RcloneListDir(ctx, ManagedClusterHosts()[0], h.location.RemotePath(fs.Path), nil)
			if err != nil {
				t.Fatal("RcloneListDir() error", err)
			}

			var remoteFileNames []string
			for _, f := range remoteFiles {
				remoteFileNames = append(remoteFileNames, f.Name)
			}

			Print("And: Scylla manifests are not uploaded")
			for _, rfn := range remoteFileNames {
				if strings.Contains(rfn, backup.ScyllaManifest) {
					t.Errorf("Unexpected Scylla manifest file at path: %s", h.location.RemotePath(fs.Path))
				}
			}

			tableFileNames := make([]string, 0, len(fs.Files))
			for _, f := range fs.Files {
				tableFileNames = append(tableFileNames, f)
			}

			opts := []cmp.Option{cmpopts.SortSlices(func(a, b string) bool { return a < b })}
			if !cmp.Equal(tableFileNames, remoteFileNames, opts...) {
				t.Fatalf("List of files from manifest doesn't match files on remote, diff: %s", cmp.Diff(fs.Files, remoteFileNames, opts...))
			}
		}
	}

	Print("And: manifests are in metadata directory")
	for _, s := range manifests {
		var m ManifestInfo
		if err := m.ParsePath(s); err != nil {
			t.Fatal("manifest file in wrong path", s)
		}
	}

	Print("When: write new data")
	// 200M is the multipart threshold we want to trigger it in the smoke test.
	WriteData(t, clusterSession, testKeyspace, 210)

	Print("And: run it again")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: there are three backups")
	items, err = h.service.List(ctx, h.clusterID, []Location{location}, backup.ListFilter{ClusterID: h.clusterID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("List() = %v, expected one item", items)
	}
	i = items[0]
	if len(i.SnapshotInfo) != 3 {
		t.Fatalf("List() = %v, expected three SnapshotTags", items)
	}

	Print("And: transfer statistics are cleared")
	for _, host := range ManagedClusterHosts() {
		job, err := h.client.RcloneJobInfo(context.Background(), host, scyllaclient.GlobalProgressID, longPollingTimeoutSeconds)
		if err != nil {
			h.t.Fatal(err)
		}
		if job.Stats.Transfers != 0 {
			t.Errorf("Expected empty transfer statistics, got %d", job.Stats.Transfers)
		}
		if len(job.Transferred) != 0 {
			for i, v := range job.Transferred {
				t.Logf("job.Transferred[%d]=%+v", i, *v)
			}
			t.Errorf("Expected empty transfers, got %d", len(job.Transferred))
		}
	}

	Print("And: user is able to list backup files using filters")
	filesInfo, err = h.service.ListFiles(ctx, h.clusterID, []Location{location}, backup.ListFilter{ClusterID: h.clusterID})
	if err != nil {
		t.Fatal("ListFiles() error", err)
	}

	// 3 backups * 3 nodes
	if len(filesInfo) != 3*3 {
		t.Fatalf("len(ListFiles()) = %d, expected %d", len(filesInfo), 3*3)
	}

	filesInfo, err = h.service.ListFiles(ctx, h.clusterID, []Location{location}, backup.ListFilter{
		ClusterID: h.clusterID,
		Keyspace:  []string{testKeyspace},
	})
	if err != nil {
		t.Fatal("ListFiles() error", err)
	}

	// 3 backups * 3 nodes
	if len(filesInfo) != 3*3 {
		t.Fatalf("len(ListFiles()) = %d, expected %d", len(filesInfo), 3*3)
	}

	assertManifestHasCorrectFormat(t, ctx, h, manifests[0], schemas)
}

func assertManifestHasCorrectFormat(t *testing.T, ctx context.Context, h *backupTestHelper, manifestPath string, schemas []string) {
	r, err := h.client.RcloneOpen(ctx, ManagedClusterHost(), h.location.RemotePath(manifestPath))
	if err != nil {
		t.Fatal(err)
	}
	var mc ManifestContentWithIndex
	if err := mc.Read(r); err != nil {
		t.Fatalf("Cannot read manifest created by backup: %s", err)
	}
	r.Close()

	if mc.ClusterName != "test_cluster" {
		t.Errorf("ClusterName=%s, expected test_cluster", mc.ClusterName)
	}
	if !strings.HasPrefix(mc.IP, "192.168.100.") {
		t.Errorf("IP=%s, expected IP address", mc.IP)
	}
	for _, fi := range mc.Index {
		if fi.Size == 0 {
			t.Errorf("Size=0 for table %s, expected non zero size", fi.Table)
		}
	}
	if mc.Size == 0 {
		t.Error("Size=0 for backup, expected non zero size")
	}
	if len(mc.Tokens) != 256 {
		t.Errorf("len(Tokens)=%d, expected 256 tokens", len(mc.Tokens))
	}
	if !strset.New(schemas...).Has(mc.Schema) {
		t.Errorf("Schema=%s, not found in schemas %s", mc.Schema, schemas)
	}
}

func TestBackupWithNodesDownIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-nodesdown"
		testKeyspace = "backuptest_data"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	Print("Given: downed node")
	if stdout, stderr, err := ExecOnHost("192.168.100.11", CmdBlockScyllaREST); err != nil {
		t.Fatal(err, stdout, stderr)
	}
	defer ExecOnHost("192.168.100.11", CmdUnblockScyllaREST)

	Print("When: get target")
	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []Location{location},
		Retention: 3,
	}

	Print("Then: target hosts does not contain the downed node")
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}
	if len(target.Hosts()) != 2 {
		t.Fatal(target.Hosts())
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: there are is no manifest for the downed node")
	manifests, _, _ := h.listS3Files()
	// Manifest meta per host per snapshot
	expectedNumberOfManifests := 2
	if len(manifests) != expectedNumberOfManifests {
		t.Fatalf("expected %d manifests, got %d", expectedNumberOfManifests, len(manifests))
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

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)
	)

	WriteData(t, clusterSession, testKeyspace, 3)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []Location{location},
		Retention: 2,
		RateLimit: []backup.DCLimit{
			{"dc1", 1},
		},
		Continue: true,
	}

	assertDataUploaded := func(t *testing.T, h *backupTestHelper) {
		t.Helper()

		manifests, _, files := h.listS3Files()
		if len(manifests) != 3 {
			t.Fatalf("Expected 3 manifests got %s", manifests)
		}
		if len(files) == 0 {
			t.Fatal("Expected data to be uploaded")
		}
	}

	assertDataUploadedAfterTag := func(t *testing.T, h *backupTestHelper, tag string) {
		t.Helper()

		manifests, _, files := h.listS3Files()
		c := 0
		for _, m := range manifests {
			if backup.SnapshotTagFromManifestPath(t, m) > tag {
				c += 1
			}
		}
		if c != 3 {
			t.Fatalf("Expected 3 new manifests got %d from %s", c, manifests)
		}
		if len(files) == 0 {
			t.Fatal("Expected data to be uploaded")
		}
	}

	getTagAndWait := func() string {
		tag := NewSnapshotTag()

		// Wait for new tag as they have a second resolution
		time.Sleep(time.Second)

		return tag
	}

	t.Run("resume after stop", func(t *testing.T) {
		var (
			h           = newBackupTestHelper(t, session, config, location, nil)
			ctx, cancel = context.WithCancel(context.Background())
			done        = make(chan struct{})
		)

		if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
			t.Fatal(err)
		}

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

		Print("And: snapshot is partially uploaded")
		_, _, s3Files := h.listS3Files()
		sfs := h.progressFilesSet()
		s3f := strset.New()
		for i := range s3Files {
			s3f.Add(path.Base(s3Files[i]))
		}
		if s3f.IsEqual(sfs) {
			h.t.Fatalf("Expected partial upload, got\n%v,\n%v", sfs, s3f)
		}

		Print("When: backup is resumed with new RunID")
		err := h.service.Backup(context.Background(), h.clusterID, h.taskID, uuid.NewTime(), target)
		if err != nil {
			t.Error("Unexpected error", err)
		}

		Print("Then: data is uploaded")
		assertDataUploaded(t, h)

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})

	t.Run("resume after agent restart", func(t *testing.T) {
		clientConf := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
		clientConf.Backoff.MaxRetries = 5

		var (
			h           = newBackupTestHelper(t, session, config, location, &clientConf)
			ctx, cancel = context.WithCancel(context.Background())
			done        = make(chan struct{})
		)
		defer cancel()

		if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
			t.Fatal(err)
		}

		go func() {
			Print("When: backup is running")
			err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target)
			if err != nil {
				t.Errorf("Expected no error but got %+v", err)
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

		Print("And: data is uploaded")
		assertDataUploaded(t, h)
	})

	t.Run("resume after snapshot failed", func(t *testing.T) {
		var (
			h          = newBackupTestHelper(t, session, config, location, nil)
			brokenHost string
			mu         sync.Mutex
		)

		Print("Given: snapshot fails on a host")
		h.hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost && req.URL.Path == "/storage_service/snapshots" {
				mu.Lock()
				if brokenHost == "" {
					t.Log("Setting broken host", req.Host)
					brokenHost = req.Host
				}
				mu.Unlock()

				if brokenHost == req.Host {
					return nil, errors.New("dial error on snapshot")
				}
			}
			return nil, nil
		}))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
			t.Fatal(err)
		}

		Print("When: run backup")
		err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target)

		Print("Then: it fails")
		if err == nil {
			t.Error("Expected error on run but got nil")
		}
		t.Log("Backup() error", err)

		Print("Given: snapshot not longer fails on a host")
		h.hrt.SetInterceptor(nil)

		Print("When: backup is resumed with new RunID")
		tag := getTagAndWait()
		err = h.service.Backup(context.Background(), h.clusterID, h.taskID, uuid.NewTime(), target)
		if err != nil {
			t.Error("Unexpected error", err)
		}

		Print("Then: data is uploaded")
		assertDataUploadedAfterTag(t, h, tag)

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})

	t.Run("continue false", func(t *testing.T) {
		var (
			h           = newBackupTestHelper(t, session, config, location, nil)
			ctx, cancel = context.WithCancel(context.Background())
			done        = make(chan struct{})
		)

		h.hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.HasPrefix(req.URL.Path, "/agent/rclone/operations/put") {
				Print("And: context is canceled")
				cancel()
			}

			return nil, nil
		}))

		target := target
		target.Continue = false
		if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
			t.Fatal(err)
		}

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
		tag := getTagAndWait()
		err := h.service.Backup(context.Background(), h.clusterID, h.taskID, uuid.NewTime(), target)
		if err != nil {
			t.Error("Unexpected error", err)
		}

		Print("Then: data is uploaded")
		assertDataUploadedAfterTag(t, h, tag)

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})
}

func TestBackupTemporaryManifestsIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-temporary-manifests"
		testKeyspace = "backuptest_temporary_manifests"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	Print("Given: retention policy of 1")
	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:           []string{"dc1"},
		Location:     []Location{location},
		Retention:    1,
		RetentionMap: backup.RetentionMap{h.taskID: {0, 1}},
	}

	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	Print("And: add a fake temporary manifest")
	manifests, _, _ := h.listS3Files()

	// Sleep to avoid tag collision.
	time.Sleep(time.Second)
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		// Mark manifest as temporary, change snapshot tag
		m.Temporary = true
		m.SnapshotTag = NewSnapshotTag()
		// Add "xxx" file to a table
		fi := &m.Index[0]
		fi.Files = append(fi.Files, "xxx")

		// Create the "xxx" file
		h.touchFile(ctx, path.Join(RemoteSSTableVersionDir(h.clusterID, m.DC, m.NodeID, fi.Keyspace, fi.Table, fi.Version)), "xxx", "xxx")

		return true
	})

	Print("Then: there is one backup in listing")
	items, err := h.service.List(ctx, h.clusterID, []Location{location}, backup.ListFilter{ClusterID: h.clusterID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("List() = %v, expected one item", items)
	}

	// Sleep to avoid tag collision.
	time.Sleep(time.Second)

	Print("When: run backup")
	h.runID = uuid.NewTime()
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	manifests, _, files := h.listS3Files()

	Print("Then: temporary manifest is removed")
	if len(manifests) != 3 {
		t.Fatalf("Expected 3 manifest got %d", len(manifests))
	}

	Print("And: xxx file is removed")
	for _, f := range files {
		if path.Base(f) == "xxx" {
			t.Fatalf("Found %s that should have been removed", f)
		}
	}
}

func TestBackupTemporaryManifestMoveRollbackOnErrorIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-rollback"
		testKeyspace = "backuptest_rollback"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
	)

	WriteData(t, clusterSession, testKeyspace, 3)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []Location{location},
		Retention: 3,
	}
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	movedManifests := atomic.NewInt64(0)
	h.hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/agent/rclone/operations/movefile" {
			return nil, nil
		}

		// Fail 3rd manifest move and do not retry
		if c := movedManifests.Add(1); c == 3 {
			return httpx.MakeAgentErrorResponse(req, 400, "explicit failure"), nil
		}

		return nil, nil
	}))

	Print("When: backup runs")
	err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target)
	Print("Then: it ends with error")
	if err == nil {
		t.Error("Expected error on run but got nil")
	} else {
		t.Log("Backup() error", err)
	}

	Print("And: manifest move is rolled back")
	var (
		manifests, _, _   = h.listS3Files()
		manifestCount     int
		tempManifestCount int
	)
	for _, m := range manifests {
		if strings.HasSuffix(m, TempFileExt) {
			tempManifestCount++
		} else {
			manifestCount++
		}
	}

	if tempManifestCount != 3 || manifestCount != 0 {
		t.Fatalf("Expected to have 3 temp manifests, found %d and %d manifests", tempManifestCount, manifestCount)
	}
}

func TestBackupTemporaryManifestsNotFoundIssue2862Integration(t *testing.T) {
	const (
		testBucket   = "backuptest-issue2862"
		testKeyspace = "backuptest_issue2862"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	Print("Given: retention policy of 1")
	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []Location{location},
		Retention: 1,
	}
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("And: interceptor removing tmp suffix from manifests")
	h.hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/rclone/operations/put") {
			q := req.URL.Query()
			// Remove .tmp suffix from remote
			remote := q.Get("remote")
			remote = strings.TrimSuffix("remote", ".tmp")
			q.Set("remote", remote)
			// Update request query
			req.URL.RawQuery = q.Encode()
		}
		return nil, nil
	}))

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}
	Print("Then: it ends with no error")
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
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)

		h   = newBackupTestHelper(t, session, config, location, nil)
		ctx = context.Background()
	)

	WriteData(t, clusterSession, testKeyspace, 3)

	task1 := h.taskID
	task2 := uuid.MustRandom()
	task3 := uuid.MustRandom()

	Print("Given: retention policy 1")
	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:           []string{"dc1"},
		Location:     []Location{location},
		Retention:    1,
		RetentionMap: map[uuid.UUID]backup.Retention{task1: {7, 1}, task2: {7, 1}, task3: {2, 7}},
	}

	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	// Get manifest prototype
	manifests, _, _ := h.listS3Files()
	if len(manifests) != 3 {
		t.Fatalf("Expected manifest per node, got %d", len(manifests))
	}
	now := timeutc.Now()

	Print("And: add manifest for removed node - should be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.NodeID = uuid.MustRandom().String()
		return true
	})
	Print("And: add manifest for task2 - should NOT be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.TaskID = task2
		m.SnapshotTag = SnapshotTagAt(now.AddDate(0, 0, -1))
		return true
	})
	Print("And: add another manifest for task2 - should be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.TaskID = task2
		m.SnapshotTag = SnapshotTagAt(now.AddDate(0, 0, -2))
		return true
	})
	Print("And: add 2 hour old manifest for task 3 - should NOT be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.TaskID = task3
		m.SnapshotTag = SnapshotTagAt(now.Add(time.Hour * -2))
		return true
	})
	Print("And: add 1 day old manifest for task 3 - should NOT be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.TaskID = task3
		m.SnapshotTag = SnapshotTagAt(now.Add(time.Hour * -26))
		return true
	})
	Print("And: add 3 day old manifest for task 3 - should be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.TaskID = task3
		m.SnapshotTag = SnapshotTagAt(now.AddDate(0, 0, -3))
		return true
	})
	Print("And: add manifest for removed old task - should be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.TaskID = uuid.MustRandom()
		m.SnapshotTag = SnapshotTagAt(now.AddDate(-1, 0, 0))
		return true
	})
	Print("And: add manifest for removed task - should NOT removed")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.TaskID = uuid.MustRandom()
		m.SnapshotTag = SnapshotTagAt(now.AddDate(0, 0, 3))
		return true
	})

	WriteData(t, clusterSession, testKeyspace, 3)

	Print("And: run backup again")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: there should be 3 + 4 manifests")
	manifests, _, files := h.listS3Files()
	if len(manifests) != 7 {
		t.Fatalf("Expected 7 manifests (1 per each node) plus 4 generated, got %d %s", len(manifests), strings.Join(manifests, "\n"))
	}

	Print("And: old sstable files are removed")
	var sstPfx []string
	for _, m := range manifests {
		r, err := h.client.RcloneOpen(ctx, ManagedClusterHost(), location.RemotePath(m))
		if err != nil {
			t.Fatal(err)
		}
		var c ManifestContentWithIndex
		if err := c.Read(r); err != nil {
			t.Fatal(err)
		}
		if err := c.LoadIndex(); err != nil {
			t.Fatal(err)
		}

		r.Close()

		for _, fi := range c.Index {
			for _, f := range fi.Files {
				sstPfx = append(sstPfx, strings.TrimSuffix(f, "-Data.db"))
			}
		}
	}

	for _, f := range files {
		ok := false
		for _, pfx := range sstPfx {
			if strings.HasPrefix(path.Base(f), pfx) || strings.HasSuffix(f, MetadataVersion) {
				ok = true
				break
			}
		}
		if !ok {
			t.Errorf("Unexpected file %s", f)
		}
	}
}

func TestPurgeTemporaryManifestsIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-purge-tmp"
		testKeyspace = "backuptest_purge_tmp"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)

		h   = newBackupTestHelper(t, session, config, location, nil)
		ctx = context.Background()
	)

	WriteData(t, clusterSession, testKeyspace, 3)

	Print("Given: retention policy 2")
	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []Location{location},
		Retention: 2,
	}
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	Print("And: add temporary manifest")
	manifests, _, _ := h.listS3Files()
	if len(manifests) != 3 {
		t.Fatalf("Expected manifest per node, got %d", len(manifests))
	}
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.NodeID = uuid.MustRandom().String()
		m.Temporary = true
		return true
	})

	Print("And: run backup again")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: there should be 3 manifests")
	manifests, _, _ = h.listS3Files()
	if len(manifests) != 3 {
		t.Fatalf("Expected 3 manifests (1 per each node), got %d %s", len(manifests), strings.Join(manifests, "\n"))
	}
}

func TestDeleteSnapshotIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-delete"
		testKeyspace = "backuptest_delete"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)

		h   = newBackupTestHelper(t, session, config, location, nil)
		ctx = context.Background()
	)

	Print("Given: retention policy of 1 for the both task")
	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []Location{location},
		Retention: 1,
	}

	Print("Given: given same data in shared keyspace")
	WriteData(t, clusterSession, testKeyspace, 3)

	Print("When: both tasks backup same data")
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewTime()
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, runID, target); err != nil {
		t.Fatal(err)
	}

	// Ensure second task has different snapshot tag
	time.Sleep(1 * time.Second)

	task2ID := uuid.NewTime()
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	runID = uuid.NewTime()
	if err := h.service.Backup(ctx, h.clusterID, task2ID, runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: both tasks references same data")

	firstTaskFilePaths := taskFiles(t, ctx, h, h.taskID)
	secondTaskFilePaths := taskFiles(t, ctx, h, task2ID)

	filesSymmetricDifference := strset.New(firstTaskFilePaths...)
	filesSymmetricDifference.Separate(strset.New(secondTaskFilePaths...))
	if !filesSymmetricDifference.IsEmpty() {
		t.Fatal("Expected to have same SST files in both tasks")
	}

	Print("When: first task snapshot is deleted")

	firstTaskTags := taskTags(t, ctx, h, h.taskID)
	if firstTaskTags.Size() != 1 {
		t.Fatalf("Expected to have single snapshot in the first task, got %d", firstTaskTags.Size())
	}

	if err := h.service.DeleteSnapshot(ctx, h.clusterID, []Location{h.location}, []string{firstTaskTags.Pop()}); err != nil {
		t.Fatal(err)
	}

	Print("Then: no files are removed")
	_, _, files := h.listS3Files()
	if len(files) == 0 {
		t.Fatal("Expected to have second task files in storage")
	}
	filesSymmetricDifference = strset.New(filterOutVersionFiles(files)...)
	filesSymmetricDifference.Separate(strset.New(secondTaskFilePaths...))

	if !filesSymmetricDifference.IsEmpty() {
		t.Fatal("Second task files were removed during first task snapshot delete")
	}

	Print("When: last snapshot is removed")
	secondTaskTags := taskTags(t, ctx, h, task2ID)
	if secondTaskTags.Size() != 1 {
		t.Fatalf("Expected have single snapshot in second task, got %d", secondTaskTags.Size())
	}

	if err := h.service.DeleteSnapshot(ctx, h.clusterID, []Location{h.location}, []string{secondTaskTags.Pop()}); err != nil {
		t.Fatal(err)
	}

	Print("Then: bucket is empty")
	manifests, schemas, files := h.listS3Files()
	sstFiles := filterOutVersionFiles(files)
	if len(manifests) != 0 || len(schemas) != 0 || len(sstFiles) != 0 {
		t.Errorf("Not all files were removed.\nmanifests: %s\nschemas: %s\nsstfiles: %s", manifests, schemas, sstFiles)
	}
}

func taskTags(t *testing.T, ctx context.Context, h *backupTestHelper, taskID uuid.UUID) *strset.Set {
	backups, err := h.service.List(ctx, h.clusterID, []Location{h.location},
		backup.ListFilter{ClusterID: h.clusterID, TaskID: taskID})
	if err != nil {
		t.Fatal(err)
	}

	taskTags := strset.New()
	for _, b := range backups {
		for _, si := range b.SnapshotInfo {
			taskTags.Add(si.SnapshotTag)
		}
	}

	return taskTags
}

func taskFiles(t *testing.T, ctx context.Context, h *backupTestHelper, taskID uuid.UUID) []string {
	t.Helper()

	filesFilter := backup.ListFilter{ClusterID: h.clusterID, TaskID: taskID}
	taskFiles, err := h.service.ListFiles(ctx, h.clusterID, []Location{h.location}, filesFilter)
	if err != nil {
		t.Fatal(err)
	}
	var taskFilesPaths []string
	for _, tf := range taskFiles {
		for _, fi := range tf.Files {
			for _, f := range fi.Files {
				taskFilesPaths = append(taskFilesPaths, path.Join(fi.Path, f))
			}
		}
	}
	return taskFilesPaths
}

func filterOutVersionFiles(files []string) []string {
	filtered := files[:0]
	for _, f := range files {
		if !strings.HasSuffix(f, MetadataVersion) {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

func TestGetValidationTargetErrorIntegration(t *testing.T) {
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
			Name:  "inaccessible location",
			JSON:  `{"location": ["s3:foo", "dc1:s3:bar"]}`,
			Error: "location is not accessible",
		},
	}

	const testBucket = "backuptest-get-validation-target-error"

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newBackupTestHelper(t, session, backup.DefaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateManagedClusterSessionAndDropAllKeyspaces(t).Close()
	S3InitBucket(t, testBucket)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			_, err := h.service.GetValidationTarget(ctx, h.clusterID, json.RawMessage(test.JSON))
			if err == nil {
				t.Fatal("GetValidationTarget() expected error")
			}

			if !strings.Contains(err.Error(), test.Error) {
				t.Fatalf("GetValidationTarget() = %v, expected %v", err, test.Error)
			} else {
				t.Log("GetValidationTarget():", err)
			}
		})
	}
}

func TestValidateIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-validate"
		testKeyspace = "backuptest_validate"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSessionAndDropAllKeyspaces(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []Location{location},
		Retention: 3,
	}
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	validationTarget := backup.ValidationTarget{
		Location: target.Location,
	}

	var (
		validateTaskID = uuid.MustRandom()
		validateRunID  uuid.UUID
	)
	runValidate := func() (progress []backup.ValidationHostProgress, err error) {
		validateRunID = uuid.NewTime()
		err = h.service.Validate(ctx, h.clusterID, validateTaskID, validateRunID, validationTarget)
		progress, _ = h.service.GetValidationProgress(ctx, h.clusterID, validateTaskID, validateRunID)
		return
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	manifests, _, files := h.listS3Files()

	genTag := func() string {
		return SnapshotTagAt(time.Unix(int64(rand.Uint32()), 0))
	}

	var (
		orphanedSnapshotTag = genTag()
		alienSnapshotTag    = genTag()
		tamperedSnapshotTag = genTag()
	)

	Print("And: add orphaned file - should be reported and deleted")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.SnapshotTag = orphanedSnapshotTag
		h.touchFile(ctx, path.Join(RemoteSSTableVersionDir(h.clusterID, m.DC, m.NodeID, "foo", "bar", "f0e76f40662e11ebbe97000000000001")), "xx0", "xxx")
		return false
	})

	Print("And: copy manifest to a different nodeID - should be reported as broken")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.SnapshotTag = alienSnapshotTag
		m.NodeID = uuid.MustRandom().String()
		m.IP = "1.2.3.4"
		return true
	})

	Print("And: add file referenced by temporary manifest - should NOT be reported nor deleted")
	h.tamperWithManifest(ctx, manifests[0], func(m ManifestInfoWithContent) bool {
		m.SnapshotTag = genTag()
		m.Temporary = true
		fi := &m.Index[0]
		fi.Files = append(fi.Files, "xx1")
		h.touchFile(ctx, path.Join(RemoteSSTableVersionDir(h.clusterID, m.DC, m.NodeID, fi.Keyspace, fi.Table, fi.Version)), "xx1", "xxx")
		return true
	})

	Print("And: add tampered manifest - should be reported as broken snapshot")
	h.tamperWithManifest(ctx, manifests[1], func(m ManifestInfoWithContent) bool {
		m.SnapshotTag = tamperedSnapshotTag
		fi := &m.Index[0]
		fi.Files = append(fi.Files, "xx2")
		return true
	})

	Print("When: run Validate")
	progress, err := runValidate()
	var buf []string
	for i := range progress {
		buf = append(buf, fmt.Sprintf("%s %+v", progress[i].Host, progress[i].ValidationResult))
	}
	t.Logf("Validate() = \n%s", strings.Join(buf, "\n"))
	t.Logf("Validate() error %s", err)

	Print("Then: error message contains description")
	if !strings.Contains(fmt.Sprint(err), "orphaned files") {
		t.Fatalf("Wrong error message: %s", err)
	}
	if !strings.Contains(fmt.Sprint(err), "broken snapshots") {
		t.Fatalf("Wrong error message: %s", err)
	}

	findRowBySnapshotTag := func(snapshotTag string) backup.ValidationHostProgress {
		for _, r := range progress {
			if slice.ContainsString(r.BrokenSnapshots, snapshotTag) {
				return r
			}
		}
		return backup.ValidationHostProgress{}
	}

	Print("And: progress is accurate")
	var deleted = 0
	for _, r := range progress {
		deleted += r.DeletedFiles
		if r.OrphanedFiles == 1 {
			if r.OrphanedFiles != 1 || r.OrphanedBytes != 3 || r.DeletedFiles != 0 {
				t.Fatal("Wrong result", r.ValidationResult)
			}
		}
	}
	if deleted != 0 {
		t.Fatalf("Wrong nr. of deleted files %d, expected 0", deleted)
	}

	r := findRowBySnapshotTag(alienSnapshotTag)
	if r.MissingFiles < 9 || r.Host != "1.2.3.4" {
		t.Error("Wrong result")
	}
	r = findRowBySnapshotTag(tamperedSnapshotTag)
	if r.MissingFiles != 1 {
		t.Error("Wrong result")
	}

	Print("When: delete orphaned files")
	validationTarget.DeleteOrphanedFiles = true

	Print("And: run Validate")
	progress, err = runValidate()

	Print("Then: error message contains broken snapshots only")
	if strings.Contains(fmt.Sprint(err), "orphaned files") {
		t.Fatalf("Wrong error message: %s", err)
	}
	if !strings.Contains(fmt.Sprint(err), "broken snapshots") {
		t.Fatalf("Wrong error message: %s", err)
	}

	Print("And: progress is accurate")
	deleted = 0
	for _, r := range progress {
		deleted += r.DeletedFiles
		if r.OrphanedFiles == 1 {
			if r.OrphanedFiles != 1 || r.OrphanedBytes != 3 || r.DeletedFiles != 1 {
				t.Fatal("Wrong result", r.ValidationResult)
			}
		}
	}
	if deleted != 1 {
		t.Fatalf("Wrong nr. of deleted files %d, expected 1", deleted)
	}

	Print("And: only orphaned files are deleted")
	_, _, postDeleteFiles := h.listS3Files()
	if !strset.New(postDeleteFiles...).Has(files...) {
		t.Fatalf("Missing files")
	}
	if len(postDeleteFiles) != len(files)+1 {
		t.Fatalf("Delete error")
	}
}

func TestBackupRestoreIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-restore"
		testKeyspace = "backuptest_restore"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session         = CreateSession(t)
		clusterSession  = CreateManagedClusterSessionAndDropAllKeyspaces(t)
		h               = newBackupTestHelper(t, session, config, location, nil)
		ctx             = context.Background()
		initialRowCount = 100
		addedRowCount   = 150
	)

	Print("When: cluster has data")
	ExecStmt(t, clusterSession, "CREATE KEYSPACE IF NOT EXISTS "+testKeyspace+" WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
	ExecStmt(t, clusterSession, "CREATE TABLE IF NOT EXISTS "+testKeyspace+".test_table (id int PRIMARY KEY)")
	for i := 1; i <= initialRowCount; i++ {
		ExecStmt(t, clusterSession, "INSERT INTO "+testKeyspace+".test_table (id) VALUES "+fmt.Sprintf("(%d)", i))
	}

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
				Tables:   []string{"test_table"},
			},
			{
				Keyspace:  "system_schema",
				AllTables: true,
			},
		},
		DC:        []string{"dc1", "dc2"},
		Location:  []Location{location},
		Retention: 3,
	}
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("And: run backup")
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, h.runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: there is one backup")
	items, err := h.service.List(ctx, h.clusterID, []Location{location}, backup.ListFilter{ClusterID: h.clusterID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("List() = %v, expected one item", items)
	}
	item := items[0]
	if len(item.SnapshotInfo) != 1 {
		t.Fatalf("List() = %v, expected one SnapshotTag", items)
	}
	snapshotTag := item.SnapshotInfo[0].SnapshotTag

	Print("And: add more data")
	for i := initialRowCount + 1; i <= addedRowCount; i++ {
		ExecStmt(t, clusterSession, "INSERT INTO "+testKeyspace+".test_table (id) VALUES "+fmt.Sprintf("(%d)", i))
	}

	Print("Then: there is 15 rows")
	count := func() int {
		c := 0
		if err := clusterSession.Query("SELECT COUNT(*) FROM "+testKeyspace+".test_table;", nil).Scan(&c); err != nil {
			t.Fatal("select count:", err)
		}
		return c
	}
	if c := count(); c != addedRowCount {
		t.Errorf("SELECT COUNT(*) = %d, expected %d", c, addedRowCount)
	}

	Print("When: delete data")
	q := clusterSession.Query("TRUNCATE "+testKeyspace+".test_table", nil)
	q.Consistency(gocql.All)
	if err := q.ExecRelease(); err != nil {
		t.Fatal("TRUNCATE error", err)
	}

	Print("And: restore snapshot")
	status, err := h.client.Status(ctx)
	if err != nil {
		t.Fatal("Status() error", err)
	}

	for _, nis := range status {
		stdout, stderr, err := ExecOnHost(nis.Addr, "chown scylla:scylla -R /var/lib/scylla/data")
		if err != nil {
			t.Log("stdout", stdout)
			t.Log("stderr", stderr)
			t.Fatal("Command failed on host", nis.Addr, err)
		}
	}
	downloadFilesCmd := []string{
		"sudo -u scylla",
		"scylla-manager-agent",
		"download-files",
		"-d", "/var/lib/scylla/data",
		"-L", location.String(),
		"-T", snapshotTag,
		"-K", testKeyspace,
		"--mode", "upload",
	}
	for _, nis := range status {
		stdout, stderr, err := ExecOnHost(nis.Addr, strings.Join(downloadFilesCmd, " "))
		if err != nil {
			t.Log("stdout", stdout)
			t.Log("err", err)
			t.Log("stderr", stderr)
			t.Fatal("Command failed on host", nis.Addr, err)
		}
	}
	for _, nis := range status {
		stdout, stderr, err := ExecOnHost(nis.Addr, "nodetool refresh "+testKeyspace+" test_table")
		if err != nil {
			t.Log("stdout", stdout)
			t.Log("stderr", stderr)
			t.Fatal("Command failed on host", nis.Addr, err)
		}
	}

	Print("Then: there is previous number of rows")
	if c := count(); c != initialRowCount {
		t.Errorf("SELECT COUNT(*) = %d, expected %d", c, addedRowCount)
	}
}
