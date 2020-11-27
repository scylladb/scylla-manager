// Copyright (C) 2017 ScyllaDB

// +build all integration

package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"
)

type backupTestHelper struct {
	session  gocqlx.Session
	hrt      *HackableRoundTripper
	client   *scyllaclient.Client
	service  *backup.Service
	location backup.Location

	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	t *testing.T
}

func newBackupTestHelper(t *testing.T, session, clusterSession gocqlx.Session, config backup.Config, location backup.Location, clientConf *scyllaclient.Config) *backupTestHelper {
	t.Helper()

	S3InitBucket(t, location.Path)

	clusterID := uuid.MustRandom()

	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	client := newTestClient(t, hrt, logger.Named("client"), clientConf)
	service := newTestService(t, session, clusterSession, client, config, clusterID, logger)

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

func newTestService(t *testing.T, session, clusterSession gocqlx.Session, client *scyllaclient.Client, c backup.Config, clusterID uuid.UUID,
	logger log.Logger) *backup.Service {
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
		func(ctx context.Context, clusterID uuid.UUID) (gocqlx.Session, error) {
			return clusterSession, nil
		},
		logger.Named("backup"),
		nil,
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

func (h *backupTestHelper) assertMetadataVersion(ctx context.Context, expected string) {
	var versionFiles []string

	_, _, files := h.listS3Files()
	for _, f := range files {
		if strings.HasSuffix(f, backup.MetadataVersion) {
			versionFiles = append(versionFiles, f)
		}
	}

	if len(versionFiles) != 3 {
		h.t.Fatalf("Expected 3 metadata version files, got %d", len(versionFiles))
	}

	for _, f := range versionFiles {
		content, err := h.client.RcloneCat(ctx, ManagedClusterHost(), h.location.RemotePath(f))
		if err != nil {
			h.t.Fatal(err)
		}

		var mv struct {
			Version string `json:"version"`
		}
		if err := json.Unmarshal(content, &mv); err != nil {
			h.t.Fatal(err)
		}

		if mv.Version != expected {
			h.t.Fatalf("Expected version %s, got %s", expected, mv.Version)
		}
	}
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
				if tr.Name != backup.ScyllaManifest {
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

func s3Location(bucket string) backup.Location {
	return backup.Location{
		Provider: backup.S3,
		Path:     bucket,
	}
}

func TestServiceGetTargetIntegration(t *testing.T) {
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
		session        = CreateSessionWithoutMigration(t)
		clusterSession = CreateManagedClusterSession(t)
		h              = newBackupTestHelper(t, session, clusterSession, backup.DefaultConfig(), s3Location(testBucket), nil)
		ctx            = context.Background()
	)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			b, err := ioutil.ReadFile(test.Input)
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

			if diff := cmp.Diff(golden, v, cmpopts.SortSlices(func(a, b string) bool { return a < b }), cmpopts.IgnoreUnexported(backup.Target{})); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestServiceGetTargetErrorIntegration(t *testing.T) {
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
			Error: "location is not accessible",
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
		session        = CreateSessionWithoutMigration(t)
		clusterSession = CreateManagedClusterSession(t)
		h              = newBackupTestHelper(t, session, clusterSession, backup.DefaultConfig(), s3Location(testBucket), nil)
		ctx            = context.Background()
	)

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

func TestServiceGetLastResumableRunIntegration(t *testing.T) {
	const testBucket = "backuptest-void"

	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSession(t)
		h              = newBackupTestHelper(t, session, clusterSession, config, s3Location(testBucket), nil)
		ctx            = context.Background()
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
			Stage:     backup.StageInit,
		}
		putRun(t, r0)

		r1 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test"}},
			Stage:     backup.StageInit,
		}
		putRun(t, r1)

		_, err := h.service.GetLastResumableRun(ctx, clusterID, taskID)
		if err != service.ErrNotFound {
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
			Stage:     backup.StageUpload,
		}
		putRun(t, r0)

		r1 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test"}},
			Stage:     backup.StageDone,
		}
		putRun(t, r1)

		_, err := h.service.GetLastResumableRun(ctx, clusterID, taskID)
		if err != service.ErrNotFound {
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
			Stage:     backup.StageUpload,
		}
		putRun(t, r0)

		r1 := &backup.Run{
			ClusterID: clusterID,
			TaskID:    taskID,
			ID:        uuid.NewTime(),
			Units:     []backup.Unit{{Keyspace: "test2"}},
			Stage:     backup.StageInit,
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
		testBucket         = "backuptest-smoke"
		testKeyspace       = "backuptest_data"
		goldenManifestPath = "testdata/manifest_format/golden.json.gz"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSession(t)
		h              = newBackupTestHelper(t, session, clusterSession, config, location, nil)
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
		Location:  []backup.Location{location},
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
	items, err := h.service.List(ctx, h.clusterID, []backup.Location{location}, backup.ListFilter{ClusterID: h.clusterID})
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

	Print("And: files")
	manifests, schemas, _ := h.listS3Files()
	// Manifest meta per host per snapshot
	expectedNumberOfManifests := 3 * 2
	if len(manifests) != expectedNumberOfManifests {
		t.Fatalf("expected %d manifests, got %d", expectedNumberOfManifests, len(manifests))
	}

	// Schema meta per snapshot
	expectedNumberOfSchemas := 2
	if len(schemas) != expectedNumberOfSchemas {
		t.Fatalf("expected %d schemas, got %d", expectedNumberOfSchemas, len(schemas))
	}

	filesInfo, err := h.service.ListFiles(ctx, h.clusterID, []backup.Location{location}, backup.ListFilter{ClusterID: h.clusterID})
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
	for _, m := range manifests {
		if err := backup.ParsePartialPath(m); err != nil {
			t.Fatal("manifest file in wrong path", m)
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
	items, err = h.service.List(ctx, h.clusterID, []backup.Location{location}, backup.ListFilter{ClusterID: h.clusterID})
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
	filesInfo, err = h.service.ListFiles(ctx, h.clusterID, []backup.Location{location}, backup.ListFilter{ClusterID: h.clusterID, Keyspace: []string{"some-other-keyspace"}})
	if err != nil {
		t.Fatal("ListFiles() error", err)
	}

	// 3 backups * 3 nodes
	if len(filesInfo) != 3*3 {
		t.Fatalf("len(ListFiles()) = %d, expected %d", len(filesInfo), 3*3)
	}

	// But empty tables because of the filter
	for _, fi := range filesInfo {
		if len(fi.Files) != 0 {
			t.Fatalf("len(ListFiles()) = %d, expected %d", len(fi.Files), 0)
		}
	}

	filesInfo, err = h.service.ListFiles(ctx, h.clusterID, []backup.Location{location}, backup.ListFilter{ClusterID: h.clusterID, Keyspace: []string{testKeyspace}})
	if err != nil {
		t.Fatal("ListFiles() error", err)
	}

	// 3 backups * 3 nodes
	if len(filesInfo) != 3*3 {
		t.Fatalf("len(ListFiles()) = %d, expected %d", len(filesInfo), 3*3)
	}

	h.assertMetadataVersion(ctx, "v2")

	thenManifestHasCorrectFormat(t, ctx, h, manifests[0], goldenManifestPath, schemas)
}

func thenManifestHasCorrectFormat(t *testing.T, ctx context.Context, h *backupTestHelper,
	manifestPath, goldenManifestPath string, schemas []string) {
	manifestsContent, err := h.client.RcloneCat(ctx, ManagedClusterHost(), h.location.RemotePath(manifestPath))
	if err != nil {
		t.Fatal(err)
	}

	var manifest backup.ManifestContent
	if err := manifest.Read(bytes.NewReader(manifestsContent)); err != nil {
		t.Fatalf("Cannot read manifest created by backup: %s", err)
	}

	if UpdateGoldenFiles() {
		var buf bytes.Buffer
		if err := manifest.Write(&buf); err != nil {
			t.Error(err)
		}
		if err := ioutil.WriteFile(goldenManifestPath, buf.Bytes(), 0666); err != nil {
			t.Error(err)
		}
	}

	buf, err := ioutil.ReadFile(goldenManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var golden backup.ManifestContent
	if err := golden.Read(bytes.NewReader(buf)); err != nil {
		t.Error(err)
	}

	opts := []cmp.Option{
		cmpopts.IgnoreFields(backup.ManifestContent{}, "Size", "TokenRanges", "Schema"),
		cmpopts.IgnoreFields(backup.ModelFilesInfo{}, "Version", "Size"),
	}
	if diff := cmp.Diff(golden, manifest, opts...); diff != "" {
		t.Fatal(diff)
	}
	if manifest.Size == 0 {
		t.Error("expected non zero backup size")
	}
	for i, fi := range manifest.Index {
		if fi.Size == 0 {
			t.Errorf("%d: expected non zero table %s size", i, fi.Table)
		}
	}
	if len(manifest.TokenRanges) == 0 {
		t.Error("expected token ranges in manifest")
	}

	if !strset.New(schemas...).Has(manifest.Schema) {
		t.Error("path from manifest not found in schemas")
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
		clusterSession = CreateManagedClusterSession(t)
		h              = newBackupTestHelper(t, session, clusterSession, config, location, nil)
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
		Location:  []backup.Location{location},
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
		clusterSession = CreateManagedClusterSession(t)
	)

	WriteData(t, clusterSession, testKeyspace, 3)

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

	assertDataUploded := func(t *testing.T, h *backupTestHelper) {
		t.Helper()

		manifests, _, files := h.listS3Files()
		if len(manifests) != 3 {
			t.Fatalf("Expected 3 manifests got %s", manifests)
		}
		if len(files) == 0 {
			t.Fatal("Expected data to be uploaded")
		}
	}

	assertDataUplodedAfterTag := func(t *testing.T, h *backupTestHelper, tag string) {
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
		tag := backup.NewSnapshotTag()

		// Wait for new tag as they have a second resolution
		time.Sleep(time.Second)

		return tag
	}

	t.Run("resume after stop", func(t *testing.T) {
		var (
			h           = newBackupTestHelper(t, session, clusterSession, config, location, nil)
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
		assertDataUploded(t, h)

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})

	t.Run("resume after agent restart", func(t *testing.T) {
		clientConf := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
		clientConf.Backoff.MaxRetries = 5

		var (
			h           = newBackupTestHelper(t, session, clusterSession, config, location, &clientConf)
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
		assertDataUploded(t, h)
	})

	t.Run("resume after snapshot failed", func(t *testing.T) {
		var (
			h          = newBackupTestHelper(t, session, clusterSession, config, location, nil)
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
		assertDataUplodedAfterTag(t, h, tag)

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})

	t.Run("continue false", func(t *testing.T) {
		var (
			h           = newBackupTestHelper(t, session, clusterSession, config, location, nil)
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
		assertDataUplodedAfterTag(t, h, tag)

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

		h   = newBackupTestHelper(t, session, clusterSession, config, location, nil)
		ctx = context.Background()
	)

	Print("Given: retention policy of 2 for the first task")
	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []backup.Location{location},
		Retention: 2,
	}

	Print("And: retention policy of 1 for the second task")
	target2 := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []backup.Location{location},
		Retention: 1,
	}

	Print("When: run both backup task 3 times")
	task2ID := uuid.NewTime()
	var runID uuid.UUID
	for i := 0; i < 3; i++ {
		// Ensure different snapshot tag between tasks
		time.Sleep(1 * time.Second)

		WriteData(t, clusterSession, testKeyspace, 3)
		if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
			t.Fatal(err)
		}
		runID = uuid.NewTime()
		if err := h.service.Backup(ctx, h.clusterID, h.taskID, runID, target); err != nil {
			t.Fatal(err)
		}

		// Ensure different snapshot tag between tasks
		time.Sleep(1 * time.Second)

		if err := h.service.InitTarget(ctx, h.clusterID, &target2); err != nil {
			t.Fatal(err)
		}
		runID = uuid.NewTime()
		if err := h.service.Backup(ctx, h.clusterID, task2ID, runID, target2); err != nil {
			t.Fatal(err)
		}
	}

	firstTaskTags := getTaskTags(t, ctx, h, h.taskID)
	if firstTaskTags.Size() != target.Retention {
		t.Errorf("First task retention policy is not preserved, expected %d, got %d snapshots", target.Retention, firstTaskTags.Size())
	}

	secondTaskTags := getTaskTags(t, ctx, h, task2ID)
	if secondTaskTags.Size() != target2.Retention {
		t.Errorf("Second task retention policy is not preserved, expected %d, got %d snapshots", target.Retention, secondTaskTags.Size())
	}

	allSnapshotTags := getTaskTags(t, ctx, h, uuid.Nil)
	if !allSnapshotTags.IsSubset(firstTaskTags) {
		t.Error("First task snapshot was removed during second task purging")
	}

	Print("Then: there are tree backups in total (2+1)")
	manifests, _, files := h.listS3Files()
	for _, m := range manifests {
		if !strings.Contains(m, h.taskID.String()) && !strings.Contains(m, task2ID.String()) {
			t.Errorf("Unexpected file %s manifest does not belong to tasks %s or %s", m, h.taskID, task2ID)
		}
	}
	if len(manifests) != 9 {
		t.Fatalf("Expected 9 manifests (3 per each node) got %s", manifests)
	}

	Print("And: old sstable files are removed")
	var sstPfx []string
	for _, m := range manifests {
		b, err := h.client.RcloneCat(ctx, ManagedClusterHost(), location.RemotePath(m))
		if err != nil {
			t.Fatal(err)
		}

		rm := backup.RemoteManifest{}
		if err := rm.ReadContent(bytes.NewReader(b)); err != nil {
			t.Fatal(err)
		}

		for _, fi := range rm.Content.Index {
			for _, f := range fi.Files {
				sstPfx = append(sstPfx, strings.TrimSuffix(f, "-Data.db"))
			}
		}
	}

	for _, f := range files {
		ok := false
		for _, pfx := range sstPfx {
			if strings.HasPrefix(path.Base(f), pfx) || strings.HasSuffix(f, backup.MetadataVersion) {
				ok = true
				break
			}
		}
		if !ok {
			t.Errorf("Unexpected file %s", f)
		}
	}
}

func TestBackupSnapshotDeleteIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-delete"
		testKeyspace = "backuptest_delete"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSession(t)

		h   = newBackupTestHelper(t, session, clusterSession, config, location, nil)
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
		Location:  []backup.Location{location},
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

	firstTaskFilePaths := getTaskFiles(t, ctx, h, h.taskID)
	secondTaskFilePaths := getTaskFiles(t, ctx, h, task2ID)

	filesSymmetricDifference := strset.New(firstTaskFilePaths...)
	filesSymmetricDifference.Separate(strset.New(secondTaskFilePaths...))
	if !filesSymmetricDifference.IsEmpty() {
		t.Fatal("Expected to have same SST files in both tasks")
	}

	Print("When: first task snapshot is deleted")

	firstTaskTags := getTaskTags(t, ctx, h, h.taskID)
	if firstTaskTags.Size() != 1 {
		t.Fatalf("Expected to have single snapshot in the first task, got %d", firstTaskTags.Size())
	}

	if err := h.service.DeleteSnapshot(ctx, h.clusterID, []backup.Location{h.location}, firstTaskTags.Pop()); err != nil {
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
	secondTaskTags := getTaskTags(t, ctx, h, task2ID)
	if secondTaskTags.Size() != 1 {
		t.Fatalf("Expected have single snapshot in second task, got %d", secondTaskTags.Size())
	}

	if err := h.service.DeleteSnapshot(ctx, h.clusterID, []backup.Location{h.location}, secondTaskTags.Pop()); err != nil {
		t.Fatal(err)
	}

	Print("Then: bucket is empty")
	manifests, schemas, files := h.listS3Files()
	sstFiles := filterOutVersionFiles(files)
	if len(manifests) != 0 || len(schemas) != 0 || len(sstFiles) != 0 {
		t.Errorf("Not all files were removed.\nmanifests: %s\nschemas: %s\nsstfiles: %s", manifests, schemas, sstFiles)
	}
}

func filterOutVersionFiles(files []string) []string {
	filtered := files[:0]
	for _, f := range files {
		if !strings.HasSuffix(f, backup.MetadataVersion) {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

func getTaskFiles(t *testing.T, ctx context.Context, h *backupTestHelper, taskID uuid.UUID) []string {
	t.Helper()

	filesFilter := backup.ListFilter{ClusterID: h.clusterID, TaskID: taskID}
	taskFiles, err := h.service.ListFiles(ctx, h.clusterID, []backup.Location{h.location}, filesFilter)
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

func TestBackupManifestIsRolledBackInCaseOfAnyErrorIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-rollback"
		testKeyspace = "backuptest_rollback"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSession(t)

		h           = newBackupTestHelper(t, session, clusterSession, config, location, nil)
		ctx, cancel = context.WithCancel(context.Background())
		done        = make(chan struct{})
	)

	WriteData(t, clusterSession, testKeyspace, 3)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []backup.Location{location},
		Retention: 3,
	}
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	putCounter := atomic.NewInt64(0)

	h.hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.Path, "/agent/rclone/operations/put") &&
			strings.Contains(req.URL.RawQuery, "manifest") {
			if putCounter.Load() >= 1 {
				h.waitCond(func() bool {
					manifests, _, _ := h.listS3Files()
					return len(manifests) > 0
				})
				Print("And: context is canceled")
				cancel()
			}
			putCounter.Inc()
		}

		return nil, nil
	}))

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

	manifests, _, _ := h.listS3Files()

	if len(manifests) != 0 {
		t.Fatalf("Expected to have 0 manifests, found %d", len(manifests))
	}
}

func TestPurgeOfV1BackupIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-purge-v1"
		testKeyspace = "backuptest_purge_v1"
	)

	location := s3Location(testBucket)
	config := backup.DefaultConfig()

	var (
		session        = CreateSession(t)
		clusterSession = CreateManagedClusterSession(t)

		h   = newBackupTestHelper(t, session, clusterSession, config, location, nil)
		ctx = context.Background()
	)

	// Test cluster nodes has different node IDs then prepared test data.
	// Fake mapping in order to make test data visible to backup logic.
	testDataNodesIDs := []string{
		"8bf49512-e550-4c3f-a3e4-8add7e3da20c",
		"99a9e3c8-9d9d-4f24-9094-54ea5e9af41a",
		"92567557-babc-46e9-a169-cd17b60fa78d",
	}

	overwriteClusterIDs(t, "dc1", testDataNodesIDs, h)
	h.clusterID = uuid.MustParse("6a14ea0a-0f7c-4d39-84bf-06413188b029")
	h.taskID = uuid.MustParse("ea986c36-9c97-4f7d-ac49-ec0166f0c8ca")

	Print("Given: backup using v1 manifests")

	uploadedFiles := uploadV1Backup(t, ctx, "testdata/v1-support", location, h)

	WriteData(t, clusterSession, testKeyspace, 3)

	Print("Given: retention policy 1")
	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []backup.Location{location},
		Retention: 2,
	}
	if err := h.service.InitTarget(ctx, h.clusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: backup is run")
	runID := uuid.NewTime()
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, runID, target); err != nil {
		t.Fatal(err)
	}

	manifests, _, _ := h.listS3Files()
	for _, m := range manifests {
		if !strings.Contains(m, h.taskID.String()) {
			t.Errorf("Unexpected file %s manifest does not belong to task %s", m, h.taskID)
		}
	}
	// 10 tables * 3 nodes + 3 v1 migrated to v2 + 3 v2
	v1Manifests := 10 * 3
	v2Manifests := 3 + 3
	if len(manifests) != v1Manifests+v2Manifests {
		t.Fatalf("Expected 3 manifests got %d", len(manifests))
	}

	Print("When: another backup is run")

	WriteData(t, clusterSession, testKeyspace, 3)
	runID = uuid.NewTime()
	if err := h.service.Backup(ctx, h.clusterID, h.taskID, runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: only the last task run is preserved")
	manifests, _, _ = h.listS3Files()
	for _, m := range manifests {
		if !strings.Contains(m, h.taskID.String()) {
			t.Errorf("Unexpected file %s manifest does not belong to task %s", m, h.taskID)
		}
	}
	if len(manifests) != 2*3 {
		t.Fatalf("Expected 3 manifests got %d", len(manifests))
	}

	Print("Then: old files are removed")

	for _, filePath := range uploadedFiles {
		files, err := h.client.RcloneListDir(ctx, ManagedClusterHost(), filepath.Dir(filePath), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 0 {
			t.Fatalf("expected to find 0 files, got %d", len(files))
		}
	}

	Print("Then: V1 directory structure is removed at node level")

	opts := &scyllaclient.RcloneListDirOpts{
		DirsOnly:  true,
		NoModTime: true,
	}

	for _, nodeID := range testDataNodesIDs {
		manifestDir := h.location.RemotePath(backup.RemoteManifestDir(h.clusterID, "dc1", nodeID))
		dirs, err := h.client.RcloneListDir(ctx, ManagedClusterHost(), manifestDir, opts)

		if err != nil {
			t.Fatal(err)
		}

		if len(dirs) != 0 {
			t.Fatalf("Expected to not have any directories at %s path, got %v", manifestDir, dirs)
		}
	}
}

func getTaskTags(t *testing.T, ctx context.Context, h *backupTestHelper, taskID uuid.UUID) *strset.Set {
	backups, err := h.service.List(ctx, h.clusterID, []backup.Location{h.location},
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

func overwriteClusterIDs(t *testing.T, dc string, nodeIDs []string, h *backupTestHelper) {
	nodeMapping, err := h.client.HostIDs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	nodeIPToID := map[string]string{}
	for nodeIP, nodeID := range nodeMapping {
		nodeIPToID[nodeIP] = nodeID
	}
	dcMap, err := h.client.Datacenters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var fakeMapping []map[string]string
	for d, nodeIps := range dcMap {
		var k, v string
		for _, nodeIP := range nodeIps {
			k = nodeIP
			if d == dc {
				v = nodeIDs[0]
				nodeIDs = nodeIDs[1:]
			} else {
				v = nodeIPToID[nodeIP]
			}
			fakeMapping = append(fakeMapping, map[string]string{
				"key":   k,
				"value": v,
			})
		}
	}

	h.hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/storage_service/host_id" {
			resp := &http.Response{
				Status:     "200 OK",
				StatusCode: 200,
				Proto:      "HTTP/1.1",
				ProtoMajor: 1,
				ProtoMinor: 1,
				Request:    req,
				Header:     make(http.Header, 0),
			}

			buf, err := json.Marshal(fakeMapping)
			if err != nil {
				return nil, err
			}

			resp.Body = ioutil.NopCloser(bytes.NewReader(buf))
			return resp, nil
		}
		return nil, nil
	}))
}

func uploadV1Backup(t *testing.T, ctx context.Context, localPath string, location backup.Location, h *backupTestHelper) []string {
	var uploadedFiles []string
	root := localPath
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		remotePath := location.RemotePath(strings.TrimPrefix(path, root))
		content, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		if err := h.client.RclonePut(ctx, ManagedClusterHost(), remotePath, bytes.NewReader(content), info.Size()); err != nil {
			return err
		}
		uploadedFiles = append(uploadedFiles, remotePath)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return uploadedFiles
}
