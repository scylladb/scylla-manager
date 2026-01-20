// Copyright (C) 2017 ScyllaDB

//go:build all || integration

package backup_test

import (
	"bytes"
	stdCmp "cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/netip"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/maps"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
	"go.uber.org/atomic"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testhelper"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func defaultTestProperties(loc backupspec.Location, ks string) map[string]any {
	properties := map[string]any{
		"location": []backupspec.Location{loc},
	}
	if ks != "" {
		properties["keyspace"] = []string{ks}
	}
	if BackupMethod() != nil && *BackupMethod() != "" {
		properties["method"] = *BackupMethod()
	}
	return properties
}

func defaultTestTarget(loc backupspec.Location, ks string, dc string, retention int) backup.Target {
	target := backup.Target{
		Units:     []backup.Unit{{Keyspace: ks}},
		DC:        []string{dc},
		Location:  []backupspec.Location{loc},
		Retention: retention,
	}
	if BackupMethod() != nil && *BackupMethod() != "" {
		target.Method = backup.Method(*BackupMethod())
	}
	return target
}

type backupTestHelper struct {
	*CommonTestHelper

	service  *backup.Service
	location backupspec.Location
}

func newBackupTestHelper(t *testing.T, session gocqlx.Session, config backup.Config, location backupspec.Location, clientConf *scyllaclient.Config) *backupTestHelper {
	return newBackupTestHelperWithUser(t, session, config, location, clientConf, "", "")
}

func newBackupTestHelperWithUser(t *testing.T, session gocqlx.Session, config backup.Config, location backupspec.Location, clientConf *scyllaclient.Config, user, pass string) *backupTestHelper {
	t.Helper()

	S3InitBucket(t, location.Path)

	clusterID := uuid.MustRandom()
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)

	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	client := newTestClient(t, hrt, logger.Named("client"), clientConf)
	service := newTestServiceWithUser(t, session, client, config, logger, clusterID, user, pass)
	cHelper := &CommonTestHelper{
		Session:   session,
		Hrt:       hrt,
		Client:    client,
		ClusterID: clusterID,
		TaskID:    uuid.MustRandom(),
		RunID:     uuid.NewTime(),
		T:         t,
	}

	for _, ip := range cHelper.GetAllHosts() {
		if err := client.RcloneResetStats(context.Background(), ip); err != nil {
			t.Error("Couldn't reset stats", ip, err)
		}
	}

	return &backupTestHelper{
		CommonTestHelper: cHelper,

		service:  service,
		location: location,
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

func newTestServiceWithUser(t *testing.T, session gocqlx.Session, client *scyllaclient.Client, c backup.Config, logger log.Logger, clusterID uuid.UUID, user, pass string) *backup.Service {
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
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			if user == "no-credentials" {
				return gocqlx.Session{}, cluster.ErrNoCQLCredentials
			}
			return CreateManagedClusterSession(t, false, client, user, pass), nil
		},
		func(ctx context.Context, clusterID uuid.UUID, host string) (*dynamodb.Client, error) {
			clusterSession := CreateManagedClusterSession(t, false, client, "", "")
			defer clusterSession.Close()
			accessKeyID, secretAccessKey := GetAlternatorCreds(t, clusterSession, user)
			return CreateAlternatorClient(t, client, host, accessKeyID, secretAccessKey), nil
		},
		NewTestConfigCacheSvc(t, clusterID, client.Config().Hosts),
		logger.Named("backup"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func defaultConfig() backup.Config {
	c := backup.DefaultConfig()
	c.DiskSpaceFreeMinPercent = 1
	return c
}

func (h *backupTestHelper) setInterceptorBlockPathOnFirstHost(paths ...string) {
	brokenHost := atomic.NewString("")
	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		for _, p := range paths {
			if strings.HasPrefix(req.URL.Path, p) {
				if brokenHost.CompareAndSwap("", req.Host) {
					h.T.Log("Setting broken host", req.Host)
				}
				if brokenHost.Load() == req.Host {
					return nil, errors.New("dial error")
				}
			}
		}
		return nil, nil
	}))
}

func (h *backupTestHelper) setInterceptorWaitPath(hosts []string, paths ...string) chan struct{} {
	wait := make(chan struct{})

	mu := sync.Mutex{}
	chanIsClosed := false
	missingHosts, err := maps.MapKeyWithError(maps.SetFromSlice(hosts), netip.ParseAddr)
	if err != nil {
		h.T.Error(errors.Wrap(err, "setInterceptorWaitPath: parse provided hosts IPs"))
	}

	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		for _, p := range paths {
			if strings.HasPrefix(req.URL.Path, p) {
				func() {
					mu.Lock()
					defer mu.Unlock()

					if chanIsClosed {
						return
					}

					ip, err := netip.ParseAddr(req.URL.Hostname())
					if err != nil {
						h.T.Error(errors.Wrap(err, "setInterceptorWaitPath: parse request host IP"))
					}

					delete(missingHosts, ip)
					if len(missingHosts) == 0 && !chanIsClosed {
						chanIsClosed = true
						close(wait)
					}
				}()
			}
		}
		return nil, nil
	}))
	return wait
}

func (h *backupTestHelper) listS3Files() (manifests, schemas, files []string) {
	h.T.Helper()
	opts := &scyllaclient.RcloneListDirOpts{
		Recurse:   true,
		FilesOnly: true,
	}
	allFiles, err := h.Client.RcloneListDir(context.Background(), ManagedClusterHost(), h.location.RemotePath(""), opts)
	if err != nil {
		h.T.Fatal(err)
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

func restartAgents(h *CommonTestHelper) {
	execOnAllHosts(h, "supervisorctl restart scylla-manager-agent")
}

func execOnAllHosts(h *CommonTestHelper, cmd string) {
	h.T.Helper()
	for _, host := range h.GetAllHosts() {
		stdout, stderr, err := ExecOnHost(host, cmd)
		if err != nil {
			h.T.Log("stdout", stdout)
			h.T.Log("stderr", stderr)
			h.T.Fatal("Command failed on host", host, err)
		}
	}
}

const (
	maxWaitCond               = time.Minute
	condCheckInterval         = 100 * time.Millisecond
	longPollingTimeoutSeconds = 2
)

func (h *backupTestHelper) waitCond(f func() bool) {
	WaitCond(h.T, f, condCheckInterval, maxWaitCond)
}

func (h *backupTestHelper) waitTransfersStarted() {
	h.waitCond(func() bool {
		h.T.Helper()
		for _, host := range h.GetAllHosts() {
			job, err := h.Client.RcloneJobInfo(context.Background(), host, scyllaclient.GlobalProgressID, longPollingTimeoutSeconds)
			if err != nil {
				h.T.Fatal(err)
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
		h.T.Helper()
		m, _, _ := h.listS3Files()
		return len(m) > 0
	})
}

func (h *backupTestHelper) waitNoTransfers() {
	h.waitCond(func() bool {
		h.T.Helper()
		for _, host := range h.GetAllHosts() {
			job, err := h.Client.RcloneJobInfo(context.Background(), host, scyllaclient.GlobalProgressID, longPollingTimeoutSeconds)
			if err != nil {
				h.T.Fatal(err)
			}
			if len(job.Stats.Transferring) > 0 {
				return false
			}
		}
		return true
	})
}

func (h *backupTestHelper) tamperWithManifest(ctx context.Context, manifestsPath string, f func(backupspec.ManifestInfoWithContent) bool) {
	h.T.Helper()

	// Parse manifest path
	m := backupspec.NewManifestInfoWithContent()
	if err := m.ParsePath(manifestsPath); err != nil {
		h.T.Fatal(err)
	}
	// Load manifest
	r, err := h.Client.RcloneOpen(ctx, ManagedClusterHost(), h.location.RemotePath(manifestsPath))
	if err != nil {
		h.T.Fatal(err)
	}
	if err := m.Read(r); err != nil {
		h.T.Fatal(err)
	}
	r.Close()

	// Since we rewrite the manifest we need the index loaded into the struct.
	if err := m.LoadIndex(); err != nil {
		h.T.Fatal(err)
	}

	// Decorate, if not changed return early
	if !f(m) {
		return
	}
	// Save modified manifest
	buf := bytes.NewBuffer(nil)
	if err = m.Write(buf); err != nil {
		h.T.Fatal(err)
	}
	if err := h.Client.RclonePut(ctx, ManagedClusterHost(), h.location.RemotePath(m.Path()), buf); err != nil {
		h.T.Fatal(err)
	}
}

func (h *backupTestHelper) touchFile(ctx context.Context, dir, file, content string) {
	h.T.Helper()
	buf := bytes.NewBufferString(content)
	if err := h.Client.RclonePut(ctx, ManagedClusterHost(), h.location.RemotePath(path.Join(dir, file)), buf); err != nil {
		h.T.Fatal(err)
	}
}

func s3Location(bucket string) backupspec.Location {
	return backupspec.Location{
		Provider: backupspec.S3,
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
		h       = newBackupTestHelper(t, session, defaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateSessionAndDropAllKeyspaces(t, h.Client).Close()
	S3InitBucket(t, testBucket)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			b, err := os.ReadFile(test.Input)
			if err != nil {
				t.Fatal(err)
			}
			v, err := h.service.GetTarget(ctx, h.ClusterID, b)
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
			golden.RemoveSystemTables()
			v.RemoveSystemTables()
			if diff := cmp.Diff(golden, v,
				cmpopts.SortSlices(func(a, b string) bool { return a < b }),
				cmpopts.IgnoreUnexported(backup.Target{}),
				cmpopts.IgnoreSliceElements(func(u backup.Unit) bool { return u.Keyspace == "system_replicated_keys" || u.Keyspace == "system_auth" }),
				cmpopts.IgnoreSliceElements(func(t string) bool { return t == "dicts" })); diff != "" {
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
			Error: `invalid location: no such datacenter foobar`,
		},
		{
			Name:  "no location for dc",
			JSON:  `{"location": ["dc1:s3:backuptest-get-target"]}`,
			Error: "invalid location: missing object for datacenter",
		},
		{
			Name:  "no location dc filtered out",
			JSON:  `{"dc": ["dc2"], "location": ["dc1:s3:backuptest-get-target"]}`,
			Error: "invalid location: missing object for datacenter",
		},
		{
			Name:  "inaccessible location",
			JSON:  `{"location": ["s3:foo", "dc1:s3:bar"]}`,
			Error: "specified bucket does not exist",
		},
		{
			Name:  "invalid rate limit dc",
			JSON:  `{"rate_limit": ["foobar:100"], "location": ["s3:backuptest-get-target"]}`,
			Error: `invalid rate-limit: no such datacenter foobar`,
		},
		{
			Name:  "invalid snapshot parallel dc",
			JSON:  `{"snapshot_parallel": ["foobar:100"], "location": ["s3:backuptest-get-target"]}`,
			Error: `invalid snapshot-parallel: no such datacenter foobar`,
		},
		{
			Name:  "invalid upload parallel dc",
			JSON:  `{"upload_parallel": ["foobar:100"], "location": ["s3:backuptest-get-target"]}`,
			Error: `invalid upload-parallel: no such datacenter foobar`,
		},
	}

	const testBucket = "backuptest-get-target-error"

	var (
		session = CreateSessionWithoutMigration(t)
		h       = newBackupTestHelper(t, session, defaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateSessionAndDropAllKeyspaces(t, h.Client).Close()
	S3InitBucket(t, testBucket)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			_, err := h.service.GetTarget(ctx, h.ClusterID, json.RawMessage(test.JSON))
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

	config := defaultConfig()

	var (
		session = CreateScyllaManagerDBSession(t)
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
		if !errors.Is(err, util.ErrNotFound) {
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
		if !errors.Is(err, util.ErrNotFound) {
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
		testBucket   = "backuptest-smoke"
		testKeyspace = "backuptest_data"
	)

	location := s3Location(testBucket)
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	target := defaultTestTarget(location, testKeyspace, "dc1", 3)
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	// Mocking /cloud/metadata endpoint as it's not expected to work reliably on ci.
	// But with mock we can at least check that response is used correctly and saved to manifest file.
	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/agent/cloud/metadata" {
			return nil, nil
		}

		metaMock := models.InstanceMetadata{
			CloudProvider: "test_provider",
			InstanceType:  "test_instance_type",
		}

		metaMockBytes, err := json.Marshal(metaMock)
		if err != nil {
			t.Fatal(err)
		}
		resp := httpx.MakeResponse(req, http.StatusOK)
		resp.Body = io.NopCloser(bytes.NewReader(metaMockBytes))
		return resp, nil
	}))

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}
	Print("And: run it again")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: there are two backups")
	items, err := h.service.List(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID})
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

	for _, host := range h.GetAllHosts() {
		s, err := h.Client.Snapshots(ctx, host)
		if err != nil {
			t.Fatal("Snapshots() error", err)
		}
		if len(s) > 0 {
			t.Fatalf("Found snapshots %s on host %s", s, host)
		}
	}

	ni, err := h.Client.AnyNodeInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}

	Print("And: files")
	manifests, schemas, _ := h.listS3Files()
	// Manifest meta per host per snapshot
	expectedNumberOfManifests := 3 * 2
	if len(manifests) != expectedNumberOfManifests {
		t.Fatalf("expected %d manifests, got %d", expectedNumberOfManifests, len(manifests))
	}

	// Schema meta per snapshot
	var schemasCount = 2
	if ok, err := ni.SupportsAlternatorSchemaBackupFromAPI(); err != nil {
		t.Fatal(err)
	} else if ok {
		// Alternator schema per snapshot
		schemasCount *= 2
	}
	if len(schemas) != schemasCount {
		t.Fatalf("expected %d schemas, got %d", schemasCount, len(schemas))
	}

	filesInfo, err := h.service.ListFiles(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID})
	if err != nil {
		t.Fatal("ListFiles() error", err)
	}
	if len(filesInfo) != 6 {
		t.Fatalf("len(ListFiles()) = %d, expected %d", len(filesInfo), len(manifests))
	}
	for _, fi := range filesInfo {
		for _, fs := range fi.Files {
			remoteFiles, err := h.Client.RcloneListDir(ctx, h.GetAllHosts()[0], h.location.RemotePath(fs.Path), nil)
			if err != nil {
				t.Fatal("RcloneListDir() error", err)
			}

			var remoteFileNames []string
			for _, f := range remoteFiles {
				remoteFileNames = append(remoteFileNames, f.Name)
			}

			Print("And: Scylla manifests are not uploaded")
			for _, rfn := range remoteFileNames {
				if strings.Contains(rfn, backupspec.ScyllaManifest) {
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
		var m backupspec.ManifestInfo
		if err := m.ParsePath(s); err != nil {
			t.Fatal("manifest file in wrong path", s)
		}
	}

	Print("When: write new data")
	// 200M is the multipart threshold we want to trigger it in the smoke test.
	WriteData(t, clusterSession, testKeyspace, 210)

	Print("And: run it again")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: there are three backups")
	items, err = h.service.List(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID})
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
	for _, host := range h.GetAllHosts() {
		job, err := h.Client.RcloneJobInfo(context.Background(), host, scyllaclient.GlobalProgressID, longPollingTimeoutSeconds)
		if err != nil {
			h.T.Fatal(err)
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
	filesInfo, err = h.service.ListFiles(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID})
	if err != nil {
		t.Fatal("ListFiles() error", err)
	}

	// 3 backups * 3 nodes
	if len(filesInfo) != 3*3 {
		t.Fatalf("len(ListFiles()) = %d, expected %d", len(filesInfo), 3*3)
	}

	filesInfo, err = h.service.ListFiles(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{
		ClusterID: h.ClusterID,
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
	r, err := h.Client.RcloneOpen(ctx, ManagedClusterHost(), h.location.RemotePath(manifestPath))
	if err != nil {
		t.Fatal(err)
	}
	var mc backupspec.ManifestContentWithIndex
	if err := mc.Read(r); err != nil {
		t.Fatalf("Cannot read manifest created by backup: %s", err)
	}
	r.Close()

	if mc.ClusterName != "test_cluster" {
		t.Errorf("ClusterName=%s, expected test_cluster", mc.ClusterName)
	}
	if !IsPublicIP(mc.IP) {
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

	var infoFromPath backupspec.ManifestInfo
	if err := infoFromPath.ParsePath(manifestPath); err != nil {
		t.Fatal("manifest file in wrong path", manifestPath)
	}

	if mc.DC != infoFromPath.DC {
		t.Errorf("DC=%s, expected %s", mc.DC, infoFromPath.DC)
	}

	if mc.ClusterID.String() != infoFromPath.ClusterID.String() {
		t.Errorf("ClustedID=%s, expected %s", mc.ClusterID, infoFromPath.ClusterID)
	}

	if mc.NodeID != infoFromPath.NodeID {
		t.Errorf("NodeID=%s, expected %s", mc.NodeID, infoFromPath.NodeID)
	}

	if mc.TaskID.String() != infoFromPath.TaskID.String() {
		t.Errorf("TaskID=%s, expected %s", mc.TaskID, infoFromPath.TaskID)
	}

	if mc.SnapshotTag != infoFromPath.SnapshotTag {
		t.Errorf("SnapshotTag=%s, expected %s", mc.SnapshotTag, infoFromPath.SnapshotTag)
	}

	if mc.Rack == "" {
		t.Error(`Rack="", expected != ""`, mc.Rack)
	}

	if mc.ShardCount == 0 {
		t.Errorf("ShardCount=0, expected > 0")
	}

	if mc.CPUCount == 0 {
		t.Errorf("CPUCount=0, expected > 0")
	}

	if mc.StorageSize == 0 {
		t.Errorf("StorageSize=0, expected > 0")
	}

	if mc.InstanceDetails.InstanceType != "test_instance_type" {
		t.Errorf("InstanceDetails.InstanceType=%s, expected test_instance_type", mc.InstanceDetails.InstanceType)
	}

	if mc.InstanceDetails.CloudProvider != "test_provider" {
		t.Errorf("InstanceDetails.CloudProvider=%s, expected aws", mc.InstanceDetails.CloudProvider)
	}
}

func TestBackupWithNodesDownIntegration(t *testing.T) {
	if IsIPV6Network() {
		t.Skip("DB node do not have ip6tables and related modules to make it work properly")
	}
	const (
		testBucket   = "backuptest-nodesdown"
		testKeyspace = "backuptest_data"
	)

	location := s3Location(testBucket)
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	Print("Given: downed node")
	if err := RunIptablesCommand(t, IPFromTestNet("11"), CmdBlockScyllaREST); err != nil {
		t.Fatal(err)
	}
	defer RunIptablesCommand(t, IPFromTestNet("11"), CmdUnblockScyllaREST)

	Print("When: get target")
	target := defaultTestTarget(location, testKeyspace, "dc1", 3)

	Print("Then: target hosts does not contain the downed node")
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}
	if len(target.Hosts()) != 2 {
		t.Fatal(target.Hosts())
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
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

func assertMaxProgress(t *testing.T, pr backup.Progress) {
	msg := "expected all bytes to be uploaded"
	if _, left := pr.ByteProgress(); left != 0 {
		t.Fatal(msg, pr)
	}
	for _, hpr := range pr.Hosts {
		if _, left := hpr.ByteProgress(); left != 0 {
			t.Fatal(msg, hpr)
		}
		for _, kpr := range hpr.Keyspaces {
			if _, left := kpr.ByteProgress(); left != 0 {
				t.Fatal(msg, kpr)
			}
			for _, tpr := range kpr.Tables {
				if _, left := tpr.ByteProgress(); left != 0 {
					t.Fatal(msg, tpr)
				}
			}
		}
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
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 3)

	target := defaultTestTarget(location, testKeyspace, "dc1", 2)
	target.RateLimit = []backup.DCLimit{{"dc1", 1}}
	target.Continue = true

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

	t.Run("resume after stop", func(t *testing.T) {
		var (
			h           = newBackupTestHelper(t, session, config, location, nil)
			ctx, cancel = context.WithCancel(context.Background())
			done        = make(chan struct{})
		)

		upload := h.setInterceptorWaitPath(h.GetHostsFromDC("dc1"), "/task_manager/wait_task", "/agent/rclone/job/progress")

		if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
			t.Fatal(err)
		}

		go func() {
			defer close(done)
			Print("When: backup is running")
			err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target)
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Expected %q error but got: %q", context.Canceled, err)
			}
		}()

		select {
		case <-time.After(backupTimeout):
			t.Fatalf("Backup failed to complete in under %s", backupTimeout)
		case <-upload:
			Print("And: context is canceled")
			cancel()
			<-ctx.Done()
		}

		select {
		case <-time.After(backupTimeout):
			t.Fatalf("Backup failed to complete in under %s", backupTimeout)
		case <-done:
			Print("Then: backup completed execution")
		}

		Print("And: nothing is transferring")
		h.waitNoTransfers()

		Print("When: backup is resumed with new RunID")
		err := h.service.Backup(context.Background(), h.ClusterID, h.TaskID, uuid.NewTime(), target)
		if err != nil {
			t.Error("Unexpected error", err)
		}

		Print("Then: data is uploaded")
		assertDataUploaded(t, h)

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})

	t.Run("resume after agent restart", func(t *testing.T) {
		clientConf := scyllaclient.TestConfig(h.GetAllHosts(), AgentAuthToken())
		clientConf.Backoff.MaxRetries = 5

		var (
			h           = newBackupTestHelper(t, session, config, location, &clientConf)
			ctx, cancel = context.WithCancel(context.Background())
			done        = make(chan struct{})
		)
		defer cancel()

		upload := h.setInterceptorWaitPath(h.GetHostsFromDC("dc1"), "/task_manager/wait_task", "/agent/rclone/job/progress")

		if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
			t.Fatal(err)
		}

		go func() {
			Print("When: backup is running")
			err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target)
			if err != nil {
				t.Errorf("Expected no error but got %+v", err)
			}
			close(done)
		}()

		select {
		case <-time.After(backupTimeout * 3):
			t.Fatalf("Backup failed to complete in under %s", backupTimeout*3)
		case <-upload:
			Print("And: we restart the agents")
			restartAgents(h.CommonTestHelper)
		}

		select {
		case <-time.After(backupTimeout * 3):
			t.Fatalf("Backup failed to complete in under %s", backupTimeout*3)
		case <-done:
		}

		Print("And: nothing is transferring")
		h.waitNoTransfers()

		Print("And: data is uploaded")
		assertDataUploaded(t, h)
	})

	t.Run("resume after snapshot failed", func(t *testing.T) {
		h := newBackupTestHelper(t, session, config, location, nil)
		Print("Given: snapshot fails on a host")
		h.setInterceptorBlockPathOnFirstHost("/storage_service/snapshots")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
			t.Fatal(err)
		}

		Print("When: run backup")
		err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target)

		Print("Then: it fails")
		if err == nil {
			t.Error("Expected error on run but got nil")
		}
		t.Log("Backup() error", err)

		Print("Given: snapshot not longer fails on a host")
		h.Hrt.SetInterceptor(nil)

		Print("When: backup is resumed with new RunID")
		tag := backup.NewSnapshotTag()
		err = h.service.Backup(context.Background(), h.ClusterID, h.TaskID, uuid.NewTime(), target)
		if err != nil {
			t.Error("Unexpected error", err)
		}

		Print("Then: data is uploaded")
		assertDataUploadedAfterTag(t, h, tag)

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})

	t.Run("resume after upload failed", func(t *testing.T) {
		h := newBackupTestHelper(t, session, config, location, nil)
		Print("Given: upload fails on a host")
		h.setInterceptorBlockPathOnFirstHost("/agent/rclone/job/progress", "/task_manager/wait_task")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
			t.Fatal(err)
		}

		Print("When: run backup")
		err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target)

		Print("Then: it fails")
		if err == nil {
			t.Error("Expected error on run but got nil")
		}
		t.Log("Backup() error", err)

		Print("Given: upload not longer fails on a host")
		h.Hrt.SetInterceptor(nil)

		Print("When: backup is resumed with new RunID")
		runID := uuid.NewTime()
		err = h.service.Backup(context.Background(), h.ClusterID, h.TaskID, runID, target)
		if err != nil {
			t.Error("Unexpected error", err)
		}

		Print("Then: data is uploaded")
		pr, err := h.service.GetProgress(context.Background(), h.ClusterID, h.TaskID, runID)
		if err != nil {
			t.Error(err)
		}
		assertMaxProgress(t, pr)

		Print("And: nothing is transferring")
		h.waitNoTransfers()
	})

	t.Run("continue false", func(t *testing.T) {
		var (
			h           = newBackupTestHelper(t, session, config, location, nil)
			ctx, cancel = context.WithCancel(context.Background())
			done        = make(chan struct{})
		)

		h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.HasPrefix(req.URL.Path, "/agent/rclone/operations/put") {
				Print("And: context is canceled")
				cancel()
			}

			return nil, nil
		}))

		target := target
		target.Continue = false
		if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
			t.Fatal(err)
		}

		go func() {
			defer close(done)
			Print("When: backup is running")
			err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target)
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
		tag := backup.NewSnapshotTag()
		err := h.service.Backup(context.Background(), h.ClusterID, h.TaskID, uuid.NewTime(), target)
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
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	Print("Given: retention policy of 1")
	target := defaultTestTarget(location, testKeyspace, "dc1", 1)
	target.RetentionMap = backup.RetentionMap{h.TaskID: {RetentionDays: 0, Retention: 1}}
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}

	Print("And: add a fake temporary manifest")
	manifests, _, _ := h.listS3Files()

	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		// Mark manifest as temporary, change snapshot tag
		m.Temporary = true
		m.SnapshotTag = backup.NewSnapshotTag()
		// Add "xxx" file to a table
		fi := &m.Index[0]
		fi.Files = append(fi.Files, "xxx")

		// Create the "xxx" file
		h.touchFile(ctx, path.Join(backupspec.RemoteSSTableVersionDir(h.ClusterID, m.DC, m.NodeID, fi.Keyspace, fi.Table, fi.Version)), "xxx", "xxx")

		return true
	})

	Print("Then: there is one backup in listing")
	items, err := h.service.List(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("List() = %v, expected one item", items)
	}

	Print("When: run backup")
	h.RunID = uuid.NewTime()
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
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
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 3)

	target := defaultTestTarget(location, testKeyspace, "dc1", 3)
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	movedManifests := atomic.NewInt64(0)
	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
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
	err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target)
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
		if strings.HasSuffix(m, backupspec.TempFileExt) {
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
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	Print("Given: retention policy of 1")
	target := defaultTestTarget(location, testKeyspace, "dc1", 1)
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("And: interceptor removing tmp suffix from manifests")
	h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
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
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
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
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 3)

	task1 := h.TaskID
	task2 := uuid.MustRandom()
	task3 := uuid.MustRandom()

	Print("Given: retention policy 1")
	target := defaultTestTarget(location, testKeyspace, "dc1", 1)
	target.RetentionMap = map[uuid.UUID]backup.RetentionPolicy{
		task1: {RetentionDays: 7, Retention: 1},
		task2: {RetentionDays: 7, Retention: 1},
		task3: {RetentionDays: 2, Retention: 7},
	}
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}

	// Get manifest prototype
	manifests, _, _ := h.listS3Files()
	if len(manifests) != 3 {
		t.Fatalf("Expected manifest per node, got %d", len(manifests))
	}
	now := timeutc.Now()

	Print("And: add manifest for removed node - should be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.NodeID = uuid.MustRandom().String()
		return true
	})
	Print("And: add manifest for task2 - should NOT be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.TaskID = task2
		m.SnapshotTag = backupspec.SnapshotTagAt(now.AddDate(0, 0, -1))
		return true
	})
	Print("And: add another manifest for task2 - should be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.TaskID = task2
		m.SnapshotTag = backupspec.SnapshotTagAt(now.AddDate(0, 0, -2))
		return true
	})
	Print("And: add 2 hour old manifest for task 3 - should NOT be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.TaskID = task3
		m.SnapshotTag = backupspec.SnapshotTagAt(now.Add(time.Hour * -2))
		return true
	})
	Print("And: add 1 day old manifest for task 3 - should NOT be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.TaskID = task3
		m.SnapshotTag = backupspec.SnapshotTagAt(now.Add(time.Hour * -26))
		return true
	})
	Print("And: add 3 day old manifest for task 3 - should be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.TaskID = task3
		m.SnapshotTag = backupspec.SnapshotTagAt(now.AddDate(0, 0, -3))
		return true
	})
	Print("And: add manifest for removed old task - should be removed")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.TaskID = uuid.MustRandom()
		m.SnapshotTag = backupspec.SnapshotTagAt(now.AddDate(-1, 0, 0))
		return true
	})
	Print("And: add manifest for removed task - should NOT removed")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.TaskID = uuid.MustRandom()
		m.SnapshotTag = backupspec.SnapshotTagAt(now.AddDate(0, 0, 3))
		return true
	})

	WriteData(t, clusterSession, testKeyspace, 3)

	Print("And: run backup again")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
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
		r, err := h.Client.RcloneOpen(ctx, ManagedClusterHost(), location.RemotePath(m))
		if err != nil {
			t.Fatal(err)
		}
		var c backupspec.ManifestContentWithIndex
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
			if strings.HasPrefix(path.Base(f), pfx) || strings.HasSuffix(f, backupspec.MetadataVersion) {
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
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 3)

	Print("Given: retention policy 2")
	target := defaultTestTarget(location, testKeyspace, "dc1", 2)
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}

	Print("And: add temporary manifest")
	manifests, _, _ := h.listS3Files()
	if len(manifests) != 3 {
		t.Fatalf("Expected manifest per node, got %d", len(manifests))
	}
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.NodeID = uuid.MustRandom().String()
		m.Temporary = true
		return true
	})

	Print("And: run backup again")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
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
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	Print("Given: retention policy of 1 for the both task")
	target := defaultTestTarget(location, testKeyspace, "dc1", 1)

	Print("Given: given same data in shared keyspace")
	WriteData(t, clusterSession, testKeyspace, 3)

	Print("When: both tasks backup same data")
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewTime()
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, runID, target); err != nil {
		t.Fatal(err)
	}

	task2ID := uuid.NewTime()
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	runID = uuid.NewTime()
	if err := h.service.Backup(ctx, h.ClusterID, task2ID, runID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: both tasks references same data")

	firstTaskFilePaths := taskFiles(t, ctx, h, h.TaskID)
	secondTaskFilePaths := taskFiles(t, ctx, h, task2ID)

	filesSymmetricDifference := strset.New(firstTaskFilePaths...)
	filesSymmetricDifference.Separate(strset.New(secondTaskFilePaths...))
	if !filesSymmetricDifference.IsEmpty() {
		t.Fatal("Expected to have same SST files in both tasks")
	}

	Print("When: first task snapshot is deleted")

	firstTaskTags := taskTags(t, ctx, h, h.TaskID)
	if firstTaskTags.Size() != 1 {
		t.Fatalf("Expected to have single snapshot in the first task, got %d", firstTaskTags.Size())
	}

	if err := h.service.DeleteSnapshot(ctx, h.ClusterID, []backupspec.Location{h.location}, []string{firstTaskTags.Pop()}); err != nil {
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

	if err := h.service.DeleteSnapshot(ctx, h.ClusterID, []backupspec.Location{h.location}, []string{secondTaskTags.Pop()}); err != nil {
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
	backups, err := h.service.List(ctx, h.ClusterID, []backupspec.Location{h.location},
		backup.ListFilter{ClusterID: h.ClusterID, TaskID: taskID})
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

	filesFilter := backup.ListFilter{ClusterID: h.ClusterID, TaskID: taskID}
	taskFiles, err := h.service.ListFiles(ctx, h.ClusterID, []backupspec.Location{h.location}, filesFilter)
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
		if !strings.HasSuffix(f, backupspec.MetadataVersion) {
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
		h       = newBackupTestHelper(t, session, defaultConfig(), s3Location(testBucket), nil)
		ctx     = context.Background()
	)

	CreateSessionAndDropAllKeyspaces(t, h.Client).Close()
	S3InitBucket(t, testBucket)

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			_, err := h.service.GetValidationTarget(ctx, h.ClusterID, json.RawMessage(test.JSON))
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
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 1)

	target := backup.Target{
		Units: []backup.Unit{
			{
				Keyspace: testKeyspace,
			},
		},
		DC:        []string{"dc1"},
		Location:  []backupspec.Location{location},
		Retention: 3,
	}
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
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
		err = h.service.Validate(ctx, h.ClusterID, validateTaskID, validateRunID, validationTarget)
		progress, _ = h.service.GetValidationProgress(ctx, h.ClusterID, validateTaskID, validateRunID)
		return
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}

	manifests, _, files := h.listS3Files()

	genTag := func() string {
		return backupspec.SnapshotTagAt(time.Unix(int64(rand.Uint32()), 0))
	}

	var (
		orphanedSnapshotTag = genTag()
		alienSnapshotTag    = genTag()
		tamperedSnapshotTag = genTag()
	)

	Print("And: add orphaned file - should be reported and deleted")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.SnapshotTag = orphanedSnapshotTag
		h.touchFile(ctx, path.Join(backupspec.RemoteSSTableVersionDir(h.ClusterID, m.DC, m.NodeID, "foo", "bar", "f0e76f40662e11ebbe97000000000001")), "xx0", "xxx")
		return false
	})

	Print("And: copy manifest to a different nodeID - should be reported as broken")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.SnapshotTag = alienSnapshotTag
		m.NodeID = uuid.MustRandom().String()
		m.IP = "1.2.3.4"
		return true
	})

	Print("And: add file referenced by temporary manifest - should NOT be reported nor deleted")
	h.tamperWithManifest(ctx, manifests[0], func(m backupspec.ManifestInfoWithContent) bool {
		m.SnapshotTag = genTag()
		m.Temporary = true
		fi := &m.Index[0]
		fi.Files = append(fi.Files, "xx1")
		h.touchFile(ctx, path.Join(backupspec.RemoteSSTableVersionDir(h.ClusterID, m.DC, m.NodeID, fi.Keyspace, fi.Table, fi.Version)), "xx1", "xxx")
		return true
	})

	Print("And: add tampered manifest - should be reported as broken snapshot")
	h.tamperWithManifest(ctx, manifests[1], func(m backupspec.ManifestInfoWithContent) bool {
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

func TestBackupListIntegration(t *testing.T) {
	const (
		testBucket    = "backuptest-list"
		testKeyspace1 = "backuptest_list1"
		testKeyspace2 = "backuptest_list2"
	)

	location := s3Location(testBucket)
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace1, 1)
	WriteData(t, clusterSession, testKeyspace2, 1)

	timeBeforeFirstBackup := timeutc.Now().Add(-time.Second)

	target := defaultTestTarget(location, testKeyspace1, "dc1", 3)
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup first keyspace")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}
	//  Sleep to avoid inconsistent filtering
	time.Sleep(time.Second)
	timeBetweenBackups := timeutc.Now()
	time.Sleep(time.Second)

	target = defaultTestTarget(location, testKeyspace2, "dc2", 3)
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup with second keyspace")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}

	timeAfterSecondBackup := timeutc.Now().Add(time.Second)

	testCases := []struct {
		name     string
		filter   backup.ListFilter
		expected int
	}{
		{
			name:     "no filter",
			filter:   backup.ListFilter{},
			expected: 2,
		},
		{
			name:     "cluster ID",
			filter:   backup.ListFilter{ClusterID: h.ClusterID},
			expected: 2,
		},
		{
			name:     "first keyspace",
			filter:   backup.ListFilter{Keyspace: []string{testKeyspace1}},
			expected: 1,
		},
		{
			name:     "both keyspaces",
			filter:   backup.ListFilter{Keyspace: []string{testKeyspace1, testKeyspace2}},
			expected: 2,
		},
		{
			name:     "non-existing keyspace",
			filter:   backup.ListFilter{Keyspace: []string{"non-existing"}},
			expected: 0,
		},
		{
			name:     "min date",
			filter:   backup.ListFilter{MinDate: timeBetweenBackups},
			expected: 1,
		},
		{
			name:     "max date",
			filter:   backup.ListFilter{MaxDate: timeBetweenBackups},
			expected: 1,
		},
		{
			name:     "min and max date",
			filter:   backup.ListFilter{MinDate: timeBeforeFirstBackup, MaxDate: timeAfterSecondBackup},
			expected: 2,
		},
		{
			name: "first backup properties",
			filter: backup.ListFilter{
				ClusterID: h.ClusterID,
				Keyspace:  []string{testKeyspace1},
				MinDate:   timeBeforeFirstBackup,
				MaxDate:   timeBetweenBackups,
			},
			expected: 1,
		},
		{
			name: "mixed properties",
			filter: backup.ListFilter{
				ClusterID: h.ClusterID,
				Keyspace:  []string{testKeyspace2},
				MinDate:   timeBeforeFirstBackup,
				MaxDate:   timeBetweenBackups,
			},
			expected: 0,
		},
	}

	for i := 0; i < len(testCases); i++ {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			items, err := h.service.List(ctx, h.ClusterID, []backupspec.Location{location}, tc.filter)
			if err != nil {
				t.Fatal(err)
			}

			if tc.expected == 0 {
				if len(items) != 0 {
					t.Fatalf("List() = %v, expected zero items", items)
				}
				return
			}

			if len(items) != 1 {
				t.Fatalf("List() = %v, expected one item", items)
			}
			info := items[0]
			if len(info.SnapshotInfo) != tc.expected {
				t.Fatalf("List() = %v, expected %d SnapshotTags", items, tc.expected)
			}
		})
	}
}

func TestBackupAlternatorIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-alternator"
		testTable    = "Tab_le-With1.da_sh2-aNd.d33ot.-"
		testKeyspace = "alternator_" + testTable
		rowCnt       = 100
	)

	location := s3Location(testBucket)
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	ni, err := h.Client.AnyNodeInfo(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	accessKeyID, secretAccessKey := GetAlternatorCreds(t, clusterSession, "")
	client := CreateAlternatorClient(t, h.Client, ManagedClusterHost(), accessKeyID, secretAccessKey)
	CreateAlternatorTable(t, client, ni, "", 0, 0, testTable)
	InsertAlternatorTableData(t, client, rowCnt, testTable)

	Print("When: validate data insertion")
	selectStmt := fmt.Sprintf("SELECT COUNT(*) FROM %q.%q", testKeyspace, testTable)
	var result int
	if err := clusterSession.Query(selectStmt, nil).Scan(&result); err != nil {
		t.Fatal(err)
	}
	if result != rowCnt {
		t.Fatal("Expected 1 row in alternator table")
	}

	target := defaultTestTarget(location, testKeyspace, "", 3)
	target.DC = []string{"dc1", "dc2"}
	if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
		t.Fatal(err)
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}

	Print("And: validate snapshot creation")
	items, err := h.service.List(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatal("Expected 1 item")
	}
	if len(items[0].SnapshotInfo) != 1 {
		t.Fatal("Expected 1 snapshot tag")
	}

	Print("And: validate manifest creation")
	manifests, _, _ := h.listS3Files()
	if len(manifests) != len(ManagedClusterHosts()) {
		t.Fatalf("expected manifest for each node, got %d", len(manifests))
	}
	for _, s := range manifests {
		var m backupspec.ManifestInfo
		if err := m.ParsePath(s); err != nil {
			t.Fatal("manifest file with wrong path", s)
		}
	}

	Print("And: validate files creation")
	filesInfo, err := h.service.ListFiles(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID})
	if err != nil {
		t.Fatal(err)
	}
	if len(filesInfo) != len(manifests) {
		t.Fatalf("Expected file info for each manifest, got %d", len(filesInfo))
	}
	for _, fi := range filesInfo {
		if len(fi.Files) == 0 {
			t.Fatal("Expected some files to be created")
		}
		for _, fs := range fi.Files {
			remoteFiles, err := h.Client.RcloneListDir(ctx, ManagedClusterHost(), h.location.RemotePath(fs.Path), nil)
			if err != nil {
				t.Fatal(err)
			}

			var remoteFileNames []string
			for _, f := range remoteFiles {
				remoteFileNames = append(remoteFileNames, f.Name)
			}

			for _, rfn := range remoteFileNames {
				if strings.Contains(rfn, backupspec.ScyllaManifest) {
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
}

func TestBackupViewsIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-views"
		testKeyspace = "backuptest_views"
		testTable    = "base_table"
		testMV       = "mv_table"
		testSI       = "si_table"
	)

	location := s3Location(testBucket)
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	Print("Given: table with MV and SI")
	WriteData(t, clusterSession, testKeyspace, 1, testTable)

	// It's not possible to create views on tablet keyspaces
	rd := scyllaclient.NewRingDescriber(context.Background(), h.Client)
	if rd.IsTabletKeyspace(testKeyspace) {
		t.Skip("Test expects to create views, but it's not possible for tablet keyspaces")
	}

	CreateMaterializedView(t, clusterSession, testKeyspace, testTable, testMV)
	CreateSecondaryIndex(t, clusterSession, testKeyspace, testTable, testSI)

	props := defaultTestProperties(location, testKeyspace)
	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create raw properties"))
	}

	Print("When: create backup target")
	target, err := h.service.GetTarget(ctx, h.ClusterID, rawProps)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create target"))
	}

	Print("Then: target consists of only base table")
	expected := []backup.Unit{
		{
			Keyspace: testKeyspace,
			Tables:   []string{testTable},
		},
	}
	if diff := cmp.Diff(target.Units, expected,
		cmpopts.IgnoreFields(backup.Unit{}, "AllTables"),
		cmpopts.IgnoreSliceElements(func(u backup.Unit) bool { return u.Keyspace == "system_schema" }),
	); diff != "" {
		t.Fatal("target units are not as expected", diff)
	}

	Print("When: run backup")
	if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(errors.Wrap(err, "run backup"))
	}

	Print("And: list backup files")
	filesInfo, err := h.service.ListFiles(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID})
	if err != nil {
		t.Fatal(errors.Wrap(err, "list backup files"))
	}

	Print("Then: backup files don't contain views")
	var foundTable int
	for _, fi := range filesInfo {
		for _, fm := range fi.Files {
			if fm.Keyspace == testKeyspace {
				if fm.Table == testTable {
					foundTable++
				}
				if fm.Table == testSI || fm.Table == testMV {
					t.Fatal("Found view in backup files")
				}
			}
		}
	}

	Print("And: backup files contain base table")
	if foundTable != len(ManagedClusterHosts()) {
		t.Fatal("Expected all hosts to back up base table")
	}
}

func TestBackupLWTIntegration(t *testing.T) {
	location := s3Location("backuptest-lwt")
	config := defaultConfig()

	var (
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelper(t, session, config, location, nil)
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	ni, err := h.Client.AnyNodeInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if CheckConstraint(t, ni.ScyllaVersion, "< 2025.1") {
		t.Skip("Test expects that it's possible to create table with tablets")
	}

	Print("Given: CQL vnode table with LWT")
	const (
		cqlVnodeKs  = "cql_vnode_ks"
		cqlVnodeTab = "cql_vnode_tab"
	)
	ExecStmt(t, clusterSession, fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %q WITH "+
		"replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND "+
		"tablets = {'enabled': 'false'}", cqlVnodeKs))
	WriteData(t, clusterSession, cqlVnodeKs, 1, cqlVnodeTab)
	ExecuteLWTCQLQuery(t, clusterSession, cqlVnodeKs, cqlVnodeTab)

	Print("And: CQL tablet table with LWT")
	const (
		cqlTabletKs  = "cql_tablet_ks"
		cqlTabletTab = "cql_tablet_tab"
	)
	ExecStmt(t, clusterSession, fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %q WITH "+
		"replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND "+
		"tablets = {'enabled': 'true'}", cqlTabletKs))
	WriteData(t, clusterSession, cqlTabletKs, 1, cqlTabletTab)
	// LWT on tablets is enabled starting from 2025.4
	if CheckConstraint(t, ni.ScyllaVersion, ">= 2025.4") {
		ExecuteLWTCQLQuery(t, clusterSession, cqlTabletKs, cqlTabletTab)
	}

	Print("And: Alternator vnode table with LWT")
	accessKeyID, secretAccessKey := GetAlternatorCreds(t, clusterSession, "")
	altClient := CreateAlternatorClient(t, h.Client, ManagedClusterHost(), accessKeyID, secretAccessKey)
	const (
		altVnodeTab = "alt_vnode_tab"
		altVnodeKs  = "alternator_" + altVnodeTab
	)
	CreateAlternatorTable(t, altClient, ni, "none", 0, 0, altVnodeTab)
	InsertAlternatorTableData(t, altClient, 100, altVnodeTab)
	ExecuteLWTAlternatorQuery(t, altClient, altVnodeTab)

	Print("And: Alternator tablet table with LWT")
	const (
		altTabletTab = "alt_tablet_tab"
		altTabletKs  = "alternator_" + altTabletTab
	)
	CreateAlternatorTable(t, altClient, ni, "8", 0, 0, altTabletTab)
	InsertAlternatorTableData(t, altClient, 100, altTabletTab)
	if CheckConstraint(t, ni.ScyllaVersion, ">= 2025.4") {
		ExecuteLWTAlternatorQuery(t, altClient, altTabletTab)
	}

	Print("When: create backup target")
	props := defaultTestProperties(location, "")
	props["keyspace"] = []string{cqlVnodeKs, cqlTabletKs, altVnodeKs, altTabletKs}
	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(err)
	}
	target, err := h.service.GetTarget(ctx, h.ClusterID, rawProps)
	if err != nil {
		t.Fatal(err)
	}

	Print("Then: target contains just the base table and system_schema")
	expected := []backup.Unit{
		{
			Keyspace: cqlVnodeKs,
			Tables:   []string{cqlVnodeTab},
		},
		{
			Keyspace: cqlTabletKs,
			Tables:   []string{cqlTabletTab},
		},
		{
			Keyspace: altVnodeKs,
			Tables:   []string{altVnodeTab},
		},
		{
			Keyspace: altTabletKs,
			Tables:   []string{altTabletTab},
		},
	}
	if diff := cmp.Diff(target.Units, expected,
		cmpopts.IgnoreFields(backup.Unit{}, "AllTables"),
		cmpopts.SortSlices(func(a, b backup.Unit) bool { return a.Keyspace < b.Keyspace }),
		cmpopts.SortSlices(func(a, b string) bool { return a < b }),
		cmpopts.IgnoreSliceElements(func(u backup.Unit) bool { return u.Keyspace == "system_schema" }),
	); diff != "" {
		t.Fatal(diff)
	}

	Print("When: run backup with generated target")
	if err = h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
		t.Fatal(err)
	}

	Print("Then: backup files contains just the base table and system_schema")
	filesInfo, err := h.service.ListFiles(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID})
	if err != nil {
		t.Fatal(err)
	}
	for _, fi := range filesInfo {
		for _, fm := range fi.Files {
			if fm.Keyspace == "system_schema" {
				continue
			}
			ok := false
			for _, u := range expected {
				for _, tab := range u.Tables {
					if fm.Keyspace == u.Keyspace && fm.Table == tab {
						ok = true
					}
				}
			}
			if !ok {
				t.Fatalf("Unexpected table %q.%q found in backed up files", fm.Keyspace, fm.Table)
			}
		}
	}
}

func TestBackupSkipSchemaIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-skip-schema"
		testKeyspace = "backuptest_skip_schema"
	)

	Print("Given: backup service without CQL password")
	var (
		location       = s3Location(testBucket)
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelperWithUser(t, session, defaultConfig(), location, nil, "no-credentials", "")
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	if CheckAnyConstraint(h.T, h.Client, "< 6.0", "< 2024.2, > 1000") {
		t.Skip("CQL credentials are not needed for the backup with this Scylla version, " +
			"so the --skip-schema flag is not needed there")
	}

	Print("And: simple table to back up")
	WriteData(t, clusterSession, testKeyspace, 1)

	Print("And: backup target with --skip-schema=false")

	props := defaultTestProperties(location, testKeyspace)
	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create raw properties"))
	}
	target, err := h.service.GetTarget(ctx, h.ClusterID, rawProps)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create target"))
	}

	Print("When: run backup")
	err = h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target)

	Print("Then: backup failed")
	if err == nil {
		t.Fatal("Expected backup to fail")
	} else {
		t.Log("Backup error", err)
	}

	Print("Given: backup target with --skip-schema=true")
	props["skip_schema"] = true
	rawProps, err = json.Marshal(props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create raw properties"))
	}
	target, err = h.service.GetTarget(ctx, h.ClusterID, rawProps)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create target"))
	}

	Print("When: run backup")
	h.RunID = uuid.NewTime()
	err = h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target)

	Print("Then: backup succeeded")
	if err != nil {
		t.Fatalf("Expected backup to succeede, got error: %s", err)
	}

	_, schemas, files := h.listS3Files()
	Print("And: CQL schema file wasn't backed up")
	if len(schemas) > 0 {
		t.Fatalf("Expected no CQL schema files to be backed up, got: %v", schemas)
	}
	Print("And: non schema sstables were backed up")
	if len(files) == 0 {
		t.Fatal("Expected non schema sstables to be backed up")
	}
	Print("And: schema sstables weren't backed up")
	for _, f := range files {
		if strings.Contains(f, "system_schema") {
			t.Fatalf("Expected no system_schema sstables to be backed up, got: %s", f)
		}
	}
}

func TestBackupMethodIntegration(t *testing.T) {
	// This test validates that the correct API is used
	// for uploading snapshot dirs (Rclone or Scylla).
	const (
		testBucket   = "backuptest-api"
		testKeyspace = "backuptest_api"
	)

	var (
		location       = s3Location(testBucket)
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelperWithUser(t, session, defaultConfig(), location, nil, "", "")
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	const tableCnt = 2
	WriteData(t, clusterSession, testKeyspace, 1, "tab1", "tab2")

	const (
		rcloneAPIPath = "/agent/rclone/sync/movedir"
		nativeAPIPath = "/storage_service/backup"
	)

	ni, err := h.Client.AnyNodeInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	support, err := ni.SupportsNativeBackupAPI()
	if err != nil {
		t.Fatal(err)
	}

	type testCase struct {
		method           backup.Method
		blockedPath      string
		ensuredPath      string
		getTargetSuccess bool
	}
	var testCases []testCase
	// As currently scylla can't handle ipv6 object storage endpoints,
	// we don't configure them for ipv6 test env and don't expect them to work.
	switch {
	case support && !IsIPV6Network():
		testCases = []testCase{
			{method: backup.MethodAuto, ensuredPath: nativeAPIPath, blockedPath: rcloneAPIPath, getTargetSuccess: true},
			{method: backup.MethodNative, ensuredPath: nativeAPIPath, blockedPath: rcloneAPIPath, getTargetSuccess: true},
			{method: backup.MethodRclone, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
		}
	case support && IsIPV6Network():
		testCases = []testCase{
			{method: backup.MethodAuto, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{method: backup.MethodNative, getTargetSuccess: false},
			{method: backup.MethodRclone, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
		}
	default:
		testCases = []testCase{
			{method: backup.MethodAuto, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{method: backup.MethodNative, getTargetSuccess: false},
			{method: backup.MethodRclone, ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
			{ensuredPath: rcloneAPIPath, blockedPath: nativeAPIPath, getTargetSuccess: true},
		}
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Test case: %s", tc.method), func(t *testing.T) {
			encounteredEnsured := atomic.NewBool(false)
			encounteredBlocked := atomic.NewBool(false)
			if tc.getTargetSuccess {
				waitForParallelUploads := make(chan struct{})
				parallelUploads := atomic.NewInt32(0)

				h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
					if strings.HasPrefix(req.URL.Path, tc.ensuredPath) {
						encounteredEnsured.Store(true)

						if tc.ensuredPath == nativeAPIPath {
							// Verify that tables uploaded with native backup API are uploaded
							// in parallel within the context of the same host.
							if cnt := parallelUploads.Add(1); cnt == tableCnt {
								close(waitForParallelUploads)
							}
							select {
							case <-waitForParallelUploads:
							case <-time.After(time.Second):
								t.Error("No parallel uploads")
							}
						}
					}
					if strings.HasPrefix(req.URL.Path, tc.blockedPath) {
						encounteredBlocked.Store(true)
					}
					return nil, nil
				}))
			}

			props := defaultTestProperties(location, testKeyspace)
			props["method"] = tc.method
			if tc.method == "" {
				delete(props, "method")
			}
			props["parallel"] = 1 // to ensure that parallel uploads come from the same host
			rawProps, err := json.Marshal(props)
			if err != nil {
				t.Fatal(errors.Wrap(err, "create raw properties"))
			}

			target, err := h.service.GetTarget(ctx, h.ClusterID, rawProps)
			if err != nil {
				if !tc.getTargetSuccess {
					return
				}
				t.Fatal(errors.Wrap(err, "create target"))
			}
			err = h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target)
			if err != nil {
				t.Fatal(err)
			}

			if tc.getTargetSuccess {
				if !encounteredEnsured.Load() {
					t.Fatalf("Expected SM to use %q API", tc.ensuredPath)
				}
				if encounteredBlocked.Load() {
					t.Fatalf("Expected SM not to use %q API", tc.blockedPath)
				}
			}
		})
	}
}

func TestGetDescribeSchemaIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-describe-schema"
		testKeyspace = "backuptest_describe_schema"
	)

	location := s3Location(testBucket)
	config := defaultConfig()
	session := CreateScyllaManagerDBSession(t)
	h := newBackupTestHelper(t, session, config, location, nil)

	if CheckAnyConstraint(h.T, h.Client, "< 6.0", "< 2024.2, > 1000") {
		t.Skip("GetSchema works only with DESCRIBE SCHEMA WITH INTERNALS schema backup")
	}
	ni, err := h.Client.AnyNodeInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	alternatorSchemaSupport, err := ni.SupportsAlternatorSchemaBackupFromAPI()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	clusterSession := CreateSessionAndDropAllKeyspaces(t, h.Client)
	accessKeyID, secretAccessKey := GetAlternatorCreds(t, clusterSession, "")
	client := CreateAlternatorClient(t, h.Client, ManagedClusterHost(), accessKeyID, secretAccessKey)

	makeBackup := func() string {
		target := defaultTestTarget(location, testKeyspace, "dc1", 1)
		if err := h.service.InitTarget(ctx, h.ClusterID, &target); err != nil {
			t.Fatal(errors.Wrap(err, "InitTarget"))
		}
		if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
			t.Fatal(errors.Wrap(err, "Backup"))
		}
		items, err := h.service.List(ctx, h.ClusterID, []backupspec.Location{location}, backup.ListFilter{ClusterID: h.ClusterID, TaskID: h.TaskID})
		if err != nil {
			t.Fatal(errors.Wrap(err, "List"))
		}
		if len(items) == 0 || len(items[0].SnapshotInfo) == 0 {
			t.Fatalf("No snapshot tag found")
		}
		return items[0].SnapshotInfo[0].SnapshotTag
	}

	// First backup with table created with CQL
	WriteData(t, clusterSession, testKeyspace, 0, "tab")
	cqlSchema1, err := query.DescribeSchemaWithInternals(clusterSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "DESCRIBE SCHEMA WITH INTERNALS"))
	}
	slices.SortFunc(cqlSchema1, func(a, b query.DescribedSchemaRow) int { return stdCmp.Compare(a.CQLStmt, b.CQLStmt) })
	tag1 := makeBackup()

	// Second backup with table created with Alternator
	const (
		tabAlt        = "tab_alt_" + AlternatorProblematicTableChars
		tabAltLSI     = AlternatorLSIPrefix + "0"
		tabAltGSI     = "gsi_alt" + AlternatorProblematicTableChars
		tabAltTag     = "tab_alt_tag"
		tabAltTTLAttr = "tab_alt_ttl_attr"
	)
	CreateAlternatorTable(t, client, ni, "none", 1, 0, tabAlt)
	CreateAlternatorGSI(t, client, tabAlt, tabAltGSI)
	TagAlternatorTable(t, client, tabAlt, tabAltTag)
	UpdateAlternatorTableTTL(t, client, tabAlt, tabAltTTLAttr, true)
	cqlSchema2, err := query.DescribeSchemaWithInternals(clusterSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "second DESCRIBE SCHEMA WITH INTERNALS"))
	}
	slices.SortFunc(cqlSchema2, func(a, b query.DescribedSchemaRow) int { return stdCmp.Compare(a.CQLStmt, b.CQLStmt) })
	tag2 := makeBackup()

	testCases := []struct {
		name             string
		tag              string
		clusterID        uuid.UUID
		taskID           uuid.UUID
		cqlSchema        query.DescribedSchema
		alternatorSchema bool
	}{
		{
			name:      "tag1 all",
			tag:       tag1,
			clusterID: h.ClusterID,
			taskID:    h.TaskID,
			cqlSchema: cqlSchema1,
		},
		{
			name:      "tag1 no task ID",
			tag:       tag1,
			clusterID: h.ClusterID,
			cqlSchema: cqlSchema1,
		},
		{
			name:      "tag1 no cluster ID",
			tag:       tag1,
			taskID:    h.TaskID,
			cqlSchema: cqlSchema1,
		},
		{
			name:      "tag1 none",
			tag:       tag1,
			cqlSchema: cqlSchema1,
		},
		{
			name:             "tag2 all",
			tag:              tag2,
			clusterID:        h.ClusterID,
			taskID:           h.TaskID,
			cqlSchema:        cqlSchema2,
			alternatorSchema: alternatorSchemaSupport,
		},
		{
			name:             "tag2 no task ID",
			tag:              tag2,
			clusterID:        h.ClusterID,
			cqlSchema:        cqlSchema2,
			alternatorSchema: alternatorSchemaSupport,
		},
		{
			name:             "tag2 no cluster ID",
			tag:              tag2,
			taskID:           h.TaskID,
			cqlSchema:        cqlSchema2,
			alternatorSchema: alternatorSchemaSupport,
		},
		{
			name:             "tag2 none",
			tag:              tag2,
			cqlSchema:        cqlSchema2,
			alternatorSchema: alternatorSchemaSupport,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			Print("When: call GetSchema")
			filter := backup.SchemaFilter{
				ClusterID: tc.clusterID,
				TaskID:    tc.taskID,
			}
			cqlSchema, alternatorSchema, err := backup.GetSchema(ctx, h.Client, tc.tag, location, filter, log.NewDevelopment())
			if err != nil {
				t.Fatal(errors.Wrap(err, "GetSchema"))
			}
			slices.SortFunc(cqlSchema, func(a, b query.DescribedSchemaRow) int { return stdCmp.Compare(a.CQLStmt, b.CQLStmt) })
			// Validate always existing CQL schema
			Print("Then: GetSchema returned correct cqlSchema")
			if diff := cmp.Diff(cqlSchema, tc.cqlSchema); diff != "" {
				t.Fatalf("Schema mismatch\n%s", diff)
			}
			// Validate optional alternator schema
			if !tc.alternatorSchema {
				if len(alternatorSchema.Tables) != 0 {
					t.Fatal("Expected alternator schema to be nil")
				}
				return
			}
			if len(alternatorSchema.Tables) != 1 {
				t.Fatalf("Expected single alternator table, got: %d", len(alternatorSchema.Tables))
			}
			altTab := alternatorSchema.Tables[0]
			if *altTab.Describe.TableName != tabAlt {
				t.Fatalf("Expected alternator table: %s, got: %s", tabAlt, *altTab.Describe.TableName)
			}
			if len(altTab.Describe.LocalSecondaryIndexes) != 1 {
				t.Fatalf("Expected singla alternator LSI, got: %d", len(altTab.Describe.LocalSecondaryIndexes))
			}
			if *altTab.Describe.LocalSecondaryIndexes[0].IndexName != tabAltLSI {
				t.Fatalf("Expected alternator LSI: %s, got: %s", tabAltLSI, *altTab.Describe.LocalSecondaryIndexes[0].IndexName)
			}
			if len(altTab.Describe.GlobalSecondaryIndexes) != 1 {
				t.Fatalf("Expected singla alternator GSI, got: %d", len(altTab.Describe.GlobalSecondaryIndexes))
			}
			if *altTab.Describe.GlobalSecondaryIndexes[0].IndexName != tabAltGSI {
				t.Fatalf("Expected alternator GSI: %s, got: %s", tabAltGSI, *altTab.Describe.GlobalSecondaryIndexes[0].IndexName)
			}
			if len(altTab.Tags) == 0 {
				t.Fatal("Expected alternator tags, got none")
			}
			if *altTab.TTL.AttributeName != tabAltTTLAttr {
				t.Fatalf("Expected alternator TTL: %s, got: %s", tabAltTTLAttr, *altTab.TTL.AttributeName)
			}
		})
	}
}

func TestBackupDeleteLocalSnapshotsIntegration(t *testing.T) {
	const (
		testBucket   = "backuptest-delete-local-snapshots"
		testKeyspace = "backuptest_delete_local_snapshots"
	)

	var (
		location       = s3Location(testBucket)
		session        = CreateScyllaManagerDBSession(t)
		h              = newBackupTestHelperWithUser(t, session, defaultConfig(), location, nil, "", "")
		ctx            = context.Background()
		clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
	)

	WriteData(t, clusterSession, testKeyspace, 1, "tab1", "tab2")

	for i, host := range ManagedClusterHosts() {
		// Make snapshots with different tags for more coverage
		tag := backupspec.SnapshotTagAt(timeutc.Now().Add(-time.Duration(i) * time.Second))
		if err := h.Client.TakeSnapshot(ctx, host, tag, testKeyspace); err != nil {
			t.Fatal(err)
		}
	}

	anySnapshotOnDisk := func() bool {
		for _, host := range ManagedClusterHosts() {
			tags, err := h.Client.Snapshots(ctx, host)
			if err != nil {
				t.Fatal(err)
			}
			if len(tags) > 0 {
				return true
			}
		}
		return false
	}

	if !anySnapshotOnDisk() {
		t.Fatal("Expected to find snapshot on disk after backup was stopped")
	}

	if err := h.service.DeleteLocalSnapshots(ctx, h.ClusterID); err != nil {
		t.Fatal(err)
	}

	if anySnapshotOnDisk() {
		t.Fatal("Expected no snapshots on disk after cleanup")
	}
}
