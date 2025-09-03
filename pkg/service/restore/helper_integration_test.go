// Copyright (C) 2024 ScyllaDB

//go:build all || integration
// +build all integration

package restore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testhelper"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type table struct {
	ks  string
	tab string
}

func defaultTestConfig() Config {
	return Config{
		DiskSpaceFreeMinPercent:   5,
		LongPollingTimeoutSeconds: 1,
	}
}

func defaultBackupTestConfig() backup.Config {
	return backup.Config{
		DiskSpaceFreeMinPercent:   5,
		LongPollingTimeoutSeconds: 1,
		AgeMax:                    24 * time.Hour,
	}
}

func randomizedName(name string) string {
	return name + strings.Replace(fmt.Sprint(uuid.NewTime()), "-", "", -1)
}

type clusterHelper struct {
	*CommonTestHelper
	rootSession gocqlx.Session
	altClient   *dynamodb.Client
}

func newCluster(t *testing.T, hosts []string) clusterHelper {
	logger := log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	hrt := NewHackableRoundTripper(scyllaclient.DefaultTransport())
	clientCfg := scyllaclient.TestConfig(hosts, AgentAuthToken())
	clientCfg.Backoff.MaxRetries = 0
	client := newTestClient(t, hrt, logger.Named("client"), &clientCfg)
	rootSession := CreateSessionAndDropAllKeyspaces(t, client)
	accessKeyID, secretAccessKey := GetAlternatorCreds(t, rootSession, "")
	altClient := CreateAlternatorClient(t, client, client.Config().Hosts[0], accessKeyID, secretAccessKey)

	for _, h := range hosts {
		if err := client.RcloneResetStats(context.Background(), h); err != nil {
			t.Fatal("Reset rclone stats", h, err)
		}
	}

	return clusterHelper{
		CommonTestHelper: &CommonTestHelper{
			Session:   CreateScyllaManagerDBSession(t),
			Hrt:       hrt,
			Client:    client,
			ClusterID: uuid.NewTime(),
			TaskID:    uuid.NewTime(),
			RunID:     uuid.NewTime(),
			T:         t,
		},
		rootSession: rootSession,
		altClient:   altClient,
	}
}

type testHelper struct {
	srcCluster   clusterHelper
	srcBackupSvc *backup.Service

	dstCluster    clusterHelper
	dstRestoreSvc *Service
	dstUser       string
	dstPass       string
}

func newTestHelper(t *testing.T, srcHosts, dstHosts []string) *testHelper {
	srcCluster := newCluster(t, srcHosts)
	dstCluster := newCluster(t, dstHosts)

	user := randomizedName("helper_user_")
	pass := randomizedName("helper_pass_")

	dropNonSuperUsers(t, dstCluster.rootSession)
	createUser(t, dstCluster.rootSession, user, pass)

	return &testHelper{
		srcCluster:    srcCluster,
		srcBackupSvc:  newBackupSvc(t, srcCluster.Session, srcCluster.Client, srcCluster.ClusterID),
		dstCluster:    dstCluster,
		dstRestoreSvc: newRestoreSvc(t, dstCluster.Session, dstCluster.Client, dstCluster.ClusterID, user, pass),
		dstUser:       user,
		dstPass:       pass,
	}
}

func newBackupSvc(t *testing.T, mgrSession gocqlx.Session, client *scyllaclient.Client, clusterID uuid.UUID) *backup.Service {
	svc, err := backup.NewService(
		mgrSession,
		defaultBackupTestConfig(),
		metrics.NewBackupMetrics(),
		func(_ context.Context, id uuid.UUID) (string, error) {
			return "test_cluster", nil
		},
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return CreateSession(t, client), nil
		},
		func(ctx context.Context, clusterID uuid.UUID, host string) (*dynamodb.Client, error) {
			clusterSession := CreateManagedClusterSession(t, false, client, "", "")
			defer clusterSession.Close()
			accessKeyID, secretAccessKey := GetAlternatorCreds(t, clusterSession, "")
			return CreateAlternatorClient(t, client, host, accessKeyID, secretAccessKey), nil
		},
		NewTestConfigCacheSvc(t, clusterID, client.Config().Hosts),
		log.NewDevelopmentWithLevel(zapcore.ErrorLevel).Named("backup"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func newRestoreSvc(t *testing.T, mgrSession gocqlx.Session, client *scyllaclient.Client, clusterID uuid.UUID, user, pass string) *Service {
	configCacheSvc := NewTestConfigCacheSvc(t, clusterID, client.Config().Hosts)

	repairSvc, err := repair.NewService(
		mgrSession,
		repair.DefaultConfig(),
		metrics.NewRepairMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return CreateSession(t, client), nil
		},
		configCacheSvc,
		log.NewDevelopmentWithLevel(zapcore.ErrorLevel).Named("repair"),
	)
	if err != nil {
		t.Fatal(err)
	}

	svc, err := NewService(
		repairSvc,
		mgrSession,
		defaultTestConfig(),
		metrics.NewRestoreMetrics(),
		func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, clusterID uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return CreateManagedClusterSession(t, false, client, user, pass), nil
		},
		func(ctx context.Context, clusterID uuid.UUID, host string) (*dynamodb.Client, error) {
			clusterSession := CreateManagedClusterSession(t, false, client, "", "")
			defer clusterSession.Close()
			accessKeyID, secretAccessKey := GetAlternatorCreds(t, clusterSession, user)
			return CreateAlternatorClient(t, client, host, accessKeyID, secretAccessKey), nil
		},
		configCacheSvc,
		log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("restore"),
	)
	if err != nil {
		t.Fatal(err)
	}

	return svc
}

func defaultTestTarget(locDC, locPath, ks string, batchSize, parallel int, restoreTables bool) Target {
	target := Target{
		Location: []backupspec.Location{
			{
				DC:       locDC,
				Provider: backupspec.S3,
				Path:     locPath,
			},
		},
		BatchSize:     batchSize,
		Parallel:      parallel,
		RestoreTables: restoreTables,
		RestoreSchema: !restoreTables,
	}
	if ks != "" {
		target.Keyspace = []string{ks}
	}
	if restoreTables && testconfig.RestoreMethod() != nil && *testconfig.RestoreMethod() != "" {
		target.Method = Method(*testconfig.RestoreMethod())
	}
	return target
}

func defaultTestProperties(loc backupspec.Location, tag string, restoreTables bool) map[string]any {
	properties := map[string]any{
		"location":       []backupspec.Location{loc},
		"snapshot_tag":   tag,
		"restore_tables": restoreTables,
		"restore_schema": !restoreTables,
	}
	if restoreTables && testconfig.RestoreMethod() != nil && *testconfig.RestoreMethod() != "" {
		properties["method"] = *testconfig.RestoreMethod()
	}
	return properties
}

func defaultTestBackupProperties(loc backupspec.Location, ks string) map[string]any {
	properties := map[string]any{
		"location": []backupspec.Location{loc},
	}
	if ks != "" {
		properties["keyspace"] = []string{ks}
	}
	if testconfig.BackupMethod() != nil && *testconfig.BackupMethod() != "" {
		properties["method"] = *testconfig.BackupMethod()
	}
	return properties
}

func (h *testHelper) runBackup(t *testing.T, props map[string]any) string {
	Printf("Run backup with properties: %v", props)
	ctx := context.Background()
	h.srcCluster.TaskID = uuid.NewTime()
	h.srcCluster.RunID = uuid.NewTime()

	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "marshal properties"))
	}

	target, err := h.srcBackupSvc.GetTarget(ctx, h.srcCluster.ClusterID, rawProps)
	if err != nil {
		t.Fatal(errors.Wrap(err, "generate target"))
	}

	err = h.srcBackupSvc.Backup(ctx, h.srcCluster.ClusterID, h.srcCluster.TaskID, h.srcCluster.RunID, target)
	if err != nil {
		t.Fatal(errors.Wrap(err, "run backup"))
	}

	pr, err := h.srcBackupSvc.GetProgress(ctx, h.srcCluster.ClusterID, h.srcCluster.TaskID, h.srcCluster.RunID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}

	return pr.SnapshotTag
}

func (h *testHelper) runRestore(t *testing.T, props map[string]any) {
	Printf("Run restore with properties: %v", props)
	ctx := context.Background()
	h.dstCluster.TaskID = uuid.NewTime()
	h.dstCluster.RunID = uuid.NewTime()

	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(errors.Wrap(err, "marshal properties"))
	}

	err = h.dstRestoreSvc.Restore(ctx, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
	if err != nil {
		t.Fatal(errors.Wrap(err, "run restore"))
	}
}

func (h *testHelper) getRestoreProgress(t *testing.T) Progress {
	pr, err := h.dstRestoreSvc.GetProgress(context.Background(), h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}
	return pr
}

func (h *testHelper) validateIdenticalTables(t *testing.T, tables []table) {
	pr := h.getRestoreProgress(t)
	validateCompleteProgress(t, pr, tables)

	views, err := query.GetAllViews(h.srcCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get all views"))
	}

	Print("Validate tombstone_gc mode")
	for _, tab := range tables {
		// Don't validate views tombstone_gc
		if views.Has(tab.ks + "." + tab.tab) {
			continue
		}
		srcMode := tombstoneGCMode(t, h.srcCluster.rootSession, tab.ks, tab.tab)
		dstMode := tombstoneGCMode(t, h.dstCluster.rootSession, tab.ks, tab.tab)
		if srcMode != dstMode {
			t.Fatalf("Expected %s tombstone_gc mode, got: %s", srcMode, dstMode)
		}
	}

	Print("Validate row count")
	for _, tab := range tables {
		dstCnt := rowCount(t, h.dstCluster.rootSession, tab.ks, tab.tab)
		srcCnt := rowCount(t, h.srcCluster.rootSession, tab.ks, tab.tab)
		if dstCnt != srcCnt {
			t.Fatalf("srcCount != dstCount")
		}
	}
}

func tombstoneGCMode(t *testing.T, s gocqlx.Session, keyspace, table string) string {
	var ext map[string]string
	q := qb.Select("system_schema.tables").
		Columns("extensions").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(s).
		Bind(keyspace, table)

	defer q.Release()
	if err := q.Scan(&ext); err != nil {
		t.Fatal(errors.Wrap(err, "get table extensions"))
	}

	// Timeout (just using gc_grace_seconds) is the default mode
	mode, ok := ext["tombstone_gc"]
	if !ok {
		return "timeout"
	}

	allModes := []string{"disabled", "timeout", "repair", "immediate"}
	for _, m := range allModes {
		if strings.Contains(mode, m) {
			return m
		}
	}

	t.Fatal(errors.New("unknown mode " + mode))
	return ""
}

// baseTable returns view's base table or "" if it's not a view.
func baseTable(t *testing.T, s gocqlx.Session, keyspace, table string) string {
	q := qb.Select("system_schema.views").
		Columns("base_table_name").
		Where(qb.Eq("keyspace_name")).
		Where(qb.Eq("view_name")).Query(s).BindMap(qb.M{
		"keyspace_name": keyspace,
		"view_name":     table,
	})
	defer q.Release()

	var bt string
	if err := q.Scan(&bt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return ""
		}
		t.Fatal(errors.Wrap(err, "get base table"))
	}
	return bt
}

func rowCount(t *testing.T, s gocqlx.Session, ks, tab string) int {
	var cnt int
	if err := s.Session.Query(fmt.Sprintf("SELECT COUNT(*) FROM %q.%q USING TIMEOUT 300s", ks, tab)).Scan(&cnt); err != nil {
		t.Fatal(errors.Wrapf(err, "get table %s.%s row count", ks, tab))
	}
	Printf("%s.%s row count: %v", ks, tab, cnt)
	return cnt
}

func validateTableContent[K, V comparable](t *testing.T, src, dst gocqlx.Session, keyspace, tab, keyColumn, valueColumn string) {
	srcM := selectTableAsMap[K, V](t, src, keyspace, tab, keyColumn, valueColumn)
	dstM := selectTableAsMap[K, V](t, dst, keyspace, tab, keyColumn, valueColumn)
	if !maps.Equal(srcM, dstM) {
		t.Fatalf("tables have different contents\nsrc:\n%v\ndst:\n%v", srcM, dstM)
	}
}

func selectTableAsMap[K, V comparable](t *testing.T, s gocqlx.Session, keyspace, tab, keyColumn, valueColumn string) map[K]V {
	var (
		k   K
		v   V
		out = make(map[K]V)
	)
	it := s.Session.Query(fmt.Sprintf("SELECT %s, %s FROM %q.%q", keyColumn, valueColumn, keyspace, tab)).Iter()
	for it.Scan(&k, &v) {
		out[k] = v
	}
	if err := it.Close(); err != nil {
		t.Fatal(errors.Wrapf(err, "select rows %s, %s from %q.%q", keyColumn, valueColumn, keyspace, tab))
	}
	return out
}

func filteredTables(t *testing.T, s gocqlx.Session, filter []string) []string {
	f, err := ksfilter.NewFilter(filter)
	if err != nil {
		t.Fatal(err)
	}

	var (
		ks, tab string
		out     []string
	)
	it := s.Session.Query("SELECT keyspace_name, table_name FROM system_schema.tables").Iter()
	for it.Scan(&ks, &tab) {
		if f.Check(ks, tab) {
			out = append(out, ks+"."+tab)
		}
	}

	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func dropNonSuperUsers(t *testing.T, s gocqlx.Session) {
	var (
		name  string
		super bool
	)
	iter := s.Query("LIST USERS", nil).Iter()
	for iter.Scan(&name, &super) {
		if !super {
			if err := s.ExecStmt(fmt.Sprintf("DROP USER '%s'", name)); err != nil {
				t.Fatal(errors.Wrapf(err, "drop user %s", name))
			}
		}
	}
}

func createUser(t *testing.T, s gocqlx.Session, user, pass string) {
	if err := s.ExecStmt(fmt.Sprintf("CREATE USER '%s' WITH PASSWORD '%s'", user, pass)); err != nil {
		t.Fatal(errors.Wrapf(err, "create user %s with pass %s", user, pass))
	}
}

func grantRestoreTablesPermissions(t *testing.T, s gocqlx.Session, restoredTablesFilter []string, user string) {
	f, err := ksfilter.NewFilter(restoredTablesFilter)
	if err != nil {
		t.Fatal(errors.Wrap(err, "create filter"))
	}

	var ks, tab string
	iter := s.Query("SELECT keyspace_name, table_name FROM system_schema.tables", nil).Iter()
	for iter.Scan(&ks, &tab) {
		// Regular tables require ALTER permission
		if f.Check(ks, tab) {
			if ks == "system_auth" {
				// Altering system_auth requires superuser
				if err = s.ExecStmt(fmt.Sprintf("ALTER role '%s' WITH superuser=true", user)); err != nil {
					t.Fatal(errors.Wrap(err, "grant superuser"))
				}
			}
			if err = s.ExecStmt(fmt.Sprintf("GRANT ALTER ON %q.%q TO '%s'", ks, tab, user)); err != nil {
				t.Fatal(errors.Wrapf(err, "grant alter on %s.%s", ks, tab))
			}
		}

		// Views of restored base tables require DROP and CREATE permissions
		if bt := baseTable(t, s, ks, tab); bt != "" {
			if f.Check(ks, bt) {
				if err = s.ExecStmt(fmt.Sprintf("GRANT DROP ON %q.%q TO '%s'", ks, bt, user)); err != nil {
					t.Fatal(errors.Wrapf(err, "grant drop on %s.%s", ks, tab))
				}
				if err = s.ExecStmt(fmt.Sprintf("GRANT CREATE ON %q TO '%s'", ks, user)); err != nil {
					t.Fatal(errors.Wrapf(err, "grant create on %s", ks))
				}
			}
		}
	}

	if err = iter.Close(); err != nil {
		t.Fatal(errors.Wrap(err, "close iterator"))
	}
}

func grantRestoreSchemaPermissions(t *testing.T, s gocqlx.Session, user string) {
	ExecStmt(t, s, "GRANT CREATE ON ALL KEYSPACES TO "+user)
}

func validateCompleteProgress(t *testing.T, pr Progress, tables []table) {
	if pr.Size != pr.Restored {
		t.Fatal("Expected complete restore")
	}
	encountered := make(map[table]struct{})
	for _, kpr := range pr.Keyspaces {
		if kpr.Size != kpr.Restored {
			t.Fatalf("Expected complete keyspace restore (%s)", kpr.Keyspace)
		}
		for _, tpr := range kpr.Tables {
			encountered[table{ks: kpr.Keyspace, tab: tpr.Table}] = struct{}{}
			if tpr.Size != tpr.Restored {
				t.Fatalf("Expected complete table restore (%s)", tpr.Table)
			}
		}
	}

	for _, tab := range tables {
		if _, ok := encountered[tab]; !ok {
			t.Fatalf("Table %s wasn't restored", tab)
		}
		delete(encountered, tab)
	}
	if len(encountered) > 0 {
		t.Fatalf("Restored more tables than expected: %v", encountered)
	}
}

func createTable(t *testing.T, session gocqlx.Session, keyspace string, tables ...string) {
	for _, tab := range tables {
		ExecStmt(t, session, fmt.Sprintf("CREATE TABLE %q.%q (id int PRIMARY KEY, data int)", keyspace, tab))
	}
}

func fillTable(t *testing.T, session gocqlx.Session, rowCnt int, keyspace string, tables ...string) {
	for _, tab := range tables {
		stmt := fmt.Sprintf("INSERT INTO %q.%q (id, data) VALUES (?, ?)", keyspace, tab)
		q := session.Query(stmt, []string{"id", "data"})

		for i := 0; i < rowCnt; i++ {
			if err := q.Bind(i, i).Exec(); err != nil {
				t.Fatal(err)
			}
		}

		q.Release()
	}
}

func runPausedRestore(t *testing.T, restore func(ctx context.Context) error, intervals ...time.Duration) (err error) {
	t.Helper()

	getInterval := func() time.Duration {
		if len(intervals) == 0 {
			return 24 * time.Hour // Return a huge value when no more ticks are expected
		}
		i := intervals[0]
		intervals = intervals[1:]
		return i
	}

	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error)
	ticker := time.NewTicker(getInterval())
	go func() {
		res <- restore(ctx)
	}()
	for {
		select {
		case err := <-res:
			cancel()
			return err
		case <-ticker.C:
			t.Log("Pause restore")
			cancel()
			err := <-res
			if err == nil || !errors.Is(err, context.Canceled) {
				return err
			}

			ctx, cancel = context.WithCancel(context.Background())
			ticker.Reset(getInterval())
			go func() {
				res <- restore(ctx)
			}()
		}
	}
}

func isDownloadOrRestoreEndpoint(path string) bool {
	return strings.HasPrefix(path, "/agent/rclone/sync/copypaths") ||
		strings.HasPrefix(path, "/storage_service/restore")
}

func isLasOrRestoreEndpoint(path string) bool {
	return strings.HasPrefix(path, "/storage_service/sstables") ||
		strings.HasPrefix(path, "/storage_service/restore")
}
