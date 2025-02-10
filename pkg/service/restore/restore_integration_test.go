// Copyright (C) 2024 ScyllaDB

//go:build all || integration
// +build all integration

package restore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/backupspec"
	schematable "github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/maputil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

func TestRestoreTablesUserIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	if CheckAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		t.Skip("Auth restore is not supported in Scylla 6.0. It requires core side support that is aimed at 6.1 release")
	}

	user := randomizedName("user_")
	pass := randomizedName("pass_")
	Printf("Create user (%s/%s) to be backed-up", user, pass)
	createUser(t, h.srcCluster.rootSession, user, pass)
	ExecStmt(t, h.srcCluster.rootSession, "GRANT CREATE ON ALL KEYSPACES TO "+user)

	Print("Run backup")
	loc := []backupspec.Location{testLocation("user", "")}
	S3InitBucket(t, loc[0].Path)
	tag := h.runBackup(t, map[string]any{
		"location": loc,
	})

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, nil, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_tables": true,
	})

	Print("Log in via restored user and check permissions")
	userSession := CreateManagedClusterSession(t, false, h.dstCluster.Client, user, pass)
	newKs := randomizedName("ks_")
	ExecStmt(t, userSession, fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}", newKs))
}

func TestRestoreTablesNoReplicationIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	ks := randomizedName("no_rep_ks_")
	tab := randomizedName("tab_")
	Printf("Create non replicated %s.%s in both cluster", ks, tab)
	ksStmt := fmt.Sprintf("CREATE KEYSPACE %q WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", ks)
	tabStmt := fmt.Sprintf("CREATE TABLE %q.%q (id int PRIMARY KEY, data int)", ks, tab)
	ExecStmt(t, h.srcCluster.rootSession, ksStmt)
	ExecStmt(t, h.srcCluster.rootSession, tabStmt)
	ExecStmt(t, h.dstCluster.rootSession, ksStmt)
	ExecStmt(t, h.dstCluster.rootSession, tabStmt)

	Print("Fill created table")
	stmt := fmt.Sprintf("INSERT INTO %q.%q (id, data) VALUES (?, ?)", ks, tab)
	q := h.srcCluster.rootSession.Query(stmt, []string{"id", "data"})
	defer q.Release()
	for i := 0; i < 100; i++ {
		if err := q.Bind(i, i).Exec(); err != nil {
			t.Fatal(errors.Wrap(err, "fill table"))
		}
	}

	Print("Run backup")
	loc := []backupspec.Location{testLocation("no-replication", "")}
	S3InitBucket(t, loc[0].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location": loc,
		"keyspace": ksFilter,
	})

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"keyspace":       ksFilter,
		"snapshot_tag":   tag,
		"restore_tables": true,
	})

	h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
}

func TestRestoreSchemaRoundtripIntegration(t *testing.T) {
	// Test scenario:
	// - create schema on src cluster
	// - back up src cluster
	// - restore src cluster schema to dst cluster
	// - drop src cluster schema
	// - back up dst cluster
	// - restore dst cluster schema to src cluster
	// - validates that schema was correct at all stages
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())
	hRev := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())

	if !CheckAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		t.Skip("This test assumes that schema is backed up and restored via DESCRIBE SCHEMA WITH INTERNALS")
	}

	ks := randomizedName("roundtrip_")
	tab := randomizedName("tab_")
	Print("Prepare schema with non-default options")
	ksOpt := "durable_writes = false"
	tabOpt := "compaction = {'class': 'NullCompactionStrategy', 'enabled': 'false'}"
	objWithOpt := map[string]string{
		ks:  ksOpt,
		tab: tabOpt,
	}
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d} AND %s"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2, ksOpt))
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data blob) WITH %s"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab, tabOpt))

	Print("Save src describe schema output")
	srcSchema, err := query.DescribeSchemaWithInternals(h.srcCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "describe src schema"))
	}

	Print("Run src backup")
	loc := []backupspec.Location{testLocation("schema-roundtrip", "")}
	S3InitBucket(t, loc[0].Path)
	tag := h.runBackup(t, map[string]any{
		"location": loc,
	})

	Print("Drop backed-up src cluster schema")
	ExecStmt(t, h.srcCluster.rootSession, "DROP KEYSPACE "+ks)

	Print("Run restore of src backup on dst cluster")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_schema": true,
	})

	Print("Save dst describe schema output from src backup")
	dstSchemaSrcBackup, err := query.DescribeSchemaWithInternals(h.dstCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "describe dst schema from src backup"))
	}

	Print("Run dst backup")
	tag = hRev.runBackup(t, map[string]any{
		"location": loc,
	})

	Print("Run restore of dst backup on src cluster")
	grantRestoreSchemaPermissions(t, hRev.dstCluster.rootSession, hRev.dstUser)
	hRev.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_schema": true,
	})

	Print("Save src describe schema output from dst backup")
	srcSchemaDstBackup, err := query.DescribeSchemaWithInternals(h.srcCluster.rootSession)
	if err != nil {
		t.Fatal(errors.Wrap(err, "describe src schema from dst backup"))
	}

	Print("Validate that schema contains objects with options")
	var (
		m1 = map[query.DescribedSchemaRow]struct{}{}
		m2 = map[query.DescribedSchemaRow]struct{}{}
		m3 = map[query.DescribedSchemaRow]struct{}{}
	)
	for _, row := range srcSchema {
		// Scylla 6.3 added roles and service levels to the output of
		// DESC SCHEMA WITH INTERNALS (https://github.com/scylladb/scylladb/pull/20168).
		// Those entities do not live in any particular keyspace, so that's how we identify them.
		// We are skipping them until we properly support their restoration.
		if row.Keyspace == "" {
			continue
		}
		m1[row] = struct{}{}
		if opt, ok := objWithOpt[row.Name]; ok {
			if !strings.Contains(row.CQLStmt, opt) {
				t.Fatalf("Object: %v, with cql: %v, does not contain option: %v", row.Name, row.CQLStmt, opt)
			}
			delete(objWithOpt, row.Name)
		}
	}
	if len(objWithOpt) > 0 {
		t.Fatalf("Src schema: %v, is missing created objects: %v", m1, objWithOpt)
	}
	for _, row := range dstSchemaSrcBackup {
		if row.Keyspace != "" {
			m2[row] = struct{}{}
		}
	}
	for _, row := range srcSchemaDstBackup {
		if row.Keyspace != "" {
			m3[row] = struct{}{}
		}
	}
	Print("Validate that all schemas are the same")
	if !maputil.Equal(m1, m2) {
		t.Fatalf("Src schema: %v, dst schema from src backup: %v, are not equal", m1, m2)
	}
	if !maputil.Equal(m1, m3) {
		t.Fatalf("Src schema: %v, dst schema from dst backup: %v, are not equal", m1, m3)
	}
}

func TestRestoreSchemaDropAddColumnIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	if !CheckAnyConstraint(t, h.dstCluster.Client, ">= 6.0, < 2000", ">= 2024.2, > 1000") {
		t.Skip("This test is the reason why SM needs to restore schema by DESCRIBE SCHEMA WITH INTERNALS")
	}

	ks := randomizedName("drop_add_")
	tab := randomizedName("tab_")

	Print(fmt.Sprintf("Create %q.%q with disabled compaction", ks, tab))
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int) WITH compaction = {'class': 'NullCompactionStrategy', 'enabled': 'false'}"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill created table")
	rowCnt := 100
	stmt := fmt.Sprintf("INSERT INTO %q.%q (id, data) VALUES (?, ?)", ks, tab)
	q := h.srcCluster.rootSession.Query(stmt, []string{"id", "data"})
	defer q.Release()
	for i := 0; i < rowCnt; i++ {
		if err := q.Bind(i, i).Exec(); err != nil {
			t.Fatal(errors.Wrap(err, "fill table"))
		}
	}

	Print("Drop and add column")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf("ALTER TABLE %q.%q DROP data", ks, tab))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf("ALTER TABLE %q.%q ADD data int", ks, tab))

	Print("Fill altered table again")
	for i := rowCnt; i < 2*rowCnt; i++ {
		if err := q.Bind(i, i).Exec(); err != nil {
			t.Fatal(errors.Wrap(err, "fill table"))
		}
	}

	Print("Run backup")
	loc := []backupspec.Location{testLocation("drop-add", "")}
	S3InitBucket(t, loc[0].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location": loc,
		"keyspace": ksFilter,
	})

	Print("Run restore schema")
	grantRestoreSchemaPermissions(t, h.dstCluster.rootSession, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_schema": true,
	})

	Print("Run restore tables")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"keyspace":       ksFilter,
		"snapshot_tag":   tag,
		"restore_tables": true,
	})

	h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
}

func TestRestoreTablesVnodeToTabletsIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	ni, err := h.dstCluster.Client.AnyNodeInfo(context.Background())
	if err != nil {
		t.Fatal(errors.Wrap(err, "get any node info"))
	}
	if !ni.EnableTablets {
		t.Skip("This test assumes that tablets are supported")
	}

	ks := randomizedName("vnode_to_tablet_")
	tab := randomizedName("tab_")
	c1 := "id"
	c2 := "data"

	Print(fmt.Sprintf("Create %q.%q with vnode replication", ks, tab))
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d} AND tablets = {'enabled': '%v'}"
	tabStmt := "CREATE TABLE %q.%q (%s int PRIMARY KEY, %s int)"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2, false))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab, c1, c2))

	Print("Fill created table")
	rowCnt := 100
	stmt := fmt.Sprintf("INSERT INTO %q.%q (id, data) VALUES (?, ?)", ks, tab)
	q := h.srcCluster.rootSession.Query(stmt, []string{c1, c2})
	defer q.Release()
	for i := 0; i < rowCnt; i++ {
		if err := q.Bind(i, i).Exec(); err != nil {
			t.Fatal(errors.Wrap(err, "fill table"))
		}
	}

	Print("Run backup")
	loc := []backupspec.Location{testLocation("vnode-to-tablets", "")}
	S3InitBucket(t, loc[0].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location": loc,
		"keyspace": ksFilter,
	})

	Print("Manually recreate tablet schema")
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 3, true))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab, c1, c2))

	Print("Run restore tables")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"keyspace":       ksFilter,
		"snapshot_tag":   tag,
		"restore_tables": true,
	})

	validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab, c1, c2)
}

func TestRestoreTablesPausedIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	// Setup:
	// ks1: tab, mv, si
	// ks2: tab1, tab2, mv1

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ks1 := randomizedName("paused_1_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks1, 1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks1, 1))
	ks2 := randomizedName("paused_2_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks2, 1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks2, 1))

	Print("Table setup")
	tab := randomizedName("tab_")
	createTable(t, h.srcCluster.rootSession, ks1, tab)
	createTable(t, h.dstCluster.rootSession, ks1, tab)
	tab1 := randomizedName("tab_1_")
	createTable(t, h.srcCluster.rootSession, ks2, tab1)
	createTable(t, h.dstCluster.rootSession, ks2, tab1)
	tab2 := randomizedName("tab_2_")
	createTable(t, h.srcCluster.rootSession, ks2, tab2)
	createTable(t, h.dstCluster.rootSession, ks2, tab2)

	Print("View setup")
	mv := randomizedName("mv_")
	CreateMaterializedView(t, h.srcCluster.rootSession, ks1, tab, mv)
	CreateMaterializedView(t, h.dstCluster.rootSession, ks1, tab, mv)
	si := randomizedName("si_")
	CreateSecondaryIndex(t, h.srcCluster.rootSession, ks1, tab, si)
	CreateSecondaryIndex(t, h.dstCluster.rootSession, ks1, tab, si)
	mv1 := randomizedName("mv_1_")
	CreateMaterializedView(t, h.srcCluster.rootSession, ks2, tab1, mv1)
	CreateMaterializedView(t, h.dstCluster.rootSession, ks2, tab1, mv1)

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks1, tab)
	fillTable(t, h.srcCluster.rootSession, 100, ks2, tab1, tab2)

	units := []backup.Unit{
		{
			Keyspace:  ks1,
			Tables:    []string{tab, mv, si + "_index"},
			AllTables: true,
		},
		{
			Keyspace:  ks2,
			Tables:    []string{tab1, tab2, mv1},
			AllTables: true,
		},
	}

	Print("Run backup")
	loc := []backupspec.Location{testLocation("paused", "")}
	S3InitBucket(t, loc[0].Path)

	// Starting from SM 3.3.1, SM does not allow to back up views,
	// but backed up views should still be tested as older backups might
	// contain them. That's why here we manually force backup target
	// to contain the views.
	ctx := context.Background()
	h.srcCluster.TaskID = uuid.NewTime()
	h.srcCluster.RunID = uuid.NewTime()

	rawProps, err := json.Marshal(map[string]any{"location": loc})
	if err != nil {
		t.Fatal(errors.Wrap(err, "marshal properties"))
	}

	target, err := h.srcBackupSvc.GetTarget(ctx, h.srcCluster.ClusterID, rawProps)
	if err != nil {
		t.Fatal(errors.Wrap(err, "generate target"))
	}
	target.Units = units

	err = h.srcBackupSvc.Backup(ctx, h.srcCluster.ClusterID, h.srcCluster.TaskID, h.srcCluster.RunID, target)
	if err != nil {
		t.Fatal(errors.Wrap(err, "run backup"))
	}

	pr, err := h.srcBackupSvc.GetProgress(ctx, h.srcCluster.ClusterID, h.srcCluster.TaskID, h.srcCluster.RunID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}
	tag := pr.SnapshotTag

	Print("Run restore tables")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, []string{ks1, ks2}, h.dstUser)
	props := map[string]any{
		"location":       loc,
		"keyspace":       []string{ks1, ks2},
		"snapshot_tag":   tag,
		"restore_tables": true,
	}
	err = runPausedRestore(t, func(ctx context.Context) error {
		h.dstCluster.RunID = uuid.NewTime()
		rawProps, err := json.Marshal(props)
		if err != nil {
			return err
		}
		return h.dstRestoreSvc.Restore(ctx, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
	}, 5*time.Second, 20*time.Second, 35*time.Second, 20*time.Second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	for _, u := range units {
		for _, tb := range u.Tables {
			validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, u.Keyspace, tb, "id", "data")
		}
	}
}

func TestRestoreTablesPreparationIntegration(t *testing.T) {
	// Scenario - setup corresponds to things like tombstone_gc mode or compaction being enabled:
	// Run restore - hang on restore data stage
	// Validate setup
	// Pause restore
	// Validate setup
	// Resume restore - hang on restore data stage
	// Validate setup
	// Resume restore - wait for success
	// Validate setup
	// Validate restore success

	h := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ks := randomizedName("prep_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 2))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int) WITH tombstone_gc = {'mode': 'repair'}"
	tab := randomizedName("tab_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab)

	validateState := func(ch clusterHelper, tombstone string, compaction bool, transfers int, rateLimit int, cpus []int64) {
		// Validate tombstone_gc mode
		if got := tombstoneGCMode(t, ch.rootSession, ks, tab); tombstone != got {
			t.Errorf("expected tombstone_gc=%s, got %s", tombstone, got)
		}
		// Validate compaction
		for _, host := range ch.Client.Config().Hosts {
			enabled, err := ch.Client.IsAutoCompactionEnabled(context.Background(), host, ks, tab)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "check compaction on host %s", host))
			}
			if compaction != enabled {
				t.Errorf("expected compaction enabled=%v, got=%v on host %s", compaction, enabled, host)
			}
		}
		// Validate transfers
		for _, host := range ch.Client.Config().Hosts {
			got, err := ch.Client.RcloneGetTransfers(context.Background(), host)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "check transfers on host %s", host))
			}
			if transfers != got {
				t.Errorf("expected transfers=%d, got=%d on host %s", transfers, got, host)
			}
		}
		// Validate rate limit
		for _, host := range ch.Client.Config().Hosts {
			got, err := ch.Client.RcloneGetBandwidthLimit(context.Background(), host)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "check transfers on host %s", host))
			}
			rawLimit := fmt.Sprintf("%dM", rateLimit)
			if rateLimit == 0 {
				rawLimit = "off"
			}
			if rawLimit != got {
				t.Errorf("expected rate_limit=%s, got=%s on host %s", rawLimit, got, host)
			}
		}
		// Validate cpu pinning
		for _, host := range ch.Client.Config().Hosts {
			got, err := ch.Client.GetPinnedCPU(context.Background(), host)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "check transfers on host %s", host))
			}
			slices.Sort(cpus)
			slices.Sort(got)
			if !slices.Equal(cpus, got) {
				t.Errorf("expected cpus=%v, got=%v on host %s", cpus, got, host)
			}
		}
	}

	pinnedCPU := []int64{0} // Taken from scylla-manager-agent.yaml used for testing
	unpinnedCPU := make([]int64, runtime.NumCPU())
	for i := range unpinnedCPU {
		unpinnedCPU[i] = int64(i)
	}

	shardCnt, err := h.dstCluster.Client.ShardCount(context.Background(), ManagedClusterHost())
	if err != nil {
		t.Fatal(err)
	}
	transfers0 := 2 * int(shardCnt)

	setTransfersAndRateLimitAndPinnedCPU := func(ch clusterHelper, transfers int, rateLimit int, pin bool) {
		for _, host := range ch.Client.Config().Hosts {
			err := ch.Client.RcloneSetTransfers(context.Background(), host, transfers)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "set transfers on host %s", host))
			}
			err = ch.Client.RcloneSetBandwidthLimit(context.Background(), host, rateLimit)
			if err != nil {
				t.Fatal(errors.Wrapf(err, "set rate limit on host %s", host))
			}
			if pin {
				err = ch.Client.PinCPU(context.Background(), host)
				if err != nil {
					t.Fatal(errors.Wrapf(err, "pin CPUs on host %s", host))
				}
			} else {
				err = ch.Client.UnpinFromCPU(context.Background(), host)
				if err != nil {
					t.Fatal(errors.Wrapf(err, "unpin CPUs on host %s", host))
				}
			}
		}
	}

	Print("Set initial transfers and rate limit")
	setTransfersAndRateLimitAndPinnedCPU(h.srcCluster, 10, 99, true)
	setTransfersAndRateLimitAndPinnedCPU(h.dstCluster, 10, 99, true)

	Print("Validate state before backup")
	validateState(h.srcCluster, "repair", true, 10, 99, pinnedCPU)

	Print("Run backup")
	loc := []backupspec.Location{testLocation("preparation", "")}
	S3InitBucket(t, loc[0].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location":   loc,
		"keyspace":   ksFilter,
		"transfers":  3,
		"rate_limit": []string{"88"},
	})

	Print("Validate state after backup")
	validateState(h.srcCluster, "repair", true, 3, 88, pinnedCPU)

	runRestore := func(ctx context.Context, finishedRestore chan error) {
		grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
		h.dstCluster.RunID = uuid.NewTime()
		rawProps, err := json.Marshal(map[string]any{
			"location":        loc,
			"keyspace":        ksFilter,
			"snapshot_tag":    tag,
			"transfers":       0,
			"rate_limit":      []string{"0"},
			"unpin_agent_cpu": true,
			"restore_tables":  true,
		})
		if err != nil {
			finishedRestore <- err
			return
		}
		finishedRestore <- h.dstRestoreSvc.Restore(ctx, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
	}

	makeLASHang := func(reachedDataStageChan, hangLAS chan struct{}) {
		cnt := atomic.Int64{}
		cnt.Add(int64(len(h.dstCluster.Client.Config().Hosts)))
		h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.HasPrefix(req.URL.Path, "/storage_service/sstables") {
				if curr := cnt.Add(-1); curr == 0 {
					Print("Reached data stage")
					close(reachedDataStageChan)
				}
				Print("Wait for LAS to stop hanging")
				<-hangLAS
			}
			return nil, nil
		}))
	}

	var (
		reachedDataStageChan = make(chan struct{})
		hangLAS              = make(chan struct{})
	)
	Print("Make LAS hang")
	makeLASHang(reachedDataStageChan, hangLAS)

	Print("Validate state before restore")
	validateState(h.dstCluster, "repair", true, 10, 99, pinnedCPU)

	Print("Run restore")
	finishedRestore := make(chan error)
	restoreCtx, restoreCancel := context.WithCancel(context.Background())
	go runRestore(restoreCtx, finishedRestore)

	Print("Wait for data stage")
	select {
	case <-reachedDataStageChan:
	case err := <-finishedRestore:
		t.Fatalf("Restore finished before reaching data stage with: %s", err)
	}

	Print("Validate state during restore data")
	validateState(h.dstCluster, "disabled", false, transfers0, 0, unpinnedCPU)

	Print("Pause restore")
	restoreCancel()

	Print("Release LAS")
	close(hangLAS)

	Print("Wait for restore")
	err = <-finishedRestore
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected restore to be paused, got: %s", err)
	}

	Print("Validate state during pause")
	validateState(h.dstCluster, "disabled", true, transfers0, 0, pinnedCPU)

	Print("Change transfers and rate limit during pause")
	setTransfersAndRateLimitAndPinnedCPU(h.dstCluster, 9, 55, false)

	reachedDataStageChan = make(chan struct{})
	hangLAS = make(chan struct{})
	Print("Make LAS hang after pause")
	makeLASHang(reachedDataStageChan, hangLAS)

	Print("Run restore after pause")
	finishedRestore = make(chan error)
	go runRestore(context.Background(), finishedRestore)

	Print("Wait for data stage")
	select {
	case <-reachedDataStageChan:
	case err := <-finishedRestore:
		t.Fatalf("Restore finished before reaching data stage with: %s", err)
	}

	Print("Validate state during restore data after pause")
	validateState(h.dstCluster, "disabled", false, transfers0, 0, unpinnedCPU)

	Print("Release LAS")
	close(hangLAS)

	Print("Wait for restore")
	err = <-finishedRestore
	if err != nil {
		t.Fatalf("Expected restore to success, got: %s", err)
	}

	Print("Validate state after restore success")
	validateState(h.dstCluster, "repair", true, transfers0, 0, pinnedCPU)

	Print("Validate table contents")
	h.validateIdenticalTables(t, []table{{ks: ks, tab: tab}})
}

func TestRestoreTablesBatchRetryIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())
	// Ensure no built-in retries
	clientCfg := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())
	clientCfg.Backoff.MaxRetries = 0
	h.dstCluster.Client = newTestClient(t, h.dstCluster.Hrt, log.NewDevelopmentWithLevel(zapcore.InfoLevel).Named("client"), &clientCfg)

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ks := randomizedName("batch_retry_1_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 1))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab1 := randomizedName("tab_1_")
	tab2 := randomizedName("tab_2_")
	tab3 := randomizedName("tab_3_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab1))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab2))
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab3))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab2))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab3))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab1, tab2, tab3)

	Print("Run backup")
	loc := []backupspec.Location{testLocation("batch-retry", "")}
	S3InitBucket(t, loc[0].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location":   loc,
		"keyspace":   ksFilter,
		"batch_size": 100,
	})

	downloadErr := errors.New("fake download error")
	lasErr := errors.New("fake las error")
	props := map[string]any{
		"location":       loc,
		"keyspace":       ksFilter,
		"snapshot_tag":   tag,
		"restore_tables": true,
	}

	t.Run("batch retry finished with success", func(t *testing.T) {
		Print("Inject errors to some download and las calls")
		downloadCnt := atomic.Int64{}
		lasCnt := atomic.Int64{}
		h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			// For this setup, we have 6 remote sstable dirs and 6 workers.
			// We inject 2 errors during download and 3 errors during LAS.
			// This means that only a single node will be restoring at the end.
			// Huge batch size and 3 LAS errors guarantee total 9 calls to LAS.
			// The last failed call to LAS (cnt=8) waits a bit so that we test
			// that batch dispatcher correctly reuses and releases nodes waiting
			// for failed sstables to come back to the batch dispatcher.
			if strings.HasPrefix(req.URL.Path, "/agent/rclone/sync/copypaths") {
				if cnt := downloadCnt.Add(1); cnt == 1 || cnt == 3 {
					t.Log("Fake download error ", cnt)
					return nil, downloadErr
				}
			}
			if strings.HasPrefix(req.URL.Path, "/storage_service/sstables/") {
				cnt := lasCnt.Add(1)
				if cnt == 8 {
					time.Sleep(15 * time.Second)
				}
				if cnt == 1 || cnt == 5 || cnt == 8 {
					t.Log("Fake LAS error ", cnt)
					return nil, lasErr
				}
			}
			return nil, nil
		}))

		Print("Run restore")
		grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
		h.runRestore(t, props)

		Print("Validate success")
		if cnt := lasCnt.Add(0); cnt < 9 {
			t.Fatalf("Expected at least 9 calls to LAS, got %d", cnt)
		}
		validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab1, "id", "data")
		validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab2, "id", "data")
		validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab3, "id", "data")
	})

	t.Run("restore with injected failures only", func(t *testing.T) {
		Print("Inject errors to all download and las calls")
		reachedDataStage := atomic.Bool{}
		reachedDataStageChan := make(chan struct{})
		h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.HasPrefix(req.URL.Path, "/agent/rclone/sync/copypaths") {
				if reachedDataStage.CompareAndSwap(false, true) {
					close(reachedDataStageChan)
				}
				return nil, downloadErr
			}
			if strings.HasPrefix(req.URL.Path, "/storage_service/sstables/") {
				return nil, lasErr
			}
			return nil, nil
		}))

		Print("Run restore")
		grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
		h.dstCluster.TaskID = uuid.NewTime()
		h.dstCluster.RunID = uuid.NewTime()
		rawProps, err := json.Marshal(props)
		if err != nil {
			t.Fatal(errors.Wrap(err, "marshal properties"))
		}
		res := make(chan error)
		go func() {
			res <- h.dstRestoreSvc.Restore(context.Background(), h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
		}()

		Print("Wait for data stage")
		select {
		case <-reachedDataStageChan:
		case err := <-res:
			t.Fatalf("Restore finished before reaching data stage with: %s", err)
		}

		Print("Validate restore failure and that it does not hang")
		select {
		case err := <-res:
			if err == nil {
				t.Fatalf("Expected restore to end with error")
			}
		case <-time.NewTimer(time.Minute).C:
			t.Fatal("Restore hanged")
		}
	})

	t.Run("paused restore with slow calls to download and las", func(t *testing.T) {
		Print("Make download and las calls slow")
		reachedDataStage := atomic.Bool{}
		reachedDataStageChan := make(chan struct{})
		h.dstCluster.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if strings.HasPrefix(req.URL.Path, "/agent/rclone/sync/copypaths") ||
				strings.HasPrefix(req.URL.Path, "/storage_service/sstables/") {
				if reachedDataStage.CompareAndSwap(false, true) {
					close(reachedDataStageChan)
				}
				time.Sleep(time.Second)
				return nil, nil
			}
			return nil, nil
		}))

		Print("Run restore")
		grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
		h.dstCluster.TaskID = uuid.NewTime()
		h.dstCluster.RunID = uuid.NewTime()
		rawProps, err := json.Marshal(props)
		if err != nil {
			t.Fatal(errors.Wrap(err, "marshal properties"))
		}
		ctx, cancel := context.WithCancel(context.Background())
		res := make(chan error)
		go func() {
			res <- h.dstRestoreSvc.Restore(ctx, h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID, rawProps)
		}()

		Print("Wait for data stage")
		select {
		case <-reachedDataStageChan:
			cancel()
		case err := <-res:
			t.Fatalf("Restore finished before reaching data stage with: %s", err)
		}

		Print("Validate restore was paused in time")
		select {
		case err := <-res:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Expected restore to end with context cancelled, got %q", err)
			}
		case <-time.NewTimer(2 * time.Second).C:
			t.Fatal("Restore wasn't paused in time")
		}
	})
}

func TestRestoreTablesMultiLocationIntegration(t *testing.T) {
	// Since we need multi-dc clusters for multi-dc backup/restore
	// we will use the same cluster as both src and dst.
	h := newTestHelper(t, ManagedClusterHosts(), ManagedClusterHosts())

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}"
	ks := randomizedName("multi_location_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab := randomizedName("tab_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab)

	Print("Save filled table into map")
	srcM := selectTableAsMap[int, int](t, h.srcCluster.rootSession, ks, tab, "id", "data")

	Print("Run backup")
	loc := []backupspec.Location{
		testLocation("multi-location-1", "dc1"),
		testLocation("multi-location-2", "dc2"),
	}
	S3InitBucket(t, loc[0].Path)
	S3InitBucket(t, loc[1].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location":   loc,
		"keyspace":   ksFilter,
		"batch_size": 100,
	})

	Print("Truncate backed up table")
	truncateStmt := "TRUNCATE TABLE %q.%q"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(truncateStmt, ks, tab))

	// Reverse dcs - just for fun
	loc[0].DC = "dc2"
	loc[1].DC = "dc1"

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	res := make(chan struct{})
	go func() {
		h.runRestore(t, map[string]any{
			"location": loc,
			"keyspace": ksFilter,
			// Test if batching does not hang with
			// limited parallel and location access.
			"parallel":       1,
			"snapshot_tag":   tag,
			"restore_tables": true,
		})
		close(res)
	}()

	select {
	case <-res:
	case <-time.NewTimer(2 * time.Minute).C:
		t.Fatal("Restore hanged")
	}

	Print("Save restored table into map")
	dstM := selectTableAsMap[int, int](t, h.dstCluster.rootSession, ks, tab, "id", "data")

	Print("Validate success")
	if !maps.Equal(srcM, dstM) {
		t.Fatalf("tables have different contents\nsrc:\n%v\ndst:\n%v", srcM, dstM)
	}
}

func TestRestoreTablesProgressIntegration(t *testing.T) {
	// It verifies that:
	// - view status progress is correct
	// - progress is available even when cluster is not

	if IsIPV6Network() {
		t.Skip("nodes don't have ip6tables and related modules to properly simulate unavailable cluster")
	}

	h := newTestHelper(t, ManagedSecondClusterHosts(), ManagedClusterHosts())

	Print("Keyspace setup")
	ks := randomizedName("progress_")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks, 1))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmt, ks, 1))

	Print("Table setup")
	tab := randomizedName("tab_")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("View setup")
	mv := randomizedName("mv_")
	CreateMaterializedView(t, h.srcCluster.rootSession, ks, tab, mv)
	CreateMaterializedView(t, h.dstCluster.rootSession, ks, tab, mv)

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 1, ks, tab)

	Print("Run backup")
	loc := []backupspec.Location{testLocation("progress", "")}
	S3InitBucket(t, loc[0].Path)
	tag := h.runBackup(t, map[string]any{
		"location": loc,
	})

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, nil, h.dstUser)
	h.runRestore(t, map[string]any{
		"location":       loc,
		"snapshot_tag":   tag,
		"restore_tables": true,
	})

	Print("Validate success")
	validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, tab, "id", "data")
	validateTableContent[int, int](t, h.srcCluster.rootSession, h.dstCluster.rootSession, ks, mv, "id", "data")

	Print("Validate view progress")
	pr, err := h.dstRestoreSvc.GetProgress(context.Background(), h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}
	for _, v := range pr.Views {
		if v.BuildStatus != scyllaclient.StatusSuccess {
			t.Fatalf("Expected status: %s, got: %s", scyllaclient.StatusSuccess, v.BuildStatus)
		}
	}

	BlockREST(t, ManagedClusterHosts()...)
	defer func() {
		TryUnblockREST(t, ManagedClusterHosts())
		if err := EnsureNodesAreUP(t, ManagedClusterHosts(), time.Minute); err != nil {
			t.Fatal(err)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	Print("Validate view progress when cluster is unavailable")
	pr, err = h.dstRestoreSvc.GetProgress(context.Background(), h.dstCluster.ClusterID, h.dstCluster.TaskID, h.dstCluster.RunID)
	if err != nil {
		t.Fatal(errors.Wrap(err, "get progress"))
	}
	for _, v := range pr.Views {
		if v.BuildStatus != scyllaclient.StatusSuccess {
			t.Fatalf("Expected status: %s, got: %s", scyllaclient.StatusSuccess, v.BuildStatus)
		}
	}
}

func TestRestoreOnlyOneDCFromLocationIntegration(t *testing.T) {
	h := newTestHelper(t, ManagedClusterHosts(), ManagedSecondClusterHosts())

	Print("Keyspace setup")
	// Source cluster
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}"
	ksTwoDC := randomizedName("two_dc_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ksTwoDC))

	// Keyspace thats only available in dc2
	ksStmtOneDC := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1':0, 'dc2': 1}"
	ksOneDC := randomizedName("one_dc_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmtOneDC, ksOneDC))

	// Target cluster
	ksStmtDst := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1}"
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmtDst, ksTwoDC))
	ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(ksStmtDst, ksOneDC))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab := randomizedName("tab_")
	for _, ks := range []string{ksTwoDC, ksOneDC} {
		ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
		ExecStmt(t, h.dstCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))
	}

	Print("Fill setup")
	for _, ks := range []string{ksTwoDC, ksOneDC} {
		fillTable(t, h.srcCluster.rootSession, 100, ks, tab)
	}

	Print("Save filled table into map")
	srcMTwoDC := selectTableAsMap[int, int](t, h.srcCluster.rootSession, ksTwoDC, tab, "id", "data")

	Print("Run backup")
	loc := []backupspec.Location{
		testLocation("one-location-1", ""),
	}
	S3InitBucket(t, loc[0].Path)
	ksFilter := []string{ksTwoDC, ksOneDC}
	tag := h.runBackup(t, map[string]any{
		"location":   loc,
		"keyspace":   ksFilter,
		"batch_size": 100,
	})

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	res := make(chan struct{})
	go func() {
		h.runRestore(t, map[string]any{
			"location": loc,
			"keyspace": ksFilter,
			// Test if batching does not hang with
			// limited parallel and location access.
			"parallel":       1,
			"snapshot_tag":   tag,
			"restore_tables": true,
			"dc_mapping": map[string]string{
				"dc1": "dc1",
			},
		})
		close(res)
	}()

	select {
	case <-res:
	case <-time.NewTimer(2 * time.Minute).C:
		t.Fatal("Restore hanged")
	}

	Print("Save restored table into map")
	dstMTwoDC := selectTableAsMap[int, int](t, h.dstCluster.rootSession, ksTwoDC, tab, "id", "data")
	dstMOneDC := selectTableAsMap[int, int](t, h.dstCluster.rootSession, ksOneDC, tab, "id", "data")

	Print("Validate success")
	if !maps.Equal(srcMTwoDC, dstMTwoDC) {
		t.Fatalf("tables have different contents\nsrc:\n%v\ndst:\n%v", srcMTwoDC, dstMTwoDC)
	}
	if len(dstMOneDC) != 0 {
		t.Fatalf("dc2 shouldn't be restored")
	}
}

func TestRestoreDCMappingsIntegration(t *testing.T) {
	// Since we need multi-dc clusters for multi-dc backup/restore
	// we will use the same cluster as both src and dst.
	h := newTestHelper(t, ManagedClusterHosts(), ManagedClusterHosts())

	Print("Keyspace setup")
	ksStmt := "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}"
	ks := randomizedName("multi_location_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(ksStmt, ks))

	Print("Table setup")
	tabStmt := "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
	tab := randomizedName("tab_")
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(tabStmt, ks, tab))

	Print("Fill setup")
	fillTable(t, h.srcCluster.rootSession, 100, ks, tab)

	Print("Save filled table into map")
	srcM := selectTableAsMap[int, int](t, h.srcCluster.rootSession, ks, tab, "id", "data")

	Print("Run backup")
	loc := []backupspec.Location{
		testLocation("dc-mapping-1", "dc1"),
		testLocation("dc-mapping-2", "dc2"),
	}
	S3InitBucket(t, loc[0].Path)
	S3InitBucket(t, loc[1].Path)
	ksFilter := []string{ks}
	tag := h.runBackup(t, map[string]any{
		"location":   loc,
		"keyspace":   ksFilter,
		"batch_size": 100,
	})

	Print("Truncate backed up table")
	truncateStmt := "TRUNCATE TABLE %q.%q"
	ExecStmt(t, h.srcCluster.rootSession, fmt.Sprintf(truncateStmt, ks, tab))

	Print("Run restore")
	grantRestoreTablesPermissions(t, h.dstCluster.rootSession, ksFilter, h.dstUser)
	res := make(chan struct{})
	dcMappings := map[string]string{
		"dc1": "dc1",
		"dc2": "dc2",
	}
	go func() {
		h.runRestore(t, map[string]any{
			"location":       loc,
			"keyspace":       ksFilter,
			"snapshot_tag":   tag,
			"restore_tables": true,
			"dc_mapping":     dcMappings,
		})
		close(res)
	}()

	select {
	case <-res:
	case <-time.NewTimer(2 * time.Minute).C:
		t.Fatal("Restore hanged")
	}

	Print("Save restored table into map")
	dstM := selectTableAsMap[int, int](t, h.dstCluster.rootSession, ks, tab, "id", "data")

	Print("Validate success")
	if !maps.Equal(srcM, dstM) {
		t.Fatalf("tables have different contents\nsrc:\n%v\ndst:\n%v", srcM, dstM)
	}

	Print("Ensure nodes downloaded tables only from corresponding DC")
	// Restore run progess has RmoteSSTableDir of downloaded table which contains dc name in the path
	// We can compare this dc with the host dc (apply mappings) and they should be equal.
	restoreProgress := selectRestoreRunProgress(t, h)
	sourceDCByTargetDC := revertDCMapping(dcMappings)
	for _, pr := range restoreProgress {
		targetDC, err := h.dstCluster.Client.HostDatacenter(context.Background(), pr.Host)
		if err != nil {
			t.Fatalf("get host dc: %v", err)
		}
		expectedSourceDC, ok := sourceDCByTargetDC[targetDC]
		if !ok {
			t.Fatalf("mapping not found for target dc: %v", targetDC)
		}
		actualDC := getDCFromRemoteSSTableDir(t, pr.RemoteSSTableDir)

		if actualDC != expectedSourceDC {
			t.Fatalf("Host should download data only from %s, but downloaded from %s", expectedSourceDC, actualDC)
		}
	}
}

func getDCFromRemoteSSTableDir(t *testing.T, remoteSSTableDir string) string {
	t.Helper()
	// hacky way of extracting value of dc_name from  /dc/{dc_name}/node/
	var re = regexp.MustCompile(`\/dc\/(.*?)\/node\/`)
	matches := re.FindStringSubmatch(remoteSSTableDir)
	if len(matches) != 2 {
		t.Fatalf("Unexpected remote sstable dir format: %s", remoteSSTableDir)
	}

	return matches[1]
}

func revertDCMapping(dcMapping map[string]string) map[string]string {
	result := make(map[string]string, len(dcMapping))
	for k, v := range dcMapping {
		result[v] = k
	}
	return result
}

func selectRestoreRunProgress(t *testing.T, h *testHelper) []restore.RunProgress {
	t.Helper()
	q := schematable.RestoreRunProgress.SelectQuery(h.dstCluster.Session)
	it := q.BindMap(qb.M{
		"cluster_id": h.dstCluster.ClusterID,
		"task_id":    h.dstCluster.TaskID,
		"run_id":     h.dstCluster.RunID,
	}).Iter()
	defer q.Release()
	var (
		result []restore.RunProgress
		row    restore.RunProgress
	)
	for it.StructScan(&row) {
		result = append(result, row)
		row = restore.RunProgress{}
	}
	return result
}
