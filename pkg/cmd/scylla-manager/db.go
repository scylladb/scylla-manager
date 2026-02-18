// Copyright (C) 2026 ScyllaDB

package main

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-log/gocqllog"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/dbutil"
	"github.com/scylladb/gocqlx/v2/migrate"
	config "github.com/scylladb/scylla-manager/v3/pkg/config/server"
	schemamigrate "github.com/scylladb/scylla-manager/v3/pkg/schema/migrate"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/schema"
)

func keyspaceExists(ctx context.Context, c config.Config, logger log.Logger) (bool, error) {
	session, err := gocqlClusterConfigForDBInit(ctx, c, logger).CreateSession()
	if err != nil {
		return false, err
	}
	defer session.Close()

	var cnt int
	q := session.Query("SELECT COUNT(keyspace_name) FROM system_schema.keyspaces WHERE keyspace_name = ?").Bind(c.Database.Keyspace)
	if err := q.Scan(&cnt); err != nil {
		return false, err
	}
	return cnt == 1, nil
}

func createKeyspace(ctx context.Context, c config.Config, logger log.Logger) error {
	const createKsStmt = "CREATE KEYSPACE IF NOT EXISTS %q WITH replication = {'class': 'NetworkTopologyStrategy', %s}"

	session, err := gocqlClusterConfigForDBInit(ctx, c, logger).CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	dcs, err := queryDCInfo(ctx, session)
	if err != nil {
		return errors.Wrap(err, "query dc info")
	}
	// First try to create scylla manager keyspace according to chooseDCRF.
	// This is the most preferred option.
	dcRF := chooseDCRF(dcs)
	if err := session.QueryWithContext(ctx, fmt.Sprintf(createKsStmt, c.Database.Keyspace, dcRFReplication(dcRF))).Exec(); err != nil {
		logger.Info(ctx, "Failed to create upgraded keyspace, trying to create rf rack valid one", "error", err)
		// In case that fails, try to create rf rack valid
		// keyspace regardless of data availability.
		dcRF = rfRackValidDCRF(dcs)
		if err = session.QueryWithContext(ctx, fmt.Sprintf(createKsStmt, c.Database.Keyspace, dcRFReplication(dcRF))).Exec(); err != nil {
			return errors.Wrap(err, "failed to create manager keyspace, "+
				"consider creating it manually and starting manager server again")
		}
	}
	return nil
}

type dcInfo struct {
	dc    string
	racks map[string]rackInfo
	nodes int
}

type rackInfo struct {
	dc    string
	rack  string
	nodes int
}

// queryDCInfo returns cluster topology aggregated by dc and rack.
func queryDCInfo(ctx context.Context, session *gocql.Session) (map[string]dcInfo, error) {
	dcs := make(map[string]dcInfo)
	updateDCs := func(dc, rack string) {
		// Initialize dc info if needed
		if _, ok := dcs[dc]; !ok {
			dcs[dc] = dcInfo{
				dc:    dc,
				racks: make(map[string]rackInfo),
			}
		}
		dci := dcs[dc]
		// Update rack info
		ri := dci.racks[rack]
		ri.dc = dc
		ri.rack = rack
		ri.nodes++
		// Update dc info
		dci.nodes++
		// Update nested structure
		dci.racks[rack] = ri
		dcs[dc] = dci
	}

	var dc, rack string
	it := session.QueryWithContext(ctx, "SELECT data_center, rack FROM system.peers").Iter()
	for it.Scan(&dc, &rack) {
		updateDCs(dc, rack)
	}
	if err := it.Close(); err != nil {
		return nil, errors.Wrap(err, "select data_center and rack from system.peers")
	}

	if err := session.QueryWithContext(ctx, "SELECT data_center, rack FROM system.local").Scan(&dc, &rack); err != nil {
		return nil, errors.Wrap(err, "select data_center and rack from system.local")
	}
	updateDCs(dc, rack)

	return dcs, nil
}

// chooseDCRF returns dc to rf mapping for scylla manager keyspace.
// It firstly prioritizes availability (rf=3) and secondly rf rack validity.
func chooseDCRF(dcs map[string]dcInfo) map[string]int {
	totalRackCnt := 0
	for _, dci := range dcs {
		totalRackCnt += len(dci.racks)
	}

	dcRF := make(map[string]int)
	// The goal is to create rf rack valid keyspace, as this
	// is the most recommended production keyspace setup.
	// In case of unusual cluster topologies, where rf rack
	// valid keyspace would have total rf lower than 3,
	// we upgrade rf up to 3, as we prioritize increased
	// availability over rf rack validity.
	switch {
	case len(dcs) == 1 && totalRackCnt <= 2:
		// 1 dc 1 rack or 1 dc 2 racks scenarios.
		// Those are the same because we can't yet/always
		// specify per rack rf. In such cases, upgrade
		// rf up to 3 for this dc.
		for _, dci := range dcs {
			dcRF[dci.dc] = min(dci.nodes, 3)
		}
	case len(dcs) == 2 && totalRackCnt == 2:
		// 2 dc 1 rack each scenario.
		// In such cases, upgrade rf up to
		// 2 in one of those dcs and keep
		// it at 1 in another one.
		orderedDCs := make([]dcInfo, 0, len(dcs))
		for _, dci := range dcs {
			orderedDCs = append(orderedDCs, dci)
		}
		// Sort dcs for deterministic result.
		// Sort func args are flipped to ensure descending order.
		slices.SortFunc(orderedDCs, func(b, a dcInfo) int {
			if a.nodes != b.nodes {
				return cmp.Compare(a.nodes, b.nodes)
			}
			return cmp.Compare(a.dc, b.dc)
		})
		dcRF[orderedDCs[0].dc] = 1
		dcRF[orderedDCs[1].dc] = 1
		if orderedDCs[0].nodes > 1 {
			dcRF[orderedDCs[0].dc] = 2
		}
	default:
		// In other scenarios, rf rack valid keyspace
		// has bigger total rf than 3.
		return rfRackValidDCRF(dcs)
	}

	return dcRF
}

// rfRackValidDCRF return rf rack valid dc to rf mapping.
func rfRackValidDCRF(dcs map[string]dcInfo) map[string]int {
	dcRF := make(map[string]int)
	for _, dci := range dcs {
		dcRF[dci.dc] = len(dci.racks)
	}
	return dcRF
}

// dcRFReplication returns parsed dc to rf mapping
// that can be used in create keyspace statement.
func dcRFReplication(dcRF map[string]int) string {
	var dcRFStmt []string
	for dc, rf := range dcRF {
		dcRFStmt = append(dcRFStmt, fmt.Sprintf("'%s': %d", dc, rf))
	}
	return strings.Join(dcRFStmt, ", ")
}

func migrateSchema(ctx context.Context, c config.Config, logger log.Logger) error {
	cluster := gocqlClusterConfigForDBInit(ctx, c, logger)
	cluster.Keyspace = c.Database.Keyspace

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		return err
	}
	defer session.Close()

	// Run migrations
	schemamigrate.Logger = logger
	migrate.Callback = schemamigrate.Callback
	if err := migrate.FromFS(ctx, session, schema.Files); err != nil {
		return err
	}

	// Run post migration actions
	return fixSchedulerTaskTTL(session, logger, cluster.Keyspace)
}

func fixSchedulerTaskTTL(session gocqlx.Session, logger log.Logger, keyspace string) error {
	ctx := context.Background()

	var ttl int
	if err := session.Query("SELECT default_time_to_live FROM system_schema.tables WHERE keyspace_name=? AND table_name=?", nil).
		Bind(keyspace, "scheduler_task").Scan(&ttl); err != nil {
		logger.Info(ctx, "Failed to get scheduler_task table properties")
	}
	if ttl == 0 {
		return nil
	}

	logger.Info(ctx, "Post migration", "action", "fix TTL in scheduler_task table")
	if err := session.ExecStmt("ALTER TABLE scheduler_task WITH default_time_to_live = 0"); err != nil {
		return err
	}

	return dbutil.RewriteTable(session, table.SchedulerTask, table.SchedulerTask, nil)
}

func gocqlClusterConfigForDBInit(ctx context.Context, c config.Config, logger log.Logger) *gocql.ClusterConfig {
	cluster := gocqlClusterConfig(c)
	cluster.Keyspace = "system"
	cluster.Timeout = c.Database.MigrateTimeout
	cluster.DefaultIdempotence = false
	cluster.MaxWaitSchemaAgreement = c.Database.MigrateMaxWaitSchemaAgreement

	cluster.Logger = gocqllog.StdLogger{
		BaseCtx: ctx,
		Logger:  logger.Named("gocql"),
	}

	// Use only a single host for migrations, using multiple hosts may lead to
	// conflicting schema changes. This can be avoided by awaiting schema
	// changes see https://github.com/scylladb/gocqlx/issues/106.
	cluster.Hosts = []string{c.Database.InitAddr}
	cluster.DisableInitialHostLookup = true

	return cluster
}

func gocqlClusterConfig(c config.Config) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(c.Database.Hosts...)

	// Chose consistency level, for a single node deployments use ONE, for
	// multi-dc deployments use LOCAL_QUORUM, otherwise use QUORUM.
	switch {
	case c.Database.LocalDC != "":
		cluster.Consistency = gocql.LocalQuorum
	case c.Database.ReplicationFactor == 1:
		cluster.Consistency = gocql.One
	default:
		cluster.Consistency = gocql.Quorum
	}

	cluster.Keyspace = c.Database.Keyspace
	cluster.Timeout = c.Database.Timeout
	cluster.DefaultIdempotence = true
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 5,
		Min:        time.Second,
		Max:        10 * time.Second,
	}

	// ReplicationFactor = 1 detects default deployment, a situation where there is only a single node.
	// Setting the ConvictionPolicy policy prevents from marking the only host as down if heart beat fails of control connection.
	// Otherwise, it would result in removal of the whole connection pool and prevent any retries.
	if c.Database.ReplicationFactor == 1 {
		cluster.ConvictionPolicy = neverConvictionPolicy{}
	}

	// SSL
	if c.Database.SSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 c.SSL.CertFile,
			CertPath:               c.SSL.UserCertFile,
			KeyPath:                c.SSL.UserKeyFile,
			EnableHostVerification: c.SSL.Validate,
		}
	}

	// Authentication
	if c.Database.User != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: c.Database.User,
			Password: c.Database.Password,
		}
	}

	if c.Database.TokenAware {
		fallback := gocql.RoundRobinHostPolicy()
		if c.Database.LocalDC != "" {
			fallback = gocql.DCAwareRoundRobinPolicy(c.Database.LocalDC)
		}
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	}
	if c.Database.Port != 0 {
		cluster.Port = c.Database.Port
	}

	return cluster
}

type neverConvictionPolicy struct{}

func (e neverConvictionPolicy) AddFailure(_ error, _ *gocql.HostInfo) bool {
	return false
}

func (e neverConvictionPolicy) Reset(_ *gocql.HostInfo) {}
