// Copyright (C) 2026 ScyllaDB

//go:build integration

package db

// Temporary proof for scylladb/scylla-manager#4941, not meant to be merged.

import (
	"crypto/rand"
	"flag"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"

	"github.com/scylladb/scylla-manager/v3/pkg/schema/nopmigrate"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/schema"
)

const (
	proofKeyspace   = "test_scylla_manager"
	proofIterations = 5
	proofFillerRows = 2000
	proofFillerSize = 4 << 10
)

func TestDropSMKeyspaceTimeoutProofIntegration(t *testing.T) {
	cluster := createCluster(testconfig.ScyllaManagerDBCluster())
	t.Logf("gocql.timeout flag: %s", testconfig.CQLTimeout())

	// Ground truth: how long DROP KEYSPACE of a populated SM keyspace takes,
	// measured on a session that cannot time out.
	var drops []time.Duration
	t.Run("measure raw drop", func(t *testing.T) {
		for i := range proofIterations {
			populateSMKeyspace(t, cluster)
			s := longSession(t, cluster, "system")
			start := time.Now()
			ExecStmt(t, s, fmt.Sprintf("DROP KEYSPACE IF EXISTS %q", proofKeyspace))
			d := time.Since(start)
			s.Close()
			drops = append(drops, d)
			t.Logf("raw drop %d: %s", i, d)
		}
		t.Logf("raw drop min=%s max=%s", slices.Min(drops), slices.Max(drops))
	})
	if len(drops) == 0 {
		t.Fatal("no drop measured")
	}

	// The code under test, with the timeout CI really uses.
	t.Run("createTestKeyspace default timeout", func(t *testing.T) {
		for i := range proofIterations {
			populateSMKeyspace(t, cluster)
			start := time.Now()
			createTestKeyspace(t, cluster, proofKeyspace)
			t.Logf("createTestKeyspace %d: %s", i, time.Since(start))
		}
	})

	// Deterministic check that the helper's timeout no longer follows
	// -gocql.timeout down: set it below the fastest measured drop.
	t.Run("createTestKeyspace timeout below drop time", func(t *testing.T) {
		short := slices.Min(drops) / 2
		orig := testconfig.CQLTimeout()
		if err := flag.Set("gocql.timeout", short.String()); err != nil {
			t.Fatal(err)
		}
		defer flag.Set("gocql.timeout", orig.String()) //nolint:errcheck
		t.Logf("gocql.timeout set to %s", short)

		populateSMKeyspace(t, cluster)
		start := time.Now()
		createTestKeyspace(t, cluster, proofKeyspace)
		t.Logf("createTestKeyspace: %s", time.Since(start))
	})
}

// populateSMKeyspace creates the SM keyspace with the full SM schema and some
// data, like the keyspace left behind by a previous test package.
func populateSMKeyspace(t *testing.T, cluster *gocql.ClusterConfig) {
	t.Helper()

	admin := longSession(t, cluster, "system")
	ExecStmt(t, admin, fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s
	WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}`, proofKeyspace))
	admin.Close()

	s := longSession(t, cluster, proofKeyspace)
	defer s.Close()
	migrate.Callback = nopmigrate.Callback
	if err := migrate.FromFS(t.Context(), s, schema.Files); err != nil {
		t.Fatal("migrate:", err)
	}

	ExecStmt(t, s, "CREATE TABLE IF NOT EXISTS proof_filler (k int PRIMARY KEY, v blob)")
	v := make([]byte, proofFillerSize)
	if _, err := rand.Read(v); err != nil {
		t.Fatal(err)
	}
	for k := range proofFillerRows {
		if err := s.Session.Query("INSERT INTO proof_filler (k, v) VALUES (?, ?)", k, v).Exec(); err != nil {
			t.Fatal("insert:", err)
		}
	}
}

func longSession(t *testing.T, cluster *gocql.ClusterConfig, keyspace string) gocqlx.Session {
	t.Helper()

	c := *cluster
	c.Keyspace = keyspace
	c.Timeout = 5 * time.Minute
	s, err := gocqlx.WrapSession(c.CreateSession())
	if err != nil {
		t.Fatal(err)
	}
	return s
}
