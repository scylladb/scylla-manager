// Copyright (C) 2025 ScyllaDB

//go:build all || integration
// +build all integration

package repair_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
)

// Read only, should be used for checking testing environment
// like Scylla version or tablets.
var globalNodeInfo *scyllaclient.NodeInfo

// Used to fill globalNodeInfo before running the tests.
func TestMain(m *testing.M) {
	if !flag.Parsed() {
		flag.Parse()
	}

	config := scyllaclient.TestConfig(ManagedClusterHosts(), AgentAuthToken())

	logger := log.NewDevelopment().Named("Setup")
	c, err := scyllaclient.NewClient(config, logger)
	if err != nil {
		logger.Fatal(context.Background(), "Failed to create client", "error", err)
	}

	globalNodeInfo, err = c.AnyNodeInfo(context.Background())
	if err != nil {
		logger.Fatal(context.Background(), "Failed to get global node info", "error", err)
	}

	os.Exit(m.Run())
}

// Creates vnode keyspace.
func createVnodeKeyspace(t *testing.T, session gocqlx.Session, keyspace string, rf1, rf2 int) {
	stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d}"
	if globalNodeInfo.EnableTablets {
		stmt += " AND tablets = {'enabled': false}"
	}
	ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2))
}

// Creates tablet keyspace or skips the test if that's not possible.
func createTabletKeyspace(t *testing.T, session gocqlx.Session, keyspace string, rf1, rf2, tablets int) {
	if !globalNodeInfo.EnableTablets {
		t.Skip("Test requires tablets enabled in order to create tablet keyspace")
	}
	stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d} AND tablets = {'enabled': true, 'initial': %d}"
	ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2, tablets))
}

// Creates keyspace with default replication type (vnode or tablets).
func createDefaultKeyspace(t *testing.T, session gocqlx.Session, keyspace string, rf1, rf2, tablets int) {
	if globalNodeInfo.EnableTablets {
		stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d} AND tablets = {'enabled': true, 'initial': %d}"
		ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2, tablets))
		return
	}
	stmt := "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d, 'dc2': %d}"
	ExecStmt(t, session, fmt.Sprintf(stmt, keyspace, rf1, rf2))
}

func dropKeyspace(t *testing.T, session gocqlx.Session, keyspace string) {
	ExecStmt(t, session, fmt.Sprintf("DROP KEYSPACE IF EXISTS %q", keyspace))
}
