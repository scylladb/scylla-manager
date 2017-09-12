// Copyright (C) 2017 ScyllaDB

package schema

import "github.com/scylladb/gocqlx/qb"

// Table represents a table in a database and it's metadata.
type Table struct {
	Keyspace string
	Name     string
	Columns  []string
	PartKey  []string
	SortKey  []string

	pk     []qb.Cmp
	delete cql
	get    cql
	insert cql
	sel    cql
}

type cql struct {
	stmt  string
	names []string
}

// Delete returns delete by primary key statement.
func (t *Table) Delete() (stmt string, names []string) {
	return t.delete.stmt, t.delete.names
}

// Get returns select by primary key statement.
func (t *Table) Get() (stmt string, names []string) {
	return t.get.stmt, t.get.names
}

// Insert returns insert all columns statement.
func (t *Table) Insert() (stmt string, names []string) {
	return t.insert.stmt, t.insert.names
}

// Select returns select by partition key statement.
func (t *Table) Select(columns ...string) (stmt string, names []string) {
	if len(columns) == 0 {
		return t.sel.stmt, t.sel.names
	}

	return qb.Select(t.Name).
		Columns(columns...).
		Where(t.pk[0:len(t.PartKey)]...).
		ToCql()
}

func (t Table) init() Table {
	// primary key comparator
	t.pk = make([]qb.Cmp, len(t.PartKey)+len(t.SortKey))
	for i, c := range append(t.PartKey, t.SortKey...) {
		t.pk[i] = qb.Eq(c)
	}

	// delete
	{
		t.delete.stmt, t.delete.names = qb.Delete(t.Name).Where(t.pk...).ToCql()
	}

	// get
	{
		t.get.stmt, t.get.names = qb.Select(t.Name).Where(t.pk...).ToCql()
	}

	// insert
	{
		t.insert.stmt, t.insert.names = qb.Insert(t.Name).Columns(t.Columns...).ToCql()
	}

	// select
	{
		t.sel.stmt, t.sel.names = qb.Select(t.Name).Where(t.pk[0:len(t.PartKey)]...).ToCql()
	}

	return t
}

// Tables listing
var (
	RepairConfig = Table{
		Keyspace: "scylla_management",
		Name:     "scylla_management.repair_config",
		Columns:  []string{"cluster_id", "type", "external_id", "enabled", "segment_size_limit", "retry_limit", "retry_backoff_seconds", "parallel_shard_percent"},
		PartKey:  []string{"cluster_id"},
		SortKey:  []string{"external_id", "type"},
	}.init()

	RepairRun = Table{
		Keyspace: "scylla_management",
		Name:     "scylla_management.repair_run",
		Columns:  []string{"cluster_id", "unit_id", "id", "topology_hash", "keyspace_name", "tables", "status", "cause", "restart_count", "start_time", "end_time", "pause_time"},
		PartKey:  []string{"cluster_id"},
		SortKey:  []string{"unit_id", "id"},
	}.init()

	RepairRunError = Table{
		Keyspace: "scylla_management",
		Name:     "scylla_management.repair_run_error",
		Columns:  []string{"cluster_id", "unit_id", "run_id", "start_token", "end_token", "status", "cause", "coordinator_host", "shard", "command_id", "start_time", "end_time", "fail_count"},
		PartKey:  []string{"cluster_id", "unit_id", "run_id"},
		SortKey:  []string{"coordinator_host", "shard", "start_token"},
	}.init()

	RepairRunProgress = Table{
		Keyspace: "scylla_management",
		Name:     "scylla_management.repair_run_progress",
		Columns:  []string{"cluster_id", "unit_id", "run_id", "host", "shard", "segment_count", "segment_success", "segment_error", "last_start_token", "last_start_time", "last_command_id"},
		PartKey:  []string{"cluster_id", "unit_id", "run_id"},
		SortKey:  []string{"host", "shard"},
	}.init()

	RepairUnit = Table{
		Keyspace: "scylla_management",
		Name:     "scylla_management.repair_unit",
		Columns:  []string{"cluster_id", "id", "keyspace_name", "tables"},
		PartKey:  []string{"cluster_id"},
		SortKey:  []string{"id"},
	}.init()
)
