package schema

import "github.com/scylladb/gocqlx/qb"

// Table represents a table in a database and it's metadata.
type Table struct {
	Keyspace string
	Name     string
	Columns  []string
	PartKey  []string
	SortKey  []string

	delete cql
	get    cql
	insert cql
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

func (t Table) init() Table {
	pk := make([]qb.Cmp, len(t.PartKey)+len(t.SortKey))
	for i, c := range append(t.PartKey, t.SortKey...) {
		pk[i] = qb.Eq(c)
	}

	// delete
	{
		t.delete.stmt, t.delete.names = qb.Delete(t.Name).Where(pk...).ToCql()
	}

	// get
	{
		t.get.stmt, t.get.names = qb.Select(t.Name).Where(pk...).ToCql()
	}

	// insert
	{
		t.insert.stmt, t.insert.names = qb.Insert(t.Name).Columns(t.Columns...).ToCql()
	}

	return t
}

// Tables listing
var (
	RepairConfig = Table{
		Keyspace: "scylla_management",
		Name:     "scylla_management.repair_config",
		Columns:  []string{"cluster_id", "type", "external_id", "enabled", "segments_per_shard", "retry_limit", "retry_backoff_seconds", "parallel_node_limit", "parallel_shard_percent"},
		PartKey:  []string{"cluster_id"},
		SortKey:  []string{"external_id", "type"},
	}.init()
)
