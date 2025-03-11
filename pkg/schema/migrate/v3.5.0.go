// Copyright (C) 2025 ScyllaDB

package migrate

import (
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/nopmigrate"
)

func init() {
	reg.Add(migrate.CallComment, "MigrateToRestoreRunProgressWithSortKeyWithScyllaTaskID", nopmigrate.MigrateToRestoreRunProgressWithSortKeyWithScyllaTaskID)
}
