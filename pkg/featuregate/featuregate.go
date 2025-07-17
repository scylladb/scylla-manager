// Copyright (C) 2025 ScyllaDB

package featuregate

import (
	"github.com/pkg/errors"
	scyllaversion "github.com/scylladb/scylla-manager/v3/pkg/util/version"
)

// ScyllaFeatureGate is a helper for checking scylla feature availability based on scylla version.
type ScyllaFeatureGate struct{}

// RepairSmallTableOptimization - scylla supports small_table_optimization param in /storage_service/repair_async/{keyspace} API.
func (fg ScyllaFeatureGate) RepairSmallTableOptimization(version string) (bool, error) {
	return checkConstraints(version, ">= 6.0, < 2000", ">= 2024.1.5")
}

// TabletRepair - scylla exposes /storage_service/tablets/repair API.
func (fg ScyllaFeatureGate) TabletRepair(version string) (bool, error) {
	return checkConstraints(version, ">= 2025.1")
}

// SafeDescribeMethodReadBarrierAPI - scylla exposes read barrier API.
func (fg ScyllaFeatureGate) SafeDescribeMethodReadBarrierAPI(version string) (bool, error) {
	return checkConstraints(version, ">= 6.1, < 2000", ">= 2025.1")
}

// SafeDescribeMethodReadBarrierCQL - scylla CQL read barrier can be used.
func (fg ScyllaFeatureGate) SafeDescribeMethodReadBarrierCQL(version string) (bool, error) {
	return checkConstraints(version, ">= 6.0, < 2000", ">= 2024.2, > 1000")
}

// NativeBackup - scylla exposes /storage_service/backup API.
func (fg ScyllaFeatureGate) NativeBackup(version string) (bool, error) {
	return checkConstraints(version, ">= 2025.2")
}

// NativeRestore - scylla exposes /storage_service/restore API.
func (fg ScyllaFeatureGate) NativeRestore(version string) (bool, error) {
	return checkConstraints(version, ">= 2025.3")
}

// SkipCleanupAndSkipReshape - scylla supports skip_cleanup and skip_reshape params in /storage_service/sstables/{keyspace} API.
// Note that scylla 2025.2.0 has a bug if these params are set to true - https://github.com/scylladb/scylladb/issues/24913.
func (fg ScyllaFeatureGate) SkipCleanupAndSkipReshape(version string) (bool, error) {
	return checkConstraints(version, ">= 2025.2.0")
}

func checkConstraints(version string, constraints ...string) (bool, error) {
	for _, constraint := range constraints {
		supports, err := scyllaversion.CheckConstraint(version, constraint)
		if err != nil {
			return false, errors.Wrapf(err, "unknown scylla version: %s", version)
		}
		if supports {
			return true, nil
		}
	}
	return false, nil
}
