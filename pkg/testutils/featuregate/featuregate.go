// Copyright (C) 2025 ScyllaDB

package featuregate

import (
	"strings"

	"github.com/scylladb/scylla-manager/v3/pkg/featuregate"
)

// ScyllaMasterFeatureGate is a special feature gate which assumes that all
// features are available on scylla versions from master branch.
// Otherwise, it works the same as featuregate.ScyllaFeatureGate.
type ScyllaMasterFeatureGate struct{}

// RepairSmallTableOptimization - true.
func (fg ScyllaMasterFeatureGate) RepairSmallTableOptimization(version string) (bool, error) {
	if isScyllaMaster(version) {
		return true, nil
	}
	return featuregate.ScyllaFeatureGate{}.RepairSmallTableOptimization(version)
}

// TabletRepair - true.
func (fg ScyllaMasterFeatureGate) TabletRepair(version string) (bool, error) {
	if isScyllaMaster(version) {
		return true, nil
	}
	return featuregate.ScyllaFeatureGate{}.TabletRepair(version)
}

// SafeDescribeMethodReadBarrierAPI - true.
func (fg ScyllaMasterFeatureGate) SafeDescribeMethodReadBarrierAPI(version string) (bool, error) {
	if isScyllaMaster(version) {
		return true, nil
	}
	return featuregate.ScyllaFeatureGate{}.SafeDescribeMethodReadBarrierAPI(version)
}

// SafeDescribeMethodReadBarrierCQL - true.
func (fg ScyllaMasterFeatureGate) SafeDescribeMethodReadBarrierCQL(version string) (bool, error) {
	if isScyllaMaster(version) {
		return true, nil
	}
	return featuregate.ScyllaFeatureGate{}.SafeDescribeMethodReadBarrierCQL(version)
}

// NativeBackup - true.
func (fg ScyllaMasterFeatureGate) NativeBackup(version string) (bool, error) {
	if isScyllaMaster(version) {
		return true, nil
	}
	return featuregate.ScyllaFeatureGate{}.NativeBackup(version)
}

// NativeRestore - true.
func (fg ScyllaMasterFeatureGate) NativeRestore(version string) (bool, error) {
	if isScyllaMaster(version) {
		return true, nil
	}
	return featuregate.ScyllaFeatureGate{}.NativeRestore(version)
}

// SkipCleanupAndSkipReshape - true.
func (fg ScyllaMasterFeatureGate) SkipCleanupAndSkipReshape(version string) (bool, error) {
	if isScyllaMaster(version) {
		return true, nil
	}
	return featuregate.ScyllaFeatureGate{}.SkipCleanupAndSkipReshape(version)
}

func isScyllaMaster(version string) bool {
	version = strings.Split(version, "-")[0]
	return strings.Contains(version, "dev")
}
