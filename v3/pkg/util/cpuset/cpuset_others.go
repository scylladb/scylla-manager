// Copyright (C) 2017 ScyllaDB
//go:build !linux
// +build !linux

package cpuset

// AvailableCPUs is a no-op implementation for all systems but Linux.
// This is intended for development purposes only.
// Scylla Manager supports only Linux OS.
func AvailableCPUs(cpus []int) ([]int, error) {
	return cpus, nil
}

// SchedSetAffinity is a no-op implementation for all systems but Linux.
// This is intended for development purposes only.
// Scylla Manager supports only Linux OS.
func SchedSetAffinity(useCPUs []int) error {
	return nil
}
