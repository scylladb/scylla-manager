// Copyright (C) 2017 ScyllaDB
// +build !linux

package cpuset

// AvailableCPUs on non-linux systems will simply return the
// same CPU list that it receives, in the future this can
// expanded to other unix systems.
func AvailableCPUs(cpus []int) ([]int, error) {
	return cpus, nil
}

// SchedSetAffinity on non-linux systems will do nothing.
func SchedSetAffinity(useCPUs []int) error {
	return nil
}
