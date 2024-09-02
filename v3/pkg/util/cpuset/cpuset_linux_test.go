// Copyright (C) 2017 ScyllaDB
//go:build linux
// +build linux

package cpuset

import (
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/sys/unix"
)

func TestAvailableCPUs(t *testing.T) {
	t.Parallel()

	var cpus unix.CPUSet
	if err := unix.SchedGetaffinity(0, &cpus); err != nil {
		t.Fatal(err)
	}
	if cpus.Count() < 2 {
		t.Skip("Not enough CPUs")
	}

	c := cpus.Count()
	a, err := AvailableCPUs([]int{0})
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != c-1 {
		t.Fatal("expected", c-1, "got", len(a))
	}
	if a[0] == 0 {
		t.Fatal("expected CPU with a high index got 0")
	}
}

func TestSchedSetAffinity(t *testing.T) {
	var cpus unix.CPUSet
	if err := unix.SchedGetaffinity(0, &cpus); err != nil {
		t.Fatal(err)
	}
	if cpus.Count() < 2 {
		t.Skip("Not enough CPUs")
	}

	if err := SchedSetAffinity([]int{0}); err != nil {
		t.Fatal(err)
	}

	if err := unix.SchedGetaffinity(0, &cpus); err != nil {
		t.Fatal(err)
	}
	if cpus.Count() != 1 {
		t.Fatal("expected", 1, "got", cpus.Count())
	}
}

func TestCPUSetCPUList(t *testing.T) {
	t.Parallel()

	cpus := []int{0, 1, 2, 3, 5}
	if diff := cmp.Diff(cpulist(cpuset(cpus)), cpus); diff != "" {
		t.Fatal(diff)
	}
}

func TestOsTasks(t *testing.T) {
	t.Parallel()

	pids, err := osTasks(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if len(pids) == 0 {
		t.Fatal("expected pids")
	}
	for _, p := range pids {
		if p == 0 {
			t.Fatal("invalid pid 0")
		}
	}
}
