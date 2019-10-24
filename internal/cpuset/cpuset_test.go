// Copyright (C) 2017 ScyllaDB

package cpuset

import (
	"os"
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/sys/unix"
)

func TestCpuSetPattern(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name  string
		Line  string
		Match bool
	}{
		{
			Name:  "line commented with space",
			Line:  `# CPUSET="--cpuset 0 --smp 1"`,
			Match: false,
		},
		{
			Name:  "line commented without space",
			Line:  `#CPUSET="--cpuset 0 --smp 1"`,
			Match: false,
		},
		{
			Name:  "cpuset without space",
			Line:  `CPUSET="--cpuset 0 --smp 1"`,
			Match: true,
		},
		{
			Name:  "cpuset with space",
			Line:  ` CPUSET="--cpuset 0 --smp 1"`,
			Match: true,
		},
		{
			Name:  "cpuset no smp",
			Line:  `CPUSET="--cpuset 0"`,
			Match: true,
		},
		{
			Name:  "cpuset no cpuset",
			Line:  `CPUSET="--smp 1"`,
			Match: true,
		},
		{
			Name:  "cpuset advanced",
			Line:  `CPUSET="--cpuset 0-10,15,17-20"`,
			Match: true,
		},
		{
			Name:  "cpuset broken",
			Line:  `CPUSET="--cpuset 0foo1bar"`,
			Match: false,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			p := regexp.MustCompile(cpuSetPattern)
			m := p.MatchString(test.Line)
			if test.Match && !m {
				t.Errorf("Expected a match")
			}
			if !test.Match && m {
				t.Errorf("Expected NOT a match")
			}
		})
	}
}

func TestParseCpuSet(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name  string
		Param string
		CPUs  []int
	}{
		{
			Name:  "list of values",
			Param: "1,2,4",
			CPUs:  []int{1, 2, 4},
		},
		{
			Name:  "range of values",
			Param: "1-2,4",
			CPUs:  []int{1, 2, 4},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			l, err := parseCPUSet(test.Param)
			if err != nil {
				t.Error(err)
			}
			if diff := cmp.Diff(l, test.CPUs); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestParseConfigFile(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name string
		File string
		CPUs []int
		Err  string
	}{
		{
			Name: "default",
			File: "testdata/cpuset_default.conf",
			Err:  "no CPUSET configuration",
		},
		{
			Name: "sMP only",
			File: "testdata/cpuset_smp_only.conf",
			Err:  "CPUSET configuration is missing cpuset flag",
		},
		{
			Name: "multiline",
			File: "testdata/cpuset_multiline.conf",
			CPUs: []int{2},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			l, err := parseConfigFile(test.File)
			if test.Err != "" {
				if err == nil || err.Error() != test.Err {
					t.Error("expected", test.Err, "got", err)
				}
			}
			if diff := cmp.Diff(l, test.CPUs); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestAvailableCPUs(t *testing.T) {
	var cpus unix.CPUSet
	if err := unix.SchedGetaffinity(0, &cpus); err != nil {
		t.Fatal(err)
	}
	if cpus.Count() < 2 {
		t.Skip("Not enough CPUs")
	}

	a, err := AvailableCPUs([]int{0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 1 {
		t.Fatal("expected", 1, "got", len(a))
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

func TestOsTasks(t *testing.T) {
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

func TestCPUSetCPUList(t *testing.T) {
	cpus := []int{0, 1, 2, 3, 5}
	if diff := cmp.Diff(cpulist(cpuset(cpus)), cpus); diff != "" {
		t.Fatal(diff)
	}
}
