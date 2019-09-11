// Copyright (C) 2017 ScyllaDB

package cpuset

import (
	"golang.org/x/sys/unix"
	"os"
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestCpuSetPattern(t *testing.T) {
	table := []struct {
		Name  string
		Line  string
		Match bool
	}{
		{
			Name:  "Line commented with space",
			Line:  `# CPUSET="--cpuset 0 --smp 1"`,
			Match: false,
		},
		{
			Name:  "Line commented without space",
			Line:  `#CPUSET="--cpuset 0 --smp 1"`,
			Match: false,
		},
		{
			Name:  "CPUSET without space",
			Line:  `CPUSET="--cpuset 0 --smp 1"`,
			Match: true,
		},
		{
			Name:  "CPUSET with space",
			Line:  ` CPUSET="--cpuset 0 --smp 1"`,
			Match: true,
		},
		{
			Name:  "CPUSET no smp",
			Line:  `CPUSET="--cpuset 0"`,
			Match: true,
		},
		{
			Name:  "CPUSET no cpuset",
			Line:  `CPUSET="--smp 1"`,
			Match: true,
		},
		{
			Name:  "CPUSET advanced",
			Line:  `CPUSET="--cpuset 0-10,15,17-20"`,
			Match: true,
		},
		{
			Name:  "CPUSET broken",
			Line:  `CPUSET="--cpuset 0foo1bar"`,
			Match: false,
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			p := regexp.MustCompile(cpuSetPattern)
			m := p.MatchString(test.Line)
			if test.Match && !m {
				t.Errorf("expected a match")
			}
			if !test.Match && m {
				t.Errorf("expected NOT a match")
			}
		})
	}
}

func TestParseCpuSet(t *testing.T) {
	table := []struct {
		Name  string
		Param string
		CPUs  []int
	}{
		{
			Name:  "List of values",
			Param: "1,2,4",
			CPUs:  []int{1, 2, 4},
		},
		{
			Name:  "Range of values",
			Param: "1-2,4",
			CPUs:  []int{1, 2, 4},
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
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
	table := []struct {
		Name string
		File string
		CPUs []int
		Err  string
	}{
		{
			Name: "Default",
			File: "testdata/cpuset_default.conf",
			Err:  "no CPUSET configuration",
		},
		{
			Name: "SMP only",
			File: "testdata/cpuset_smp_only.conf",
			Err:  "CPUSET configuration is missing cpuset flag",
		},
		{
			Name: "Multiline",
			File: "testdata/cpuset_multiline.conf",
			CPUs: []int{2},
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
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
