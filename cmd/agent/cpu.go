// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/cpuset"
	"github.com/spf13/cobra"
)

func pinToCPU(cpu int) ([]int, error) {
	// Get CPUs to run on
	var cpus []int
	if cpu != -1 {
		cpus = []int{cpu}
	} else {
		busy, err := cpuset.ParseScyllaConfigFile()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse Scylla cpuset configuration run scylla_cpuset_setup or set cpu field in the Scylla Manager Agent configuration file")
		}
		cpus, err = cpuset.AvailableCPUs(busy, 1)
		if err != nil {
			return nil, errors.Wrap(err, "failed to pin to CPU")
		}
	}

	if err := cpuset.SchedSetAffinity(cpus); err != nil {
		return nil, errors.Wrapf(err, "failed to pin to CPUs %d", cpus)
	}

	return cpus, nil
}

var cpuTestArgs = struct {
	cpu      int
	duration time.Duration
	parallel int
	sleep    time.Duration
}{}

var cpuTestCmd = &cobra.Command{
	Use:           "cpu-test",
	Short:         "Generate load to observe the CPU consumption by the process and analyse CPU pinning",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	Hidden:        true,

	RunE: func(cmd *cobra.Command, args []string) error {
		cpus, err := pinToCPU(cpuTestArgs.cpu)
		if err != nil {
			return errors.Wrap(err, "failed to pin to CPU")
		}
		fmt.Println("Pinned to CPUs", cpus, "pid", os.Getpid())

		for i := 0; i < cpuTestArgs.parallel; i++ {
			go func(i int) {
				for {
					i = i + 1
					time.Sleep(cpuTestArgs.sleep)
				}
			}(i)
		}
		<-time.NewTimer(cpuTestArgs.duration).C
		return nil
	},
}

func init() {
	f := cpuTestCmd.Flags()
	f.IntVarP(&cpuTestArgs.cpu, "cpu", "c", 0, "CPU to pin to")
	f.DurationVarP(&cpuTestArgs.duration, "duration", "d", 30*time.Second, "test duration")
	f.IntVarP(&cpuTestArgs.parallel, "parallel", "p", 10*runtime.NumCPU(), "number of threads")
	f.DurationVarP(&cpuTestArgs.sleep, "sleep", "s", 10*time.Nanosecond, "sleep in a loop")

	rootCmd.AddCommand(cpuTestCmd)
}
