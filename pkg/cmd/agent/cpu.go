// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/cpuset"
	"github.com/spf13/cobra"
)

func findFreeCPUs() ([]int, error) {
	busy, err := cpuset.ParseScyllaConfigFile()
	if err != nil {
		return nil, errors.Wrapf(err, "cpuset parse error")
	}
	cpus, err := cpuset.AvailableCPUs(busy)
	if err != nil {
		return nil, err
	}
	return cpus, nil
}

func pinToCPUs(cpus []int) error {
	return cpuset.SchedSetAffinity(cpus)
}

var cpuTestArgs = struct {
	cpus     []int
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
		cpus := cpuTestArgs.cpus
		if len(cpus) == 0 {
			fmt.Println("Running on all CPUs", "pid", os.Getpid())
			runtime.GOMAXPROCS(1)
		} else {
			if err := pinToCPUs(cpus); err != nil {
				return errors.Wrapf(err, "pin to CPUs %v", cpus)
			}
			fmt.Println("Pinned to CPUs", cpuTestArgs.cpus, "pid", os.Getpid())
			runtime.GOMAXPROCS(len(cpus))
		}

		for i := 0; i < cpuTestArgs.parallel; i++ {
			go func(i int) {
				for {
					i++
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
	f.IntSliceVarP(&cpuTestArgs.cpus, "cpu", "c", nil, "CPUs to pin to")
	f.DurationVarP(&cpuTestArgs.duration, "duration", "d", 30*time.Second, "test duration")
	f.IntVarP(&cpuTestArgs.parallel, "parallel", "p", 10*runtime.NumCPU(), "number of threads")
	f.DurationVarP(&cpuTestArgs.sleep, "sleep", "s", 10*time.Nanosecond, "sleep in a loop")

	rootCmd.AddCommand(cpuTestCmd)
}
