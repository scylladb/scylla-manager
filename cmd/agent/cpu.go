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

func findFreeCPU() (int, error) {
	busy, err := cpuset.ParseScyllaConfigFile()
	if err != nil {
		return noCPU, errors.Wrapf(err, "failed to parse Scylla cpuset configuration, "+
			"run scylla_cpuset_setup or set cpu field in the Scylla Manager Agent configuration file")
	}
	cpus, err := cpuset.AvailableCPUs(busy, 1)
	if err != nil {
		return noCPU, err
	}
	return cpus[0], nil
}

func pinToCPU(cpu int) error {
	return cpuset.SchedSetAffinity([]int{cpu})
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
		var cpu = cpuTestArgs.cpu
		if cpu >= 0 {
			if err := pinToCPU(cpu); err != nil {
				return errors.Wrapf(err, "failed to pin to CPU %d", cpu)
			}
			fmt.Println("Pinned to CPU", cpuTestArgs.cpu, "pid", os.Getpid())
		} else {
			fmt.Println("Running on all CPUs", "pid", os.Getpid())
		}

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
