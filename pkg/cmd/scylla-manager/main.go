// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"syscall"
	"time"
)

func init() {
	seedMathRand()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(rootCmd.OutOrStderr(), "\nSTARTUP ERROR: %s\n\n", err)

		// Due to a bug in systemd [1] last log messages for failed processes
		// are lost. They are still visible with journalctl -xe but not all
		// users know that. To make the logs visible in systemctl status we wait
		// here for a bit over a second before exiting.
		//
		// [1] https://github.com/systemd/systemd/issues/2913
		time.Sleep(1100 * time.Millisecond)

		syscall.Exit(1)
	}

	syscall.Exit(0)
}
