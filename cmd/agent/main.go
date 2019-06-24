// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"os"
	"runtime"
)

func main() {
	// Inform Go to execute on one CPU, for CPU pinning read below...
	runtime.GOMAXPROCS(1)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(rootCmd.OutOrStderr(), "STARTUP ERROR:\n\n%s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
