// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"os"
)

func init() {
	seedMathRand()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(rootCmd.OutOrStderr(), "\nSTARTUP ERROR:%s\n\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
