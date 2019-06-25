// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

func init() {
	seedMathRand()
}

func main() {
	log.SetOutput(ioutil.Discard)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(rootCmd.OutOrStderr(), "STARTUP ERROR:\n\n%s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
