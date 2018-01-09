// Copyright (C) 2017 ScyllaDB

package main

import (
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
		os.Exit(1)
	}

	os.Exit(0)
}
