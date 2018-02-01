// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"
)

var ctx = context.Background()

func main() {
	log.SetOutput(ioutil.Discard)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}

	os.Exit(0)
}
