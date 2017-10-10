// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/mitchellh/cli"
	"github.com/scylladb/mermaid/command/client"
)

func main() {
	os.Exit(realMain())
}

func realMain() int {
	log.SetOutput(ioutil.Discard)

	cli := cli.NewCLI("sctool", version)
	cli.Args = os.Args[1:]
	cli.Commands = client.Commands()

	exitCode, err := cli.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
		return 1
	}

	return exitCode
}
