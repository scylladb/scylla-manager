// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mitchellh/cli"
	"github.com/scylladb/mermaid/command/client"
)

func main() {
	os.Exit(realMain())
}

func realMain() int {
	log.SetOutput(ioutil.Discard)

	// cancel on signal
	ctx, cancel := context.WithCancel(context.Background())

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		_, ok := <-signalCh
		if ok {
			cancel()
		}
	}()
	defer close(signalCh)

	cli := cli.NewCLI("sctool", version)
	cli.Args = os.Args[1:]
	cli.Commands = client.Commands(ctx)

	exitCode, err := cli.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
		return 1
	}

	return exitCode
}
