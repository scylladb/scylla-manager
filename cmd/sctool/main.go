// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/scylladb/mermaid/mermaidclient"
)

var ctx = context.Background()

func main() {
	log.SetOutput(ioutil.Discard)

	if err := rootCmd.Execute(); err != nil {
		printError(rootCmd.OutOrStderr(), err)
		os.Exit(1)
	}

	os.Exit(0)
}

func printError(w io.Writer, err error) {
	v, ok := err.(interface {
		GetPayload() *mermaidclient.ErrorResponse
	})
	if ok {
		p := v.GetPayload()
		fmt.Fprintf(w, "Error: failed to %s\n", mermaidclient.FormatMultiHostError(p.Message, " "))
		fmt.Fprintf(w, "Trace ID: %s (for more info grep logs for this)\n", p.TraceID)
	} else {
		fmt.Fprintf(w, "Error: %s\n", err)
	}
	fmt.Fprintln(w)
}
