// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/scylladb/scylla-manager/pkg/managerclient"
)

var ctx = context.Background()

func main() {
	log.SetOutput(io.Discard)

	if err := rootCmd.Execute(); err != nil {
		printError(rootCmd.OutOrStderr(), err)
		os.Exit(1)
	}

	os.Exit(0)
}

func printError(w io.Writer, err error) {
	v, ok := err.(interface { // nolint: errorlint
		GetPayload() *managerclient.ErrorResponse
	})
	if ok {
		p := v.GetPayload()

		fmt.Fprintf(w, "Error: %s\n", managerclient.FormatError(p.Message))
		fmt.Fprintf(w, "Trace ID: %s (grep in scylla-manager logs)\n", p.TraceID)
	} else {
		fmt.Fprintf(w, "Error: %s\n", err)
	}
	fmt.Fprintln(w)
}
