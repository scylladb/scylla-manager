// Copyright (C) 2017 ScyllaDB

package main

import (
	"io"
	"io/ioutil"

	"github.com/scylladb/mermaid/internal/fsutil"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

func render(w io.Writer, d mermaidclient.TableRenderer) error {
	return d.Render(w)
}

func register(cmd *cobra.Command, parent *cobra.Command) {
	// fix defaults
	if cmd.Args == nil {
		cmd.Args = cobra.NoArgs
	}
	parent.AddCommand(cmd)
}

func requireFlags(cmd *cobra.Command, flags ...string) {
	for _, f := range flags {
		if err := cmd.MarkFlagRequired(f); err != nil {
			panic(err)
		}
	}
}

func readFile(filename string) ([]byte, error) {
	f, err := fsutil.ExpandPath(filename)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(f)
}
