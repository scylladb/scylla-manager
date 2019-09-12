// Copyright (C) 2017 ScyllaDB

package main

import (
	"io"
	"io/ioutil"
	"strings"

	"github.com/scylladb/mermaid/internal/fsutil"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

func render(w io.Writer, d mermaidclient.TableRenderer) error {
	return d.Render(w)
}

func register(cmd *cobra.Command, parent *cobra.Command) {
	// By default do not accept any arguments
	if cmd.Args == nil {
		cmd.Args = cobra.NoArgs
	}
	// Do not print errors, error printing is handled in main
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true

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

// accommodate for escaping of bash expansions, we can safely remove '\'
// as it's not a valid char in keyspace or table name
func unescapeFilters(strs []string) []string {
	for i := range strs {
		strs[i] = strings.Replace(strs[i], "\\", "", -1)
	}
	return strs
}
