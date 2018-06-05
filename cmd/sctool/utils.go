// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/scylladb/mermaid/uuid"
	"github.com/spf13/cobra"
)

func register(cmd *cobra.Command, parent *cobra.Command) {
	// fix defaults
	if cmd.Args == nil {
		cmd.Args = cobra.NoArgs
	}
	parent.AddCommand(cmd)
}

func requireFlags(cmd *cobra.Command, flags ...string) {
	for _, f := range flags {
		cmd.MarkFlagRequired(f)
	}
}

func taskSplit(s string) (taskType string, taskID uuid.UUID, err error) {
	i := strings.LastIndex(s, "/")
	if i != -1 {
		taskType = s[:i]
	}
	taskID, err = uuid.Parse(s[i+1:])
	return
}

func taskJoin(taskType string, taskID interface{}) string {
	return fmt.Sprint(taskType, "/", taskID)
}

func dumpMap(m map[string]interface{}) string {
	if len(m) == 0 {
		return "-"
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	s := make([]string, 0, len(m))
	for _, k := range keys {
		s = append(s, fmt.Sprint(k, ":", m[k]))
	}
	return strings.Join(s, ", ")
}
