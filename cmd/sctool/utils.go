// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/scylladb/mermaid/uuid"
	"github.com/spf13/cobra"
)

func withoutArgs(cmd *cobra.Command) *cobra.Command {
	cmd.Args = cobra.NoArgs
	return cmd
}

func subcommand(cmd *cobra.Command, parent *cobra.Command) {
	parent.AddCommand(cmd)
}

func requireFlags(cmd *cobra.Command, flags ...string) {
	for _, f := range flags {
		cmd.MarkFlagRequired(f)
	}
}

func parseTaskStartDate(startDate string) (time.Time, error) {
	const nowSafety = 30 * time.Second

	if strings.HasPrefix(startDate, "now") {
		now := time.Now()
		var d time.Duration
		if startDate != "now" {
			var err error
			d, err = time.ParseDuration(startDate[3:])
			if err != nil {
				return time.Time{}, err
			}
		}

		activation := now.Add(d)
		if activation.Before(now.Add(nowSafety)) {
			activation = now.Add(nowSafety)
		}
		return activation.UTC(), nil
	}

	t, err := time.Parse(time.RFC3339, startDate)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
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

func dumpMap(m map[string]string) string {
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
