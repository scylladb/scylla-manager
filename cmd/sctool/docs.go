// Copyright (C) 2017 ScyllaDB

package main

import (
	"strings"

	"github.com/spf13/cobra"
)

const docsVersion = "1.4"

func docsURL(urlPath string) string {
	return "https://docs.scylladb.com/operating-scylla/manager/" + docsVersion + urlPath
}

func withScyllaDocs(cmd *cobra.Command, docs ...string) {
	sb := make([]string, len(docs)+1)
	sb[0] = "Scylla Docs:"
	for i := 0; i < len(docs); i++ {
		sb[1+i] = "  " + docsURL(docs[i])
	}
	tpl := cmd.UsageTemplate() + "\n" + strings.Join(sb, "\n") + "\n"
	cmd.SetUsageTemplate(tpl)
}
