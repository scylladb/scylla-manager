// Copyright (C) 2017 ScyllaDB

package main

import (
	"github.com/spf13/cobra"
)

func addScyllaDocsURLToLong(cmd *cobra.Command) {
	decorate := func(text string) string {
		cdu := commandDocsURL(cmd)
		if cdu == "" {
			return text
		}

		return text + "\n\nCommand docs: " + cdu
	}

	if cmd.Long == "" {
		cmd.Long = decorate(cmd.Short)
	} else {
		cmd.Long = decorate(cmd.Long)
	}
}

func commandDocsURL(cmd *cobra.Command) string {
	var (
		command    string
		subcommand string
	)
	if cmd.Parent() == cmd.Root() {
		command = cmd.Name()
	} else {
		subcommand = cmd.Name()

		parent := cmd.Parent()
		if parent.Parent() != cmd.Root() {
			return ""
		}
		command = parent.Name()
	}

	link := command
	if subcommand != "" {
		link += "#" + command + "-" + subcommand
	}

	return docsURL(link)
}

const docsBaseURL = "https://scylladb.github.io/scylla-manager/master"

func docsURL(link string) string {
	return docsBaseURL + "/sctool/" + link
}
