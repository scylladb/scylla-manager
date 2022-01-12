// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"testing"

	"github.com/spf13/cobra"
)

func TestCommandTree(t *testing.T) {
	printCommandTree(buildCommand(), "")

}

func printCommandTree(cmd *cobra.Command, prefix string) {
	s := fmt.Sprint(prefix, " ", cmd.Name())
	if cmd.Runnable() {
		if cmd.Deprecated == "" {
			fmt.Println(s)
		} else {
			fmt.Println("DEPRECATED ", s)
		}

	}
	for _, c := range cmd.Commands() {
		printCommandTree(c, s)
	}
}
