//go:build tools
// +build tools

package main

import (
	_ "github.com/golang/mock/mockgen"
	_ "github.com/scylladb/gocqlx/v2/cmd/schemagen"
	_ "github.com/stoewer/go-strcase"
)
