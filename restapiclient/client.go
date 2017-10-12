// Copyright (C) 2017 ScyllaDB

package restapiclient

import (
	api "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/scylladb/mermaid/restapiclient/client/operations"
)

//go:generate swagger generate client -f ../swagger/restapi.json -t .

// New returns a new mermaid rest API client.
func New(host string) *operations.Client {
	return operations.New(api.New(host, "/api/v1", []string{"http"}), strfmt.Default)
}
