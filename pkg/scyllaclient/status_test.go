// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/pkg/errors"
	rcloneOperations "github.com/scylladb/mermaid/pkg/scyllaclient/internal/agent/client/operations"
	agentModels "github.com/scylladb/mermaid/pkg/scyllaclient/internal/agent/models"
	scylla2ConfigOperations "github.com/scylladb/mermaid/pkg/scyllaclient/internal/scylla_v2/client/config"
	scylla2Models "github.com/scylladb/mermaid/pkg/scyllaclient/internal/scylla_v2/models"
)

func TestStatusCodeOf(t *testing.T) {
	t.Parallel()

	configNotFound := scylla2ConfigOperations.NewFindConfigAPIAddressDefault(404)
	configNotFound.Payload = &scylla2Models.ErrorModel{Code: 404}

	rcloneNotFound := rcloneOperations.NewOperationsListDefault(404)
	rcloneNotFound.Payload = &agentModels.ErrorResponse{Status: 404}

	table := []struct {
		Name   string
		Err    error
		Status int
	}{
		{
			Name:   "nil",
			Err:    nil,
			Status: 0,
		},
		{
			Name:   "not HTTP error",
			Err:    errors.New("foobar"),
			Status: 0,
		},
		{
			Name:   "scylla",
			Err:    runtime.NewAPIError("GET", errors.New("foobar"), 404),
			Status: 404,
		},
		{
			Name:   "scylla config",
			Err:    configNotFound,
			Status: 404,
		},
		{
			Name:   "rclone",
			Err:    rcloneNotFound,
			Status: 404,
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			if s := StatusCodeOf(test.Err); s != test.Status {
				t.Error("expected", test.Status, "got", s)
			}
		})
	}
}
