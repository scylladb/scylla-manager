// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"bytes"
	"io"
	"net/http"
	"regexp"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/pkg/errors"
	rcloneOperations "github.com/scylladb/scylla-manager/v3/swagger/gen/agent/client/operations"
	agentModels "github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
	scylla2ConfigOperations "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v2/client/config"
	scylla2Models "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v2/models"
)

func TestStatusCodeOf(t *testing.T) {
	t.Parallel()

	configNotFound := scylla2ConfigOperations.NewFindConfigAPIAddressDefault(404)
	configNotFound.Payload = &scylla2Models.ErrorModel{Code: 404, Message: "not found"}

	rcloneNotFound := rcloneOperations.NewOperationsListDefault(404)
	rcloneNotFound.Payload = &agentModels.ErrorResponse{Status: 404, Message: "not found"}

	table := []struct {
		Name    string
		Err     error
		Status  int
		Message string
	}{
		{
			Name: "nil",
			Err:  nil,
		},
		{
			Name: "not HTTP error",
			Err:  errors.New("foobar"),
		},
		{
			Name:    "scylla",
			Err:     runtime.NewAPIError("GET", errors.New("not found"), 404),
			Status:  404,
			Message: "not found",
		},
		{
			Name:    "scylla config",
			Err:     configNotFound,
			Status:  404,
			Message: "not found",
		},
		{
			Name:    "rclone",
			Err:     rcloneNotFound,
			Status:  404,
			Message: "not found",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			if s := StatusCodeOf(test.Err); s != test.Status {
				t.Errorf("StatusCodeOf() = %d, expected %d", s, test.Status)
			}
			if s, m := StatusCodeAndMessageOf(test.Err); s != test.Status || m != test.Message {
				t.Errorf("StatusCodeOf() = %d, %s, expected %d, %s", s, m, test.Status, test.Message)
			}
		})
	}
}

func TestAgentError(t *testing.T) {
	p := agentModels.ErrorResponse{
		Status:  400,
		Message: "test",
	}
	b, err := p.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	resp := &http.Response{
		Body: io.NopCloser(bytes.NewReader(b)),
	}

	ae := makeAgentError(resp)

	status, msg := StatusCodeAndMessageOf(ae)
	if status != 400 || msg != "test" {
		t.Fatalf("StatusCodeAndMessageOf() = %d, %s", status, msg)
	}

	if !regexp.MustCompile(`^agent \[HTTP \d+\]`).MatchString(ae.Error()) {
		t.Fatalf("Error = %s not matching expected pattern", ae)
	}
}

func TestAgentErrorStatusCode2XX(t *testing.T) {
	p := agentModels.ErrorResponse{
		Status:  200,
		Message: "test",
	}
	b, err := p.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(b)),
	}

	if err := makeAgentError(resp); err != nil {
		t.Fatal(err)
	}
}
