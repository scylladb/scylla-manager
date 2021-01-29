// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/pkg/errors"
	agentModels "github.com/scylladb/scylla-manager/swagger/gen/agent/models"
	scyllaModels "github.com/scylladb/scylla-manager/swagger/gen/scylla/v1/models"
	scylla2Models "github.com/scylladb/scylla-manager/swagger/gen/scylla/v2/models"
)

// StatusCodeAndMessageOf returns HTTP status code and it's message carried
// by the error or it's cause.
// If not status can be found it returns 0.
func StatusCodeAndMessageOf(err error) (status int, message string) {
	cause := errors.Cause(err)
	switch v := cause.(type) { // nolint: errorlint
	case *runtime.APIError:
		return v.Code, fmt.Sprint(v.Response)
	case interface {
		GetPayload() *agentModels.ErrorResponse
	}:
		p := v.GetPayload()
		if p != nil {
			return int(p.Status), p.Message
		}
	case interface {
		GetPayload() *scylla2Models.ErrorModel
	}:
		p := v.GetPayload()
		if p != nil {
			return int(p.Code), p.Message
		}
	case interface {
		GetPayload() *scyllaModels.ErrorModel
	}:
		p := v.GetPayload()
		if p != nil {
			return int(p.Code), p.Message
		}

	case interface {
		Code() int
	}:
		return v.Code(), ""
	}

	return 0, ""
}

// StatusCodeOf returns HTTP status code carried by the error or it's cause.
// If not status can be found it returns 0.
func StatusCodeOf(err error) int {
	s, _ := StatusCodeAndMessageOf(err)
	return s
}
