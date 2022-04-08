// Copyright (C) 2017 ScyllaDB

package scyllaclienttest

import (
	"fmt"

	agentModels "github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

type agentError struct {
	payload *agentModels.ErrorResponse
}

// MakeAgentError creates synthetic errors that can be used with scyllaclient
// Status and StatusAndMessage functions.
func MakeAgentError(payload agentModels.ErrorResponse) error {
	return agentError{
		payload: &payload,
	}
}

func (ae agentError) GetPayload() *agentModels.ErrorResponse {
	return ae.payload
}

func (ae agentError) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", ae.payload.Status, ae.payload.Message)
}
