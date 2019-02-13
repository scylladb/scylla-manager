// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
	"net/http"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid"
)

// httpError is a wrapper holding an error, HTTP status code and a user-facing
// message.
type httpError struct {
	Cause      string `json:"cause,omitempty"`
	StatusCode int    `json:"-"`
	Message    string `json:"message"`
	TraceID    string `json:"trace_id"`
}

func (e *httpError) Error() string {
	return e.Cause
}

func respondBadRequest(w http.ResponseWriter, r *http.Request, err error) {
	render.Respond(w, r, &httpError{
		StatusCode: http.StatusBadRequest,
		Message:    fmt.Sprintf("malformed request: %s", err),
		TraceID:    log.TraceID(r.Context()),
	})
}

func respondError(w http.ResponseWriter, r *http.Request, err error, msg string) {
	cause := errors.Cause(err)

	switch {
	case cause == mermaid.ErrNotFound:
		render.Respond(w, r, &httpError{
			StatusCode: http.StatusNotFound,
			Message:    fmt.Sprintf("resource not found: %s", msg),
			TraceID:    log.TraceID(r.Context()),
		})
	case mermaid.IsErrValidate(cause):
		render.Respond(w, r, &httpError{
			StatusCode: http.StatusBadRequest,
			Message:    err.Error(),
			TraceID:    log.TraceID(r.Context()),
		})
	default:
		render.Respond(w, r, &httpError{
			Cause:      cause.Error(),
			StatusCode: http.StatusInternalServerError,
			Message:    msg,
			TraceID:    log.TraceID(r.Context()),
		})
	}
}
