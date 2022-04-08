// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
)

// httpError is a wrapper holding an error, HTTP status code and a user-facing
// message.
type httpError struct {
	StatusCode int    `json:"-"`
	Message    string `json:"message"`
	TraceID    string `json:"trace_id"`
}

func (e *httpError) Error() string {
	return e.Message
}

func respondBadRequest(w http.ResponseWriter, r *http.Request, err error) {
	render.Respond(w, r, &httpError{
		StatusCode: http.StatusBadRequest,
		Message:    errors.Wrap(err, "malformed request").Error(),
		TraceID:    log.TraceID(r.Context()),
	})
}

// nolint: errorlint
func respondError(w http.ResponseWriter, r *http.Request, err error) {
	cause := errors.Cause(err)

	switch {
	case cause == service.ErrNotFound:
		render.Respond(w, r, &httpError{
			StatusCode: http.StatusNotFound,
			Message:    errors.Wrap(err, "get resource").Error(),
			TraceID:    log.TraceID(r.Context()),
		})
	case service.IsErrValidate(cause):
		render.Respond(w, r, &httpError{
			StatusCode: http.StatusBadRequest,
			Message:    err.Error(),
			TraceID:    log.TraceID(r.Context()),
		})
	default:
		render.Respond(w, r, &httpError{
			StatusCode: http.StatusInternalServerError,
			Message:    err.Error(),
			TraceID:    log.TraceID(r.Context()),
		})
	}
}
