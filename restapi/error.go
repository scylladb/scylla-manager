// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/render"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/log"
)

// httpError is a wrapper holding an error, HTTP status code and a user-facing
// message.
type httpError struct {
	Err        error  `json:"-"`
	StatusCode int    `json:"-"`
	Message    string `json:"message"`
	TraceID    string `json:"trace_id"`
}

func (e *httpError) Error() string {
	return e.Err.Error()
}

// httpErrNotFound wraps err with a generic http error with status code
// NotFound, and a user-facing description.
func httpErrNotFound(r *http.Request, err error) *httpError {
	return &httpError{
		Err:        err,
		StatusCode: http.StatusNotFound,
		Message:    "specified resource not found",
		TraceID:    log.TraceID(r.Context()),
	}
}

// httpErrBadRequest wraps err with a generic http error with status code
// BadRequest, and a user-facing description.
func httpErrBadRequest(r *http.Request, err error) *httpError {
	return &httpError{
		Err:        err,
		StatusCode: http.StatusBadRequest,
		Message:    "malformed request",
		TraceID:    log.TraceID(r.Context()),
	}
}

// notFoundOrInternal coverts mermaid.ErrNotFound to httpErrNotFound and
// everything else to httpErrInternal.
func notFoundOrInternal(w http.ResponseWriter, r *http.Request, err error, msg string) {
	if err == mermaid.ErrNotFound {
		render.Respond(w, r, httpErrNotFound(r, err))
	} else {
		render.Respond(w, r, &httpError{
			Err:        err,
			StatusCode: http.StatusInternalServerError,
			Message:    msg,
			TraceID:    log.TraceID(r.Context()),
		})
	}
}
