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

// respondError coverts mermaid.ErrNotFound to httpErrNotFound and
// everything else to httpErrInternal.
func respondError(w http.ResponseWriter, r *http.Request, err error, msg string) {
	if err == mermaid.ErrNotFound {
		render.Respond(w, r, &httpError{
			Err:        err,
			StatusCode: http.StatusNotFound,
			Message:    "specified resource not found",
			TraceID:    log.TraceID(r.Context()),
		})
		return
	}

	switch err.(type) {
	case mermaid.ParamError:
		render.Respond(w, r, &httpError{
			Err:        err,
			StatusCode: http.StatusBadRequest,
			Message:    err.Error(),
			TraceID:    log.TraceID(r.Context()),
		})
	default:
		render.Respond(w, r, &httpError{
			Err:        err,
			StatusCode: http.StatusInternalServerError,
			Message:    msg,
			TraceID:    log.TraceID(r.Context()),
		})
	}
}
