// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
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

// newHTTPError wraps err with a generic http error with status code status,
// and a user-facing description errText.
func newHTTPError(r *http.Request, err error, status int, msg string) *httpError {
	message := fmt.Sprintf("%s: %s", msg, err)
	if err == nil {
		message = msg
	}

	return &httpError{
		Err:        err,
		StatusCode: status,
		Message:    message,
		TraceID:    log.TraceID(r.Context()),
	}
}

// httpErrNotFound wraps err with a generic http error with status code
// NotFound, and a user-facing description.
func httpErrNotFound(r *http.Request, err error) *httpError {
	return newHTTPError(r, err, http.StatusNotFound, "specified resource not found")
}

// httpErrBadRequest wraps err with a generic http error with status code
// BadRequest, and a user-facing description.
func httpErrBadRequest(r *http.Request, err error) *httpError {
	return newHTTPError(r, err, http.StatusBadRequest, "malformed request")
}

// httpErrInternal wraps err with a generic http error with status code
// StatusInternalServerError.
func httpErrInternal(r *http.Request, err error, msg string) *httpError {
	return newHTTPError(r, err, http.StatusInternalServerError, msg)
}

// notFoundOrInternal coverts mermaid.ErrNotFound to httpErrNotFound and
// everything else to httpErrInternal.
func notFoundOrInternal(w http.ResponseWriter, r *http.Request, err error, msg string) {
	if err == mermaid.ErrNotFound {
		render.Respond(w, r, httpErrNotFound(r, err))
	} else {
		render.Respond(w, r, httpErrInternal(r, err, msg))
	}
}
