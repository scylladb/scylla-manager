// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/render"
	"github.com/scylladb/mermaid"
)

// httpError is a wrapper holding an error, HTTP status code and a user-facing
// message.
type httpError struct {
	Err        error  `json:"-"`
	StatusCode int    `json:"-"`
	ErrorText  string `json:"error"` // application-level error message
}

func (e *httpError) Error() string {
	return e.Err.Error()
}

// newHTTPError wraps err with a generic http error with status code status,
// and a user-facing description errText.
func newHTTPError(err error, status int, errText string) *httpError {
	return &httpError{
		Err:        err,
		StatusCode: status,
		ErrorText:  errText,
	}
}

// httpErrNotFound wraps err with a generic http error with status code
// NotFound, and a user-facing description.
func httpErrNotFound(err error) *httpError {
	return &httpError{
		Err:        err,
		StatusCode: http.StatusNotFound,
		ErrorText:  "specified resource not found",
	}
}

// httpErrBadRequest wraps err with a generic http error with status code
// BadRequest, and a user-facing description.
func httpErrBadRequest(err error) *httpError {
	return &httpError{
		Err:        err,
		StatusCode: http.StatusBadRequest,
		ErrorText:  "malformed request",
	}
}

// httpErrInternal wraps err with a generic http error with status code
// StatusInternalServerError.
func httpErrInternal(err error, msg string) *httpError {
	return &httpError{
		Err:        err,
		StatusCode: http.StatusInternalServerError,
		ErrorText:  msg,
	}
}

// notFoundOrInternal coverts mermaid.ErrNotFound to httpErrNotFound and
// everything else to httpErrInternal.
func notFoundOrInternal(w http.ResponseWriter, r *http.Request, err error, msg string) {
	if err == mermaid.ErrNotFound {
		render.Respond(w, r, httpErrNotFound(err))
	} else {
		render.Respond(w, r, httpErrInternal(err, msg))
	}
}
