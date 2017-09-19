// Copyright (C) 2017 ScyllaDB

package restapi

import "net/http"

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

// httpErrNotFound wraps err with a generic http error with status code NotFound,
// and a user-facing description.
func httpErrNotFound(err error) *httpError {
	return &httpError{
		Err:        err,
		StatusCode: http.StatusNotFound,
		ErrorText:  "specified resource not found",
	}
}

// httpErrBadRequest wraps err with a generic http error with status code BadRequest,
// and a user-facing description.
func httpErrBadRequest(err error) *httpError {
	return &httpError{
		Err:        err,
		StatusCode: http.StatusBadRequest,
		ErrorText:  "malformed request",
	}
}
