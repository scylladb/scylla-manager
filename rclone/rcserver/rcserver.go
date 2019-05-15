// Copyright (C) 2017 ScyllaDB

// Package rcserver implements the HTTP endpoint to serve the remote control
package rcserver

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/accounting"
	"github.com/ncw/rclone/fs/rc"
	"github.com/pkg/errors"
)

// Server implements http.Handler interface.
type Server struct{}

// New creates new rclone server.
func New() *Server {
	return &Server{}
}

// writeError writes a formatted error to the output.
func writeError(path string, in rc.Params, w http.ResponseWriter, err error, status int) {
	fs.Errorf(nil, "rc: %q: error: %v", path, err)
	// Adjust the error return for some well known errors
	errOrig := errors.Cause(err)
	switch {
	case errOrig == fs.ErrorDirNotFound || errOrig == fs.ErrorObjectNotFound:
		status = http.StatusNotFound
	case rc.IsErrParamInvalid(err) || rc.IsErrParamNotFound(err):
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	err = rc.WriteJSON(w, rc.Params{
		"status": status,
		"error":  err.Error(),
		"input":  in,
		"path":   path,
	})
	if err != nil {
		// can't return the error at this point
		fs.Errorf(nil, "rc: failed to write JSON output: %v", err)
	}
}

// ServeHTTP implements http.Handler interface.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimLeft(r.URL.Path, "/")

	switch r.Method {
	case "POST":
		s.handlePost(w, r, path)
	case "GET", "HEAD":
		s.handleGet(w, r, path)
	default:
		writeError(path, nil, w, errors.Errorf("method %q not allowed", r.Method), http.StatusMethodNotAllowed)
		return
	}
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request, path string) {
	contentType := r.Header.Get("Content-Type")

	values := r.URL.Query()
	if contentType == "application/x-www-form-urlencoded" {
		// Parse the POST and URL parameters into r.Form, for others r.Form will be empty value
		err := r.ParseForm()
		if err != nil {
			writeError(path, nil, w, errors.Wrap(err, "failed to parse form/URL parameters"), http.StatusBadRequest)
			return
		}
		values = r.Form
	}

	// Read the POST and URL parameters into in
	in := make(rc.Params)
	for k, vs := range values {
		if len(vs) > 0 {
			in[k] = vs[len(vs)-1]
		}
	}

	// Parse a JSON blob from the input
	if contentType == "application/json" {
		err := json.NewDecoder(r.Body).Decode(&in)
		if err != nil {
			writeError(path, in, w, errors.Wrap(err, "failed to read input JSON"), http.StatusBadRequest)
			return
		}
	}

	// Find the call
	call := rc.Calls.Get(path)
	if call == nil {
		writeError(path, in, w, errors.Errorf("couldn't find method %q", path), http.StatusNotFound)
		return
	}

	call.Fn = wrapCall(call.Fn)

	// Check to see if it is async or not
	isAsync, err := in.GetBool("_async")
	if rc.NotErrParamNotFound(err) {
		writeError(path, in, w, err, http.StatusBadRequest)
		return
	}

	var out rc.Params
	if isAsync {
		out, err = rc.StartJob(call.Fn, in)
	} else {
		out, err = call.Fn(in)
	}
	if err != nil {
		writeError(path, in, w, err, http.StatusInternalServerError)
		return
	}
	if out == nil {
		out = make(rc.Params)
	}

	err = rc.WriteJSON(w, out)
	if err != nil {
		writeError(path, in, w, err, http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, path string) { //nolint:unparam
	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

// wrapCall wrap functional call with error and stats.
func wrapCall(f rc.Func) rc.Func {
	return func(in rc.Params) (rc.Params, error) {
		var err error
		out, err := f(in)
		if err != nil {
			return out, err
		}
		lastErr := accounting.Stats.GetLastError()
		return out, lastErr
	}
}
