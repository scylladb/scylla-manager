// Copyright (C) 2017 ScyllaDB

// Package rcserver implements the HTTP endpoint to serve the remote control
package rcserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/accounting"
	"github.com/ncw/rclone/fs/filter"
	"github.com/ncw/rclone/fs/fshttp"
	"github.com/ncw/rclone/fs/rc"
	"github.com/ncw/rclone/fs/rc/jobs"
	"github.com/pkg/errors"
)

var initOnce sync.Once

// Server implements http.Handler interface.
type Server struct{}

// New creates new rclone server.
// Since we are overriding default behavior of saving remote configuration to
// files, we need to include code that was called in
// rclone/fs/config.LoadConfig, which initializes accounting processes but is
// no longer called.
// It's probably done this way to make sure that configuration has opportunity
// to modify global config object before these processes are started as they
// depend on it.
// We are initializing it once here to make sure it's executed only when server
// is needed and configuration is completely loaded.
func New() *Server {
	initOnce.Do(func() {
		// Start the token bucket limiter
		accounting.StartTokenBucket()
		// Start the bandwidth update ticker
		accounting.StartTokenTicker()
		// Start the transactions per second limiter
		fshttp.StartHTTPTokenBucket()
	})

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
	w.Header().Set("Content-Type", "application/json")

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

	exclude, err := in.Get("exclude")
	if rc.NotErrParamNotFound(err) {
		writeError(path, in, w, err, http.StatusBadRequest)
		return
	}
	if exclude != nil {
		e, ok := exclude.([]interface{})
		if !ok {
			writeError(path, in, w, errors.New("exclude should be list of strings"), http.StatusBadRequest)
			return
		}
		for i := range e {
			v, ok := e[i].(string)
			if !ok {
				writeError(path, in, w, errors.New("exclude should be list of strings"), http.StatusBadRequest)
			}
			if err := filter.Active.Add(false, v); err != nil {
				writeError(path, in, w, errors.Wrap(err, "failed to read exclude pattern"), http.StatusBadRequest)
				return
			}
		}
	}

	// Find the call
	call := rc.Calls.Get(path)
	if call == nil {
		writeError(path, in, w, errors.Errorf("couldn't find method %q", path), http.StatusNotFound)
		return
	}

	// Check to see if it is async or not
	isAsync, err := in.GetBool("_async")
	if rc.NotErrParamNotFound(err) {
		writeError(path, in, w, err, http.StatusBadRequest)
		return
	}

	var out rc.Params
	if isAsync {
		out, err = jobs.StartAsyncJob(call.Fn, in)
	} else {
		var jobID int64
		out, jobID, err = jobs.ExecuteJob(r.Context(), call.Fn, in)
		w.Header().Add("x-rclone-jobid", fmt.Sprintf("%d", jobID))
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
