// Copyright (C) 2017 ScyllaDB

// Package rcserver implements the HTTP endpoint to serve the remote control
package rcserver

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/rc"
	"github.com/scylladb/mermaid/rclone/jobs"
)

var initOnce sync.Once

// Server implements http.Handler interface.
type Server struct {
	mu     sync.Mutex
	filter *filter.Filter
}

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

	return &Server{
		filter: filter.Active,
	}
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

const bodySizeLimit int64 = 1024 * 1024

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
		j, err := ioutil.ReadAll(&io.LimitedReader{R: r.Body, N: bodySizeLimit})
		if err != nil {
			writeError(path, in, w, errors.Wrap(err, "failed to read request body"), http.StatusBadRequest)
			return
		}
		if len(j) > 0 {
			if err := json.Unmarshal(j, &in); err != nil {
				writeError(path, in, w, errors.Wrap(err, "failed to read input JSON"), http.StatusBadRequest)
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
	fn := call.Fn
	exclude, err := getExclude(in)
	if err != nil {
		writeError(path, in, w, err, http.StatusBadRequest)
		return
	}
	if len(exclude) > 0 {
		// File filter is global variable and this is a workaround to serialize
		// access to it.
		// In the current setup server is the only access point to this
		// variable so we are blocking access to it until underlying function
		// is complete.
		// In the use of the API this is not a big problem because parameter
		// use is always synchronous but we are protecting it to be safe.
		// The change is needed upstream to remove this.
		fn = func(fn rc.Func) rc.Func {
			return func(ctx context.Context, in rc.Params) (rc.Params, error) {
				s.mu.Lock()
				defer func() {
					s.filter.Clear()
					s.mu.Unlock()
				}()
				for _, e := range exclude {
					if err := s.filter.Add(false, e); err != nil {
						return nil, errors.Wrap(err, "failed to set exclude pattern")
					}
				}
				return fn(ctx, in)
			}
		}(fn)
	}

	// Check to see if it is async or not
	isAsync, err := in.GetBool("_async")
	if rc.NotErrParamNotFound(err) {
		writeError(path, in, w, err, http.StatusBadRequest)
		return
	}

	fs.Debugf(nil, "rc: %q: with parameters %+v", path, in)
	var out rc.Params
	if isAsync {
		out, err = jobs.StartAsyncJob(fn, in)
	} else {
		var jobID string
		out, jobID, err = jobs.ExecuteJob(r.Context(), fn, in)
		w.Header().Add("x-rclone-jobid", jobID)
	}
	if err != nil {
		writeError(path, in, w, err, http.StatusInternalServerError)
		return
	}
	if out == nil {
		out = make(rc.Params)
	}

	fs.Debugf(nil, "rc: %q: reply %+v: %v", path, out, err)
	err = rc.WriteJSON(w, out)
	if err != nil {
		writeError(path, in, w, err, http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, path string) { //nolint:unparam
	fs.Errorf(nil, "rc: received unsupported GET request")
	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func getExclude(in rc.Params) ([]string, error) {
	exclude, err := in.Get("exclude")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}
	var out []string
	if exclude != nil {
		e, ok := exclude.([]interface{})
		if !ok {
			return nil, errors.New("exclude should be list of strings")
		}
		for i := range e {
			v, ok := e[i].(string)
			if !ok {
				return nil, errors.New("exclude should be list of strings")
			}
			out = append(out, v)
		}
	}
	return out, nil
}
