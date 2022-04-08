// Copyright (C) 2017 ScyllaDB

package httphandler

import (
	"net/http"

	"github.com/go-chi/render"
	"github.com/scylladb/scylla-manager/v3/pkg"
)

type version struct {
	Version string `json:"version"`
}

// Version responds with application version.
func Version() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		render.Respond(w, r, version{Version: pkg.Version()})
	}
}
