// Copyright (C) 2017 ScyllaDB

package httpmw

import (
	"net/http"

	"github.com/go-chi/render"
	"github.com/scylladb/mermaid/pkg"
)

type version struct {
	Version string `json:"version"`
}

// VersionHandler responds with application version.
func VersionHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		render.Respond(w, r, version{Version: pkg.Version()})
	}
}
