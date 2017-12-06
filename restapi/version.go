// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/render"
	"github.com/scylladb/mermaid"
)

// Version of build.
type Version struct {
	Version string `json:"version"`
}

func newVersionHandler() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := &Version{Version: mermaid.Version()}
		render.Respond(w, r, v)
	})
}
