// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/scylladb/mermaid/log"
)

// New returns an http.Handler implementing mermaid v1 REST API.
func New(repairSvc RepairService, logger log.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RequestLogger(httpLogger{logger}))
	r.Use(render.SetContentType(render.ContentTypeJSON))

	r.Mount("/api/v1/cluster/{cluster_id}/repair/", newRepairHandler(repairSvc))
	return r
}
