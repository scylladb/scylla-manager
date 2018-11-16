// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid"
)

func init() {
	render.Respond = httpErrorRender
}

// Services contains REST API services.
type Services struct {
	Cluster     ClusterService
	HealthCheck HealthCheckService
	Repair      RepairService
	Scheduler   SchedService
}

// New returns an http.Handler implementing mermaid v1 REST API.
func New(svc *Services, logger log.Logger) http.Handler {
	r := chi.NewRouter()

	r.Use(
		heartbeatMiddleware("/ping"),
		traceIDMiddleware,
		recoverPanicsMiddleware,
		middleware.RequestLogger(httpLogger{logger}),
		render.SetContentType(render.ContentTypeJSON),
	)

	r.Get("/api/v1/version", newVersionHandler())
	r.Mount("/api/v1/", newClusterHandler(svc.Cluster))
	r.With(clusterFilter{svc: svc.Cluster}.clusterCtx).
		Mount("/api/v1/cluster/{cluster_id}/status", newStatusHandler(svc.Cluster, svc.HealthCheck))
	r.With(clusterFilter{svc: svc.Cluster}.clusterCtx).
		Mount("/api/v1/cluster/{cluster_id}/", newTaskHandler(svc.Scheduler, svc.Repair))

	// NotFound registered last due to https://github.com/go-chi/chi/issues/297
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		respondError(w, r, mermaid.ErrNotFound, "")
	})

	return r
}

// NewPrometheus returns an http.Handler exposing Prometheus metrics on '/metrics'.
func NewPrometheus() http.Handler {
	r := chi.NewRouter()
	r.Get("/metrics", promhttp.Handler().ServeHTTP)
	return r
}
