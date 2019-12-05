// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/util/httpmw"
)

func init() {
	render.Respond = responder
}

// New returns an http.Handler implementing mermaid v1 REST API.
func New(services Services, logger log.Logger) http.Handler {
	r := chi.NewRouter()

	r.Use(
		httpmw.RequestTraceID,
		httpmw.RequestLogger(logger),
		render.SetContentType(render.ContentTypeJSON),
	)

	r.Get("/ping", httpmw.HeartbeatHandler())
	r.Get("/version", httpmw.VersionHandler())
	r.Get("/api/v1/version", httpmw.VersionHandler()) // For backwards compatibility

	r.Mount("/api/v1/", newClusterHandler(services.Cluster))
	f := clusterFilter{svc: services.Cluster}.clusterCtx
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/status", newStatusHandler(services.Cluster, services.HealthCheck))
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/tasks", newTasksHandler(services))
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/task", newTaskHandler(services))
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/backups", newBackupHandler(services))

	// NotFound registered last due to https://github.com/go-chi/chi/issues/297
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		respondError(w, r, service.ErrNotFound)
	})

	return r
}

// NewPrometheus returns an http.Handler exposing Prometheus metrics on
// '/metrics'.
func NewPrometheus(svc ClusterService) http.Handler {
	r := chi.NewRouter()

	r.Get("/metrics", promhttp.Handler().ServeHTTP)

	// Exposing Consul API to Prometheus for discovering nodes.
	// The idea is to use already working discovering mechanism to avoid
	// extending Prometheus it self.
	r.Mount("/v1", newConsulHandler(svc))

	return r
}
