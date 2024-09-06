// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httplog"
)

func init() {
	render.Respond = responder
}

// New returns an http.Handler implementing scylla-manager v1 REST API.
func New(services Services, logger log.Logger) http.Handler {
	r := chi.NewRouter()

	r.Use(
		interactive,
		httplog.TraceID,
		httplog.RequestLogger(logger),
		render.SetContentType(render.ContentTypeJSON),
		middleware.Recoverer,
	)

	r.Get("/ping", Heartbeat())
	r.Get("/version", Version())
	r.Get("/api/v1/version", Version()) // For backwards compatibility

	r.Mount("/api/v1/", newClusterHandler(services.Cluster))
	f := clusterFilter{svc: services.Cluster}.clusterCtx
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/status", newStatusHandler(services.Cluster, services.HealthCheck))
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/suspended", newSuspendHandler(services))
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/tasks", newTasksHandler(services))
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/task", newTaskHandler(services))
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/backups", newBackupHandler(services))
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/repairs", newRepairHandler(services))

	// NotFound registered last due to https://github.com/go-chi/chi/issues/297
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		logger.Info(r.Context(), "Request path not found", "path", r.URL.Path)
		render.Respond(w, r, &httpError{
			StatusCode: http.StatusNotFound,
			Message:    fmt.Sprintf("find endpoint for path %s - make sure api-url is correct", r.URL.Path),
			TraceID:    log.TraceID(r.Context()),
		})
	})

	return r
}

func interactive(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(scyllaclient.Interactive(r.Context()))
		next.ServeHTTP(w, r)
	})
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
