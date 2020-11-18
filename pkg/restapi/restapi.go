// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/httphandler"
	"github.com/scylladb/scylla-manager/pkg/util/httplog"
	"github.com/scylladb/scylla-manager/pkg/util/prom"
)

func init() {
	render.Respond = responder
}

// New returns an http.Handler implementing scylla-manager v1 REST API.
func New(services Services, swaggerUIPath string, logger log.Logger) http.Handler {
	r := chi.NewRouter()

	r.Use(
		interactive,
		httplog.TraceID,
		httplog.RequestLogger(logger),
		render.SetContentType(render.ContentTypeJSON),
	)

	// Swagger UI
	if swaggerUIPath != "" {
		r.Handle("/ui", http.RedirectHandler("/ui/", http.StatusMovedPermanently))
		r.Mount("/ui/", http.StripPrefix("/ui/", http.FileServer(http.Dir(swaggerUIPath))))
	}

	r.Get("/ping", httphandler.Heartbeat())
	r.Get("/version", httphandler.Version())
	r.Get("/api/v1/version", httphandler.Version()) // For backwards compatibility

	r.Mount("/api/v1/", newClusterHandler(services.Cluster))
	f := clusterFilter{svc: services.Cluster}.clusterCtx
	r.With(f).Mount("/api/v1/cluster/{cluster_id}/status", newStatusHandler(services.Cluster, services.HealthCheck))
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
func NewPrometheus(svc ClusterService, mw *prom.MetricsWatcher) http.Handler {
	r := chi.NewRouter()

	r.Get("/metrics", mw.WrapHandler(promhttp.Handler()))

	// Exposing Consul API to Prometheus for discovering nodes.
	// The idea is to use already working discovering mechanism to avoid
	// extending Prometheus it self.
	r.Mount("/v1", newConsulHandler(svc))

	return r
}
