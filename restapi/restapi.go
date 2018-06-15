// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid"
)

// Services contains REST API services.
type Services struct {
	Cluster   ClusterService
	Repair    RepairService
	Scheduler SchedService
}

// New returns an http.Handler implementing mermaid v1 REST API.
func New(svc *Services, logger log.Logger) http.Handler {
	r := chi.NewRouter()

	r.Use(
		heartbeatMiddleware("/ping"),
		prometheusMiddleware("/metrics"),
		traceIDMiddleware,
		recoverPanicsMiddleware,
		middleware.RequestLogger(httpLogger{logger}),
		render.SetContentType(render.ContentTypeJSON),
	)

	r.Get("/api/v1/version", newVersionHandler())

	r.Mount("/api/v1/", newClusterHandler(svc.Cluster))
	r.With(clusterFilter{svc: svc.Cluster}.clusterCtx).Mount("/api/v1/cluster/{cluster_id}/", newTaskHandler(svc.Scheduler, svc.Repair))

	// NotFound registered last due to https://github.com/go-chi/chi/issues/297
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		respondError(w, r, mermaid.ErrNotFound, "")
	})

	return r
}

func init() {
	render.Respond = httpErrorRender
}

func httpErrorRender(w http.ResponseWriter, r *http.Request, v interface{}) {
	if err, ok := v.(error); ok {
		httpErr, _ := v.(*httpError)
		if httpErr == nil {
			httpErr = &httpError{
				Err:        err,
				StatusCode: http.StatusInternalServerError,
				Message:    "unexpected error, consult logs",
				TraceID:    log.TraceID(r.Context()),
			}
		}

		if le, _ := middleware.GetLogEntry(r).(*httpLogEntry); le != nil {
			le.AddFields("Error", httpErr.Error())
		}

		render.Status(r, httpErr.StatusCode)
		render.DefaultResponder(w, r, httpErr)
		return
	}

	render.DefaultResponder(w, r, v)
}
