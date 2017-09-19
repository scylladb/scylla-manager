// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
	"github.com/scylladb/mermaid/log"
)

// New returns an http.Handler implementing mermaid v1 REST API.
func New(repairSvc RepairService, logger log.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(traceIDMiddleware)
	r.Use(middleware.RequestLogger(httpLogger{logger}))
	r.Use(recoverPanics)
	r.Use(render.SetContentType(render.ContentTypeJSON))

	r.Mount("/api/v1/cluster/{cluster_id}/repair/", newRepairHandler(repairSvc))

	render.Respond = httpErrorRender
	return r
}

func httpErrorRender(w http.ResponseWriter, r *http.Request, v interface{}) {
	if err, ok := v.(error); ok {
		httpErr, _ := v.(*httpError)
		if httpErr == nil {
			httpErr = newHTTPError(err,
				http.StatusInternalServerError,
				fmt.Sprintf("traceID=%s an unexpected error has occurred, consult logs.", log.TraceID(r.Context())),
			)
		}

		if le, _ := middleware.GetLogEntry(r).(*httpLogEntry); le != nil {
			le.AddFields("Error", httpErr.Err.Error())
		}

		render.Status(r, httpErr.StatusCode)
		render.DefaultResponder(w, r, httpErr)
		return
	}

	render.DefaultResponder(w, r, v)
}
