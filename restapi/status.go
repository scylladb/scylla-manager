// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
)

type statusHandler struct {
	clusterFilter
	service HealthCheckService
}

func (h *statusHandler) getStatus(w http.ResponseWriter, r *http.Request) {
	c := mustClusterFromCtx(r)

	status, err := h.service.GetStatus(r.Context(), c.ID)
	if err != nil {
		respondError(w, r, err, fmt.Sprintf("failed to check cluster %q status", c.ID))
		return
	}
	render.Respond(w, r, status)

}

func newStatusHandler(svc ClusterService, healthCheckService HealthCheckService) *chi.Mux {
	m := chi.NewMux()
	h := &statusHandler{
		clusterFilter: clusterFilter{
			svc: svc,
		},
		service: healthCheckService,
	}
	m.Route("/", func(r chi.Router) {
		r.Use(h.clusterCtx)
		r.Get("/", h.getStatus)
	})
	return m
}
