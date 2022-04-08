// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

type suspendHandler struct {
	svc SchedService
}

func newSuspendHandler(services Services) *chi.Mux {
	m := chi.NewMux()
	h := suspendHandler{
		svc: services.Scheduler,
	}

	m.Get("/", h.get)
	m.Put("/", h.update)

	return m
}

func (h suspendHandler) get(w http.ResponseWriter, r *http.Request) {
	v := h.svc.IsSuspended(r.Context(), mustClusterIDFromCtx(r))
	render.Respond(w, r, models.Suspended(v))
}

func (h suspendHandler) update(w http.ResponseWriter, r *http.Request) {
	var suspend models.Suspended

	if err := render.DecodeJSON(r.Body, &suspend); err != nil {
		respondError(w, r, errors.Wrap(err, "parse body"))
		return
	}

	if suspend {
		h.suspend(w, r)
	} else {
		h.resume(w, r)
	}
}

func (h suspendHandler) suspend(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Suspend(r.Context(), mustClusterIDFromCtx(r)); err != nil {
		respondError(w, r, errors.Wrap(err, "suspend"))
	}
}

func (h suspendHandler) resume(w http.ResponseWriter, r *http.Request) {
	var (
		startTasks bool
		err        error
	)
	if v := r.URL.Query().Get("start_tasks"); v != "" {
		startTasks, err = strconv.ParseBool(v)
		if err != nil {
			respondError(w, r, err)
			return
		}
	}

	if err := h.svc.Resume(r.Context(), mustClusterIDFromCtx(r), startTasks); err != nil {
		respondError(w, r, errors.Wrap(err, "resume"))
	}
}
