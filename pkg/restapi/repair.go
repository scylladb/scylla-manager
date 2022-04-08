// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
)

type repairHandler struct {
	svc RepairService
}

func newRepairHandler(services Services) *chi.Mux {
	m := chi.NewMux()
	h := repairHandler{
		svc: services.Repair,
	}

	m.Put("/intensity", h.updateIntensity)
	m.Put("/parallel", h.updateParallel)

	return m
}

func (h repairHandler) intensity(r *http.Request) (float64, error) {
	var (
		intensity float64
		err       error
	)

	// Read intensity from the request
	if v := r.FormValue("intensity"); v != "" {
		if intensity, err = strconv.ParseFloat(v, 64); err != nil {
			return 0, service.ErrValidate(err)
		}
	}

	return intensity, err
}

func (h repairHandler) parallel(r *http.Request) (int64, error) {
	var (
		parallel int64
		err      error
	)

	// Read parallel value from the request
	if v := r.FormValue("parallel"); v != "" {
		if parallel, err = strconv.ParseInt(v, 10, 64); err != nil {
			return parallel, service.ErrValidate(err)
		}
	}

	return parallel, err
}

func (h repairHandler) updateIntensity(w http.ResponseWriter, r *http.Request) {
	intensity, err := h.intensity(r)
	if err != nil {
		respondError(w, r, err)
		return
	}
	if err := h.svc.SetIntensity(
		r.Context(),
		mustClusterIDFromCtx(r),
		intensity,
	); err != nil {
		respondError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h repairHandler) updateParallel(w http.ResponseWriter, r *http.Request) {
	parallel, err := h.parallel(r)
	if err != nil {
		respondError(w, r, err)
		return
	}

	if err := h.svc.SetParallel(
		r.Context(),
		mustClusterIDFromCtx(r),
		int(parallel),
	); err != nil {
		respondError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}
