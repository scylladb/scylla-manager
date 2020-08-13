// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/service"
)

type repairHandler struct {
	svc RepairService
}

func newRepairHandler(services Services) *chi.Mux {
	m := chi.NewMux()
	h := repairHandler{
		svc: services.Repair,
	}

	m.Post("/intensity", h.updateIntensity)

	return m
}

const maxIntensity = -1

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

	// Report error if intensity is missing
	if intensity < maxIntensity {
		return 0, service.ErrValidate(errors.New("missing intensity"))
	}

	return intensity, err
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
