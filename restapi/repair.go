// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"net/http"
	"net/url"
	"path"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/uuid"
)

//go:generate mockgen -source repair.go -destination ../mermaidmock/repairservice_mock.go -package mermaidmock

// RepairService is the repair service interface required by the repair REST API handlers.
type RepairService interface {
	GetUnit(ctx context.Context, clusterID, ID uuid.UUID) (*repair.Unit, error)
	PutUnit(ctx context.Context, u *repair.Unit) error
	DeleteUnit(ctx context.Context, clusterID, ID uuid.UUID) error
	ListUnitIDs(ctx context.Context, clusterID uuid.UUID) ([]uuid.UUID, error)
}

type repairHandler struct {
	chi.Router
	svc RepairService
}

func newRepairHandler(svc RepairService) http.Handler {
	h := &repairHandler{
		Router: chi.NewRouter(),
		svc:    svc,
	}

	h.Get("/units", h.ListUnits)
	h.Post("/units", h.CreateUnit)
	h.Get("/unit/{unit_id}", h.LoadUnit)
	h.Put("/unit/{unit_id}", h.UpdateUnit)
	h.Delete("/unit/{unit_id}", h.DeleteUnit)

	return h
}

type repairUnitRequest struct {
	*repair.Unit

	ProtectedID        string `json:"id,omitempty"`
	ProtectedClusterID string `json:"cluster_id,omitempty"`
}

func (h *repairHandler) ListUnits(w http.ResponseWriter, r *http.Request) {
	clusterID, err := reqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ids, err := h.svc.ListUnitIDs(r.Context(), clusterID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render.Respond(w, r, ids)
}

func (h *repairHandler) CreateUnit(w http.ResponseWriter, r *http.Request) {
	var err error
	clusterID, err := reqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	u := new(repairUnitRequest)
	if err := render.DecodeJSON(r.Body, u); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if u.ID, err = uuid.NewRandom(); err != nil {
		http.Error(w, "failed to generate a unit ID "+err.Error(), http.StatusInternalServerError)
		return
	}
	u.ClusterID = clusterID

	err = h.svc.PutUnit(r.Context(), u.Unit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	unitURL := r.URL.ResolveReference(&url.URL{Path: path.Join("unit", u.Unit.ID.String())})
	w.Header().Set("Location", unitURL.String())
	w.WriteHeader(http.StatusCreated)
}

func (h *repairHandler) LoadUnit(w http.ResponseWriter, r *http.Request) {
	clusterID, err := reqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := reqUnitID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	u, err := h.svc.GetUnit(r.Context(), clusterID, id)
	if err != nil {
		if err == mermaid.ErrNotFound {
			http.Error(w, id.String(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	render.Respond(w, r, u)
}

func (h *repairHandler) UpdateUnit(w http.ResponseWriter, r *http.Request) {
	clusterID, err := reqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := reqUnitID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	u := new(repairUnitRequest)
	if err := render.DecodeJSON(r.Body, u); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	u.ID = id
	u.ClusterID = clusterID
	err = h.svc.PutUnit(r.Context(), u.Unit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render.Respond(w, r, u.Unit)
}

func (h *repairHandler) DeleteUnit(w http.ResponseWriter, r *http.Request) {
	clusterID, err := reqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := reqUnitID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.svc.DeleteUnit(r.Context(), clusterID, id)
	if err != nil {
		if err == mermaid.ErrNotFound {
			http.Error(w, id.String(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}
