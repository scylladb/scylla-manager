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
		Router: chi.NewRouter().With(requireClusterID),
		svc:    svc,
	}

	h.Get("/units", h.listUnits)
	h.Post("/units", h.createUnit)
	h.Get("/unit/{unit_id}", h.loadUnit)
	h.Put("/unit/{unit_id}", h.updateUnit)
	h.Delete("/unit/{unit_id}", h.deleteUnit)

	return h
}

type repairUnitRequest struct {
	*repair.Unit

	ProtectedID        string `json:"id,omitempty"`
	ProtectedClusterID string `json:"cluster_id,omitempty"`
}

func parseUnitRequest(r *http.Request) (repairUnitRequest, error) {
	var u repairUnitRequest
	if err := render.DecodeJSON(r.Body, &u); err != nil {
		return repairUnitRequest{}, httpErrBadRequest(err)
	}
	u.ClusterID = clusterIDFromCtx(r.Context())

	return u, nil
}

func (h *repairHandler) listUnits(w http.ResponseWriter, r *http.Request) {
	ids, err := h.svc.ListUnitIDs(r.Context(), clusterIDFromCtx(r.Context()))
	if err != nil {
		if err == mermaid.ErrNotFound {
			render.Respond(w, r, []*repair.Unit{})
			return
		}
		render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to list units"))
		return
	}
	render.Respond(w, r, ids)
}

func (h *repairHandler) createUnit(w http.ResponseWriter, r *http.Request) {
	u, err := parseUnitRequest(r)
	if err != nil {
		render.Respond(w, r, err)
		return
	}

	if u.ID, err = uuid.NewRandom(); err != nil {
		render.Respond(w, r, newHTTPError(err, http.StatusInternalServerError, "failed to generate a unit ID "))
		return
	}

	if err := h.svc.PutUnit(r.Context(), u.Unit); err != nil {
		render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to create unit"))
		return
	}

	unitURL := r.URL.ResolveReference(&url.URL{Path: path.Join("unit", u.Unit.ID.String())})
	w.Header().Set("Location", unitURL.String())
	w.WriteHeader(http.StatusCreated)
}

func (h *repairHandler) loadUnit(w http.ResponseWriter, r *http.Request) {
	id, err := reqUnitID(r)
	if err != nil {
		render.Respond(w, r, httpErrBadRequest(err))
		return
	}

	u, err := h.svc.GetUnit(r.Context(), clusterIDFromCtx(r.Context()), id)
	if err != nil {
		if err == mermaid.ErrNotFound {
			render.Respond(w, r, httpErrNotFound(err))
		} else {
			render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to load unit"))
		}
		return
	}
	render.Respond(w, r, u)
}

func (h *repairHandler) updateUnit(w http.ResponseWriter, r *http.Request) {
	id, err := reqUnitID(r)
	if err != nil {
		render.Respond(w, r, httpErrBadRequest(err))
		return
	}

	u, err := parseUnitRequest(r)
	if err != nil {
		render.Respond(w, r, err)
		return
	}
	u.ID = id

	err = h.svc.PutUnit(r.Context(), u.Unit)
	if err != nil {
		render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to update unit"))
		return
	}
	render.Respond(w, r, u.Unit)
}

func (h *repairHandler) deleteUnit(w http.ResponseWriter, r *http.Request) {
	id, err := reqUnitID(r)
	if err != nil {
		render.Respond(w, r, httpErrBadRequest(err))
		return
	}

	if err := h.svc.DeleteUnit(r.Context(), clusterIDFromCtx(r.Context()), id); err != nil {
		if err == mermaid.ErrNotFound {
			render.Respond(w, r, httpErrNotFound(err))
		} else {
			render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to delete unit"))
		}
		return
	}
}
