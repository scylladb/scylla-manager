// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
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
	ListUnits(ctx context.Context, clusterID uuid.UUID) ([]*repair.Unit, error)

	GetConfig(ctx context.Context, src repair.ConfigSource) (*repair.Config, error)
	PutConfig(ctx context.Context, src repair.ConfigSource, c *repair.Config) error
	DeleteConfig(ctx context.Context, src repair.ConfigSource) error

	Repair(ctx context.Context, u *repair.Unit, taskID uuid.UUID) error
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
	h.Put("/unit/{unit_id}/repair", h.triggerRepair)

	h.Get("/config", h.getConfig)
	h.Get("/config/{config_type}/{external_id}", h.getConfig)
	h.Put("/config", h.updateConfig)
	h.Put("/config/{config_type}/{external_id}", h.updateConfig)
	h.Delete("/config", h.deleteConfig)
	h.Delete("/config/{config_type}/{external_id}", h.deleteConfig)

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
	ids, err := h.svc.ListUnits(r.Context(), clusterIDFromCtx(r.Context()))
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
		render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to delete unit"))
		return
	}
}

func (h *repairHandler) triggerRepair(w http.ResponseWriter, r *http.Request) {
	id, err := reqUnitID(r)
	if err != nil {
		render.Respond(w, r, httpErrBadRequest(err))
		return
	}

	var args struct {
		TaskID uuid.UUID `json:"task_id"`
	}
	if err := render.DecodeJSON(r.Body, &args); err != nil {
		if err != io.EOF {
			render.Respond(w, r, httpErrBadRequest(err))
			return
		}
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

	if err := h.svc.Repair(r.Context(), u, args.TaskID); err != nil {
		render.Respond(w, r, newHTTPError(err, http.StatusInternalServerError, "failed to start repair on unit: "+u.ID.String()))
		return
	}
}

type repairConfigRequest struct {
	*repair.Config
	repair.ConfigSource `json:"-"`
}

func parseConfigRequest(r *http.Request) (*repairConfigRequest, error) {
	var cr repairConfigRequest
	if err := render.DecodeJSON(r.Body, &cr.Config); err != nil {
		if err != io.EOF {
			return nil, httpErrBadRequest(err)
		}
	}

	routeCtx := chi.RouteContext(r.Context())
	if typ := routeCtx.URLParam("config_type"); typ != "" {
		err := cr.Type.UnmarshalText([]byte(typ))
		if err != nil {
			return nil, httpErrBadRequest(errors.Wrap(err, "bad config type"))
		}
	} else {
		cr.Type = repair.ClusterConfig
	}
	switch cr.Type {
	case repair.UnitConfig, repair.KeyspaceConfig, repair.ClusterConfig:
	default:
		return nil, httpErrBadRequest(fmt.Errorf("config type %q not allowed", cr.Type))
	}

	if id := routeCtx.URLParam("external_id"); id != "" {
		cr.ExternalID = id
	}
	cr.ClusterID = clusterIDFromCtx(r.Context())
	return &cr, nil
}

func (h *repairHandler) getConfig(w http.ResponseWriter, r *http.Request) {
	cr, err := parseConfigRequest(r)
	if err != nil {
		render.Respond(w, r, err)
		return
	}

	c, err := h.svc.GetConfig(r.Context(), cr.ConfigSource)
	if err != nil {
		if err == mermaid.ErrNotFound {
			render.Respond(w, r, httpErrNotFound(err))
		} else {
			render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to load config"))
		}
		return
	}
	render.Respond(w, r, c)
}

func (h *repairHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	cr, err := parseConfigRequest(r)
	if err != nil {
		render.Respond(w, r, err)
		return
	}

	if err := h.svc.PutConfig(r.Context(), cr.ConfigSource, cr.Config); err != nil {
		render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to update config"))
		return
	}
}

func (h *repairHandler) deleteConfig(w http.ResponseWriter, r *http.Request) {
	cr, err := parseConfigRequest(r)
	if err != nil {
		render.Respond(w, r, err)
		return
	}

	if err := h.svc.DeleteConfig(r.Context(), cr.ConfigSource); err != nil {
		render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to delete config"))
	}
}
