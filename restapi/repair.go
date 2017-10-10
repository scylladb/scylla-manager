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
	GetRun(ctx context.Context, u *repair.Unit, taskID uuid.UUID) (*repair.Run, error)
	GetProgress(ctx context.Context, u *repair.Unit, taskID uuid.UUID, hosts ...string) ([]*repair.RunProgress, error)
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
	h.Put("/unit/{unit_id}/repair", h.startRepair)

	h.Get("/config", h.getConfig)
	h.Get("/config/{config_type}/{external_id}", h.getConfig)
	h.Put("/config", h.updateConfig)
	h.Put("/config/{config_type}/{external_id}", h.updateConfig)
	h.Delete("/config", h.deleteConfig)
	h.Delete("/config/{config_type}/{external_id}", h.deleteConfig)

	h.Get("/task/{task_id}", h.taskStats)

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
		render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to list units"))
		return
	}

	if len(ids) == 0 {
		render.Respond(w, r, []string{})
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

func (h *repairHandler) startRepair(w http.ResponseWriter, r *http.Request) {
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

	taskID := uuid.NewTime()

	if err := h.svc.Repair(r.Context(), u, taskID); err != nil {
		render.Respond(w, r, newHTTPError(err, http.StatusInternalServerError, "failed to start repair"))
		return
	}

	repairURL := r.URL.ResolveReference(&url.URL{
		Path:     path.Join("../../task", taskID.String()),
		RawQuery: fmt.Sprintf("unit_id=%s", id)},
	)
	w.Header().Set("Location", repairURL.String())
	w.WriteHeader(http.StatusCreated)
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

func (h *repairHandler) taskStats(w http.ResponseWriter, r *http.Request) {
	var taskID uuid.UUID
	if err := taskID.UnmarshalText([]byte(chi.URLParam(r, "task_id"))); err != nil {
		render.Respond(w, r, httpErrBadRequest(err))
		return
	}

	unitID, err := reqUnitIDQuery(r)
	if err != nil {
		render.Respond(w, r, err)
		return
	}
	u, err := h.svc.GetUnit(r.Context(), clusterIDFromCtx(r.Context()), unitID)
	if err != nil {
		render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to load unit"))
		return
	}

	taskRun, err := h.svc.GetRun(r.Context(), u, taskID)
	if err != nil {
		render.Respond(w, r, newHTTPError(err, http.StatusServiceUnavailable, "failed to load task"))
		return
	}

	total, details, err := h.calcRepairProgress(r.Context(), u, taskID)
	if err != nil {
		render.Respond(w, r, err)
		return
	}
	render.Respond(w, r, struct {
		Keyspace string         `json:"keyspace"`
		Tables   []string       `json:"tables"`
		Status   repair.Status  `json:"status"`
		Total    int            `json:"total"`
		Details  map[string]int `json:"details"`
	}{
		Keyspace: taskRun.Keyspace,
		Tables:   taskRun.Tables,
		Status:   taskRun.Status,
		Total:    total,
		Details:  details,
	})
}

// calcRepairProgress returns the total repair progress of taskID, plus the host and host/shard progress
// percentage.
func (h *repairHandler) calcRepairProgress(ctx context.Context, u *repair.Unit, taskID uuid.UUID) (int, map[string]int, error) {
	runs, err := h.svc.GetProgress(ctx, u, taskID)
	if err != nil {
		return 0, nil, newHTTPError(err, http.StatusServiceUnavailable, "failed to load task progress")
	}

	if len(runs) == 0 {
		return 0, nil, httpErrNotFound(err)
	}

	type shardProgress struct {
		Shard    int
		Complete float64
	}
	hostProgress := make(map[string][]shardProgress)
	for _, r := range runs {
		if r.SegmentCount == 0 {
			hostProgress[r.Host] = append(hostProgress[r.Host], shardProgress{Shard: r.Shard})
			continue
		}
		hostProgress[r.Host] = append(hostProgress[r.Host], shardProgress{
			Shard:    r.Shard,
			Complete: float64(r.SegmentSuccess+r.SegmentError) / float64(r.SegmentCount),
		})
	}

	var (
		totalSum float64
		details  = make(map[string]int)
	)
	for k := range hostProgress {
		var sum float64
		for _, d := range hostProgress[k] {
			details[fmt.Sprintf("%s/%d", k, d.Shard)] = int(100 * d.Complete)
			sum += d.Complete
		}
		hostTotal := sum / float64(len(hostProgress[k]))
		details[k] = int(100 * hostTotal)
		totalSum += hostTotal
	}
	total := int(100 * totalSum / float64(len(hostProgress)))
	return total, details, nil
}
