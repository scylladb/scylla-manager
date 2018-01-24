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
	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/uuid"
)

//go:generate mockgen -source repair.go -destination ../mermaidmock/repairservice_mock.go -package mermaidmock

// RepairService is the repair service interface required by the repair REST API handlers.
type RepairService interface {
	GetUnit(ctx context.Context, clusterID uuid.UUID, idOrName string) (*repair.Unit, error)
	PutUnit(ctx context.Context, u *repair.Unit) error
	DeleteUnit(ctx context.Context, clusterID, ID uuid.UUID) error
	ListUnits(ctx context.Context, clusterID uuid.UUID, f *repair.UnitFilter) ([]*repair.Unit, error)

	GetConfig(ctx context.Context, src repair.ConfigSource) (*repair.Config, error)
	PutConfig(ctx context.Context, src repair.ConfigSource, c *repair.Config) error
	DeleteConfig(ctx context.Context, src repair.ConfigSource) error

	GetRun(ctx context.Context, u *repair.Unit, runID uuid.UUID) (*repair.Run, error)
	GetLastRun(ctx context.Context, u *repair.Unit) (*repair.Run, error)
	GetProgress(ctx context.Context, u *repair.Unit, runID uuid.UUID, hosts ...string) ([]*repair.RunProgress, error)
}

type repairHandler struct {
	chi.Router
	svc      RepairService
	schedSvc SchedService
}

func newRepairHandler(svc RepairService, schedSvc SchedService) http.Handler {
	h := &repairHandler{
		Router:   chi.NewRouter(),
		svc:      svc,
		schedSvc: schedSvc,
	}

	// unit
	h.Route("/units", func(r chi.Router) {
		r.Get("/", h.listUnits)
		r.Post("/", h.createUnit)
	})
	h.Route("/unit/{unit_id}", func(r chi.Router) {
		r.Use(h.unitCtx)
		r.Get("/", h.loadUnit)
		r.Put("/", h.updateUnit)
		r.Delete("/", h.deleteUnit)
		r.Get("/progress", h.repairProgress)
	})

	// config
	h.Route("/config", func(r chi.Router) {
		r.Get("/", h.getConfig)
		r.Put("/", h.updateConfig)
		r.Delete("/", h.deleteConfig)

		r.Route("/{config_type}/{external_id}", func(r chi.Router) {
			r.Get("/", h.getConfig)
			r.Put("/", h.updateConfig)
			r.Delete("/", h.deleteConfig)
		})
	})

	return h
}

//
// unit
//

func (h *repairHandler) unitCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unitID := chi.URLParam(r, "unit_id")
		if unitID == "" {
			respondBadRequest(w, r, errors.New("missing unit ID"))
			return
		}

		u, err := h.svc.GetUnit(r.Context(), mustClusterIDFromCtx(r), unitID)
		if err != nil {
			respondError(w, r, err, "failed to load unit")
			return
		}

		ctx := context.WithValue(r.Context(), ctxRepairUnit, u)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *repairHandler) parseUnit(r *http.Request) (*repair.Unit, error) {
	var u repair.Unit
	if err := render.DecodeJSON(r.Body, &u); err != nil {
		return nil, err
	}
	u.ClusterID = mustClusterIDFromCtx(r)
	return &u, nil
}

func (h *repairHandler) listUnits(w http.ResponseWriter, r *http.Request) {
	units, err := h.svc.ListUnits(r.Context(), mustClusterIDFromCtx(r), &repair.UnitFilter{})
	if err != nil {
		respondError(w, r, err, "failed to list units")
		return
	}

	if len(units) == 0 {
		render.Respond(w, r, []struct{}{})
		return
	}
	render.Respond(w, r, units)
}

func (h *repairHandler) createUnit(w http.ResponseWriter, r *http.Request) {
	newUnit, err := h.parseUnit(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	if newUnit.ID != uuid.Nil {
		respondBadRequest(w, r, errors.Errorf("unexpected ID %q", newUnit.ID))
		return
	}

	if err := h.svc.PutUnit(r.Context(), newUnit); err != nil {
		respondError(w, r, err, "failed to create unit")
		return
	}

	location := r.URL.ResolveReference(&url.URL{
		Path: path.Join("unit", newUnit.ID.String()),
	})
	w.Header().Set("Location", location.String())
	w.WriteHeader(http.StatusCreated)
}

func (h *repairHandler) loadUnit(w http.ResponseWriter, r *http.Request) {
	u := mustUnitFromCtx(r)
	render.Respond(w, r, u)
}

func (h *repairHandler) updateUnit(w http.ResponseWriter, r *http.Request) {
	u := mustUnitFromCtx(r)

	newUnit, err := h.parseUnit(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	newUnit.ID = u.ID

	if err := h.svc.PutUnit(r.Context(), newUnit); err != nil {
		respondError(w, r, err, "failed to update unit")
		return
	}
	render.Respond(w, r, newUnit)
}

func (h *repairHandler) deleteUnit(w http.ResponseWriter, r *http.Request) {
	u := mustUnitFromCtx(r)

	if err := h.svc.DeleteUnit(r.Context(), mustClusterIDFromCtx(r), u.ID); err != nil {
		respondError(w, r, err, "failed to delete unit")
		return
	}
}

type repairProgress struct {
	PercentComplete int `json:"percent_complete"`
	Total           int `json:"total"`
	Success         int `json:"success"`
	Error           int `json:"error"`
}

type repairHostProgress struct {
	repairProgress
	Shards map[int]*repairProgress `json:"shards"`
}

type repairProgressResponse struct {
	Keyspace string        `json:"keyspace"`
	Tables   []string      `json:"tables"`
	Status   repair.Status `json:"status"`
	repairProgress

	Hosts map[string]*repairHostProgress `json:"hosts"`
}

func (h *repairHandler) repairProgress(w http.ResponseWriter, r *http.Request) {
	u := mustUnitFromCtx(r)

	var (
		run *repair.Run
		err error
	)

	if taskID := r.FormValue("task_id"); taskID == "" {
		run, err = h.svc.GetLastRun(r.Context(), u)
	} else {
		if h.schedSvc == nil {
			respondError(w, r, errors.New("missing scheduler service"), "cannot lookup task")
			return
		}
		var t *sched.Task
		t, err = h.schedSvc.GetTask(r.Context(), u.ClusterID, sched.RepairTask, taskID)
		if err != nil {
			respondError(w, r, err, "failed to load task")
			return
		}
		var runs []*sched.Run
		runs, err = h.schedSvc.GetLastRun(r.Context(), t, 1)
		if err != nil {
			respondError(w, r, err, "failed to load task run")
			return
		}
		if len(runs) == 0 {
			respondError(w, r, mermaid.ParamError{Cause: errors.New("task did not run yet")}, "")
			return
		}
		run, err = h.svc.GetRun(r.Context(), u, runs[0].ID)
	}

	if err != nil {
		respondError(w, r, err, "failed to load repair run")
		return
	}

	resp, err := h.createProgressResponse(r, u, run)
	if err != nil {
		respondError(w, r, err, "failed to load run repairProgress")
		return
	}

	render.Respond(w, r, resp)
}

func (h *repairHandler) createProgressResponse(r *http.Request, u *repair.Unit, t *repair.Run) (*repairProgressResponse, error) {
	runs, err := h.svc.GetProgress(r.Context(), u, t.ID)
	if err != nil {
		return nil, err
	}

	resp := &repairProgressResponse{
		Keyspace: t.Keyspace,
		Tables:   t.Tables,
		Status:   t.Status,
	}
	if len(runs) == 0 {
		return resp, nil
	}

	resp.Hosts = make(map[string]*repairHostProgress)
	for _, r := range runs {
		if _, exists := resp.Hosts[r.Host]; !exists {
			resp.Hosts[r.Host] = &repairHostProgress{
				Shards: make(map[int]*repairProgress),
			}
		}

		if r.SegmentCount == 0 {
			resp.Hosts[r.Host].Shards[r.Shard] = &repairProgress{}
			continue
		}
		resp.Hosts[r.Host].Shards[r.Shard] = &repairProgress{
			PercentComplete: r.PercentComplete(),
			Total:           r.SegmentCount,
			Success:         r.SegmentSuccess,
			Error:           r.SegmentError,
		}
	}

	var totalSum float64
	for _, hostProgress := range resp.Hosts {
		var sum float64
		for _, s := range hostProgress.Shards {
			sum += float64(s.PercentComplete)
			hostProgress.Total += s.Total
			hostProgress.Success += s.Success
			hostProgress.Error += s.Error
		}
		sum /= float64(len(hostProgress.Shards))
		totalSum += sum
		hostProgress.PercentComplete = int(sum)
		resp.Total += hostProgress.Total
		resp.Success += hostProgress.Success
		resp.Error += hostProgress.Error
	}
	resp.PercentComplete = int(totalSum / float64(len(resp.Hosts)))
	return resp, nil
}

//
// config
//

type repairConfigRequest struct {
	*repair.Config
	repair.ConfigSource `json:"-"`
}

func parseConfigRequest(r *http.Request) (*repairConfigRequest, error) {
	var cr repairConfigRequest
	if err := render.DecodeJSON(r.Body, &cr.Config); err != nil {
		if err != io.EOF {
			return nil, err
		}
	}

	routeCtx := chi.RouteContext(r.Context())
	if typ := routeCtx.URLParam("config_type"); typ != "" {
		err := cr.Type.UnmarshalText([]byte(typ))
		if err != nil {
			return nil, errors.Wrap(err, "bad config type")
		}
	} else {
		cr.Type = repair.ClusterConfig
	}
	switch cr.Type {
	case repair.UnitConfig, repair.KeyspaceConfig, repair.ClusterConfig:
	default:
		return nil, fmt.Errorf("config type %q not allowed", cr.Type)
	}

	if id := routeCtx.URLParam("external_id"); id != "" {
		cr.ExternalID = id
	}
	cr.ClusterID = mustClusterIDFromCtx(r)

	return &cr, nil
}

func (h *repairHandler) getConfig(w http.ResponseWriter, r *http.Request) {
	cr, err := parseConfigRequest(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}

	c, err := h.svc.GetConfig(r.Context(), cr.ConfigSource)
	if err != nil {
		respondError(w, r, err, "failed to load config")
		return
	}
	render.Respond(w, r, c)
}

func (h *repairHandler) updateConfig(w http.ResponseWriter, r *http.Request) {
	cr, err := parseConfigRequest(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}

	if err := h.svc.PutConfig(r.Context(), cr.ConfigSource, cr.Config); err != nil {
		respondError(w, r, err, "failed to update config")
		return
	}
}

func (h *repairHandler) deleteConfig(w http.ResponseWriter, r *http.Request) {
	cr, err := parseConfigRequest(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}

	if err := h.svc.DeleteConfig(r.Context(), cr.ConfigSource); err != nil {
		respondError(w, r, err, "failed to delete config")
	}
}
