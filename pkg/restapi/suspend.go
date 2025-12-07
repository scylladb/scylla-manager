// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"
	"slices"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
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
	m.Get("/details", h.getDetails)
	m.Put("/", h.update)

	return m
}

func (h suspendHandler) get(w http.ResponseWriter, r *http.Request) {
	v := h.svc.IsSuspended(r.Context(), mustClusterIDFromCtx(r))
	render.Respond(w, r, models.Suspended(v))
}

func (h suspendHandler) getDetails(w http.ResponseWriter, r *http.Request) {
	status := h.svc.SuspendStatus(r.Context(), mustClusterIDFromCtx(r))
	render.Respond(w, r, models.SuspendDetails{
		Suspended:     status.Suspended,
		AllowTaskType: status.AllowTask.String(),
	})
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
	allowTaskType := r.URL.Query().Get("allow_task_type")

	suspendPolicy := scheduler.SuspendPolicy(r.URL.Query().Get("suspend_policy"))
	if suspendPolicy == "" {
		suspendPolicy = scheduler.SuspendPolicyStopRunningTasks
	}
	policies := []scheduler.SuspendPolicy{scheduler.SuspendPolicyStopRunningTasks, scheduler.SuspendPolicyFailIfRunningTasks}
	if !slices.Contains(policies, suspendPolicy) {
		respondBadRequest(w, r, errors.Errorf("unknown 'suspend_policy'. Known suspend policies are: %v", policies))
		return
	}

	noContinue, err := parseQueryBool(r, "no_continue")
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}

	if err := h.svc.Suspend(r.Context(), mustClusterIDFromCtx(r), allowTaskType, suspendPolicy, noContinue); err != nil {
		if errors.Is(err, scheduler.ErrNotAllowedTasksRunning) {
			render.Respond(w, r, &httpError{
				StatusCode: http.StatusConflict,
				Message:    err.Error(),
				TraceID:    log.TraceID(r.Context()),
			})
			return
		}
		respondError(w, r, errors.Wrap(err, "suspend"))
	}
}

func (h suspendHandler) resume(w http.ResponseWriter, r *http.Request) {
	startTasks, err := parseQueryBool(r, "start_tasks")
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	startTasksMissedActivation, err := parseQueryBool(r, "start_tasks_missed_activation")
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	noContinue, err := parseQueryBool(r, "no_continue")
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}

	if err := h.svc.Resume(r.Context(), mustClusterIDFromCtx(r), startTasks, startTasksMissedActivation, noContinue); err != nil {
		respondError(w, r, errors.Wrap(err, "resume"))
	}
}

func parseQueryBool(r *http.Request, name string) (bool, error) {
	if v := r.URL.Query().Get(name); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return false, errors.Wrapf(err, "parse %s", name)
		}
		return parsed, nil
	}
	return false, nil
}
