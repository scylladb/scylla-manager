// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"net/http"
	"net/url"
	"path"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/uuid"
)

//go:generate mockgen -source sched.go -destination ../mermaidmock/schedservice_mock.go -package mermaidmock

// SchedService is the scheduler service interface required by the scheduler REST API handlers.
type SchedService interface {
	GetTask(ctx context.Context, clusterID uuid.UUID, tp sched.TaskType, idOrName string) (*sched.Task, error)
	PutTask(ctx context.Context, t *sched.Task) error
	DeleteTask(ctx context.Context, t *sched.Task) error
	ListTasks(ctx context.Context, clusterID uuid.UUID, tp sched.TaskType) ([]*sched.Task, error)
	StartTask(ctx context.Context, t *sched.Task) error
	StopTask(ctx context.Context, t *sched.Task) error
	GetLastRunN(ctx context.Context, t *sched.Task, n int) ([]*sched.Run, error)
}

type schedHandler struct {
	chi.Router
	svc SchedService
}

func newSchedHandler(svc SchedService) http.Handler {
	h := &schedHandler{
		Router: chi.NewRouter(),
		svc:    svc,
	}

	h.Route("/tasks", func(r chi.Router) {
		r.Get("/", h.listTasks)
		r.Post("/", h.createTask)
	})

	h.Route("/task/{task_type}/{task_id}", func(r chi.Router) {
		r.Use(h.taskCtx)
		r.Get("/", h.loadTask)
		r.Put("/", h.updateTask)
		r.Delete("/", h.deleteTask)
		r.Put("/start", h.startTask)
		r.Put("/stop", h.stopTask)
	})

	return h
}

func (h *schedHandler) taskCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rctx := chi.RouteContext(r.Context())
		var taskType sched.TaskType
		if t := rctx.URLParam("task_type"); t == "" {
			render.Respond(w, r, httpErrBadRequest(r, errors.New("missing task type")))
			return
		} else if err := taskType.UnmarshalText([]byte(t)); err != nil {
			render.Respond(w, r, httpErrBadRequest(r, err))
			return
		}
		taskID := rctx.URLParam("task_id")
		if taskID == "" {
			render.Respond(w, r, httpErrBadRequest(r, errors.New("missing task ID")))
			return
		}

		t, err := h.svc.GetTask(r.Context(), mustClusterIDFromCtx(r), taskType, taskID)
		if err != nil {
			notFoundOrInternal(w, r, err, "failed to load task")
			return
		}

		ctx := context.WithValue(r.Context(), ctxTask, t)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *schedHandler) listTasks(w http.ResponseWriter, r *http.Request) {
	var taskType sched.TaskType
	if t := r.FormValue("type"); t != "" {
		if err := taskType.UnmarshalText([]byte(t)); err != nil {
			render.Respond(w, r, httpErrBadRequest(r, err))
			return
		}
	}

	tasks, err := h.svc.ListTasks(r.Context(), mustClusterIDFromCtx(r), taskType)
	if err != nil {
		render.Respond(w, r, httpErrInternal(r, err, "failed to list tasks"))
		return
	}

	if len(tasks) == 0 {
		render.Respond(w, r, []string{})
		return
	}
	render.Respond(w, r, tasks)
}

func (h *schedHandler) parseTask(r *http.Request) (*sched.Task, error) {
	var t sched.Task
	if err := render.DecodeJSON(r.Body, &t); err != nil {
		return nil, err
	}
	t.ClusterID = mustClusterIDFromCtx(r)
	return &t, nil
}

func (h *schedHandler) createTask(w http.ResponseWriter, r *http.Request) {
	newTask, err := h.parseTask(r)
	if err != nil {
		render.Respond(w, r, httpErrBadRequest(r, err))
		return
	}
	if newTask.ID != uuid.Nil {
		render.Respond(w, r, httpErrBadRequest(r, errors.Errorf("unexpected ID %q", newTask.ID)))
		return
	}

	if err := h.svc.PutTask(r.Context(), newTask); err != nil {
		render.Respond(w, r, httpErrInternal(r, err, "failed to create task"))
		return
	}

	taskURL := r.URL.ResolveReference(&url.URL{Path: path.Join("task", newTask.Type.String(), newTask.ID.String())})
	w.Header().Set("Location", taskURL.String())
	w.WriteHeader(http.StatusCreated)
}

func (h *schedHandler) loadTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)
	render.Respond(w, r, t)
}

func (h *schedHandler) updateTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)
	newTask, err := h.parseTask(r)
	if err != nil {
		render.Respond(w, r, httpErrBadRequest(r, err))
		return
	}
	newTask.ID = t.ID
	newTask.Type = t.Type

	if err := h.svc.PutTask(r.Context(), newTask); err != nil {
		render.Respond(w, r, httpErrInternal(r, err, "failed to update task"))
		return
	}
	render.Respond(w, r, newTask)
}

func (h *schedHandler) deleteTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)
	if err := h.svc.DeleteTask(r.Context(), t); err != nil {
		render.Respond(w, r, httpErrInternal(r, err, "failed to delete task"))
		return
	}
}

func (h *schedHandler) startTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)
	if err := h.svc.StartTask(r.Context(), t); err != nil {
		render.Respond(w, r, httpErrInternal(r, err, "failed to start task"))
		return
	}
}

func (h *schedHandler) stopTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)
	if err := h.svc.StopTask(r.Context(), t); err != nil {
		render.Respond(w, r, httpErrInternal(r, err, "failed to stop task"))
		return
	}
}
