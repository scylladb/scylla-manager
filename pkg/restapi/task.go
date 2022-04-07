// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/service/backup"
	"github.com/scylladb/scylla-manager/pkg/service/repair"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

type taskHandler struct {
	Services
}

func newTasksHandler(services Services) *chi.Mux {
	m := chi.NewMux()
	h := &taskHandler{services}

	m.Get("/", h.listTasks)
	m.Post("/", h.createTask)
	m.Get("/{task_type}/target", h.getTarget)

	return m
}

func newTaskHandler(services Services) *chi.Mux {
	m := chi.NewMux()
	h := &taskHandler{services}

	m.Route("/{task_type}/{task_id}", func(r chi.Router) {
		r.Use(h.taskCtx)
		r.Get("/", h.loadTask)
		r.Put("/", h.updateTask)
		r.Delete("/", h.deleteTask)
		r.Put("/start", h.startTask)
		r.Put("/stop", h.stopTask)
		r.Get("/history", h.taskHistory)
		r.Get("/{run_id}", h.taskRunProgress)
	})

	return m
}

func (h *taskHandler) taskCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rctx := chi.RouteContext(r.Context())
		var taskType scheduler.TaskType
		if t := rctx.URLParam("task_type"); t == "" {
			respondBadRequest(w, r, errors.New("missing task type"))
			return
		} else if err := taskType.UnmarshalText([]byte(t)); err != nil {
			respondBadRequest(w, r, err)
			return
		}
		taskIDStr := rctx.URLParam("task_id")
		if taskIDStr == "" {
			respondBadRequest(w, r, errors.New("missing task ID"))
			return
		}
		taskID, err := uuid.Parse(taskIDStr)
		if err != nil {
			respondBadRequest(w, r, errors.New("invalid task ID"))
		}

		t, err := h.Scheduler.GetTaskByID(r.Context(), mustClusterIDFromCtx(r), taskType, taskID)
		if err != nil {
			respondError(w, r, errors.Wrapf(err, "load task %q", taskID))
			return
		}

		ctx := context.WithValue(r.Context(), ctxTask, t)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *taskHandler) listTasks(w http.ResponseWriter, r *http.Request) {
	var (
		filter scheduler.ListFilter
		err    error
	)

	// Never expose deleted tasks over API
	filter.Deleted = false

	if s := r.FormValue("all"); s != "" {
		filter.Disabled, err = strconv.ParseBool(s)
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
	}
	if s := r.FormValue("type"); s != "" {
		var taskType scheduler.TaskType
		err = taskType.UnmarshalText([]byte(s))
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
		filter.TaskType = append(filter.TaskType, taskType)
	}
	if s := r.FormValue("status"); s != "" {
		var status scheduler.Status
		err = status.UnmarshalText([]byte(s))
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
		filter.Status = append(filter.Status, status)
	}
	if s := r.FormValue("short"); s != "" {
		filter.Short, err = strconv.ParseBool(s)
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
	}

	cid := mustClusterIDFromCtx(r)
	tasks, err := h.Scheduler.ListTasks(r.Context(), cid, filter)
	if err != nil {
		respondError(w, r, errors.Wrapf(err, "list cluster %q tasks", cid))
		return
	}

	render.Respond(w, r, tasks)
}

func (h *taskHandler) parseTask(r *http.Request) (*scheduler.Task, error) {
	var t scheduler.Task
	if err := render.DecodeJSON(r.Body, &t); err != nil {
		return nil, err
	}
	t.ClusterID = mustClusterIDFromCtx(r)
	return &t, nil
}

type backupTarget struct {
	backup.Target
	Size int64 // Target size in bytes.
}

func (h *taskHandler) getTarget(w http.ResponseWriter, r *http.Request) {
	newTask, err := h.parseTask(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	if newTask.ID != uuid.Nil {
		respondBadRequest(w, r, errors.Errorf("unexpected ID %q", newTask.ID))
		return
	}

	d := h.Services.Scheduler.PropertiesDecorator(newTask.Type)
	p := newTask.Properties
	if d != nil {
		p, err = d(r.Context(), newTask.ClusterID, newTask.ID, newTask.Properties)
		if err != nil {
			respondBadRequest(w, r, errors.Wrap(err, "evaluate properties"))
		}
	}

	var t interface{}

	switch newTask.Type {
	case scheduler.BackupTask:
		bt, err := h.Backup.GetTarget(r.Context(), newTask.ClusterID, p)
		if err != nil {
			respondError(w, r, errors.Wrap(err, "get backup target"))
			return
		}
		size, err := h.Backup.GetTargetSize(r.Context(), newTask.ClusterID, bt)
		if err != nil {
			respondError(w, r, errors.Wrap(err, "get backup target size"))
			return
		}
		t = backupTarget{
			Target: bt,
			Size:   size,
		}
	case scheduler.RepairTask:
		if t, err = h.Repair.GetTarget(r.Context(), newTask.ClusterID, p); err != nil {
			respondError(w, r, errors.Wrap(err, "get repair target"))
			return
		}
	default:
		respondBadRequest(w, r, errors.Errorf("invalid task type %q", newTask.Type))
		return
	}

	render.Respond(w, r, t)
}

func (h *taskHandler) validateTask(ctx context.Context, newTask *scheduler.Task, p []byte) error {
	switch newTask.Type {
	case scheduler.BackupTask:
		if _, err := h.Backup.GetTarget(ctx, newTask.ClusterID, p); err != nil {
			return errors.Wrap(err, "create backup target")
		}
	case scheduler.RepairTask:
		if _, err := h.Repair.GetTarget(ctx, newTask.ClusterID, p); err != nil {
			return errors.Wrap(err, "create repair target")
		}
	case scheduler.ValidateBackupTask:
		if _, err := h.Backup.GetValidationTarget(ctx, newTask.ClusterID, p); err != nil {
			return errors.Wrap(err, "create backup validation target")
		}
	}

	return nil
}

func (h *taskHandler) createTask(w http.ResponseWriter, r *http.Request) {
	newTask, err := h.parseTask(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	if newTask.ID != uuid.Nil {
		respondBadRequest(w, r, errors.Errorf("unexpected ID %q", newTask.ID))
		return
	}

	d := h.Services.Scheduler.PropertiesDecorator(newTask.Type)
	p := newTask.Properties
	if d != nil {
		p, err = d(r.Context(), newTask.ClusterID, newTask.ID, newTask.Properties)
		if err != nil {
			respondBadRequest(w, r, errors.Wrap(err, "evaluate properties"))
		}
	}

	if err := h.validateTask(r.Context(), newTask, p); err != nil {
		respondError(w, r, err)
		return
	}

	if err := h.Scheduler.PutTask(r.Context(), newTask); err != nil {
		respondError(w, r, errors.Wrap(err, "create task"))
		return
	}

	taskURL := r.URL.ResolveReference(&url.URL{Path: path.Join("task", newTask.Type.String(), newTask.ID.String())})
	w.Header().Set("Location", taskURL.String())
	w.WriteHeader(http.StatusCreated)
}

func (h *taskHandler) loadTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)
	render.Respond(w, r, t)
}

func (h *taskHandler) updateTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)
	newTask, err := h.parseTask(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	newTask.ID = t.ID
	newTask.Type = t.Type

	if err := h.validateTask(r.Context(), newTask, newTask.Properties); err != nil {
		respondError(w, r, err)
		return
	}

	if err := h.Scheduler.PutTask(r.Context(), newTask); err != nil {
		respondError(w, r, errors.Wrapf(err, "update task %q", t.ID))
		return
	}
	render.Respond(w, r, newTask)
}

func (h *taskHandler) deleteTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)
	if err := h.Scheduler.DeleteTask(r.Context(), t); err != nil {
		respondError(w, r, errors.Wrapf(err, "delete task %q", t.ID))
		return
	}
}

func (h *taskHandler) startTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)

	noContinue, err := h.noContinue(r)
	if err != nil {
		respondBadRequest(w, r, err)
	}

	if noContinue {
		err = h.Scheduler.StartTaskNoContinue(r.Context(), t)
	} else {
		err = h.Scheduler.StartTask(r.Context(), t)
	}
	if err != nil {
		respondError(w, r, errors.Wrapf(err, "start task %q", t.ID))
		return
	}
}

func (h *taskHandler) noContinue(r *http.Request) (bool, error) {
	v := r.FormValue("continue")
	if v == "" {
		return false, nil
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, errors.Wrap(err, "parse continue param")
	}
	return !b, nil
}

func (h *taskHandler) stopTask(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)

	disable := false
	if d := r.FormValue("disable"); d != "" {
		var err error
		disable, err = strconv.ParseBool(d)
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
	}

	if t.Enabled && disable {
		t.Enabled = false
		// current task is canceled on save no need to stop it again
		if err := h.Scheduler.PutTask(r.Context(), t); err != nil {
			respondError(w, r, errors.Wrapf(err, "update task %q", t.ID))
			return
		}
	} else if err := h.Scheduler.StopTask(r.Context(), t); err != nil {
		respondError(w, r, errors.Wrapf(err, "stop task %q", t.ID))
		return
	}
}

func (h *taskHandler) taskHistory(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)

	limit := 10
	if l := r.FormValue("limit"); l != "" {
		var err error
		limit, err = strconv.Atoi(l)
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
	}

	runs, err := h.Scheduler.GetLastRuns(r.Context(), t, limit)
	if err != nil {
		respondError(w, r, errors.Wrapf(err, "load task %q history", t.ID))
		return
	}
	if len(runs) == 0 {
		render.Respond(w, r, []string{})
		return
	}
	render.Respond(w, r, runs)
}

type taskRunProgress struct {
	Run      *scheduler.Run `json:"run"`
	Progress interface{}    `json:"progress"`
}

func (h *taskHandler) taskRunProgress(w http.ResponseWriter, r *http.Request) {
	t := mustTaskFromCtx(r)
	p := chi.URLParam(r, "run_id")

	var prog taskRunProgress

	if n, err := tryReadOffset(p); err != nil { // nolint
		respondBadRequest(w, r, errors.Wrap(err, "parse run offset"))
		return
	} else if n >= 0 {
		prog.Run, err = h.Scheduler.GetNthLastRun(r.Context(), t, n)
		if n == 0 && errors.Is(err, service.ErrNotFound) {
			prog.Run = &scheduler.Run{
				ClusterID: t.ClusterID,
				Type:      t.Type,
				TaskID:    t.ID,
				Status:    scheduler.StatusNew,
			}
			switch t.Type {
			case scheduler.RepairTask:
				prog.Progress = repair.Progress{}
			case scheduler.BackupTask:
				prog.Progress = backup.Progress{}
			}
			render.Respond(w, r, prog)
			return
		}
		if err != nil {
			respondError(w, r, errors.Wrapf(err, "run ~%d", n))
			return
		}
	} else {
		runID, err := uuid.Parse(p)
		if err != nil {
			respondBadRequest(w, r, errors.Wrapf(err, "parse uuid %s", p))
			return
		}
		prog.Run, err = h.Scheduler.GetRun(r.Context(), t, runID)
		if err != nil {
			respondError(w, r, errors.Wrapf(err, "run %s", runID))
			return
		}
	}

	var (
		pr  interface{}
		err error
	)
	switch t.Type {
	case scheduler.RepairTask:
		pr, err = h.Repair.GetProgress(r.Context(), t.ClusterID, t.ID, prog.Run.ID)
	case scheduler.BackupTask:
		pr, err = h.Backup.GetProgress(r.Context(), t.ClusterID, t.ID, prog.Run.ID)
	case scheduler.ValidateBackupTask:
		pr, err = h.Backup.GetValidationProgress(r.Context(), t.ClusterID, t.ID, prog.Run.ID)
	default:
		respondBadRequest(w, r, errors.Errorf("unsupported task type %s", t.Type))
		return
	}
	if err != nil {
		// Ignoring ErrNotFound because progress can have task runs without repair progress recorded.
		// If we can't find any repair progress reference then just return what we have (prog.Run).
		// prog.Progress is assigned separately to force nil on the returned value instead of an empty object.
		// This is required for correct JSON representation and detection if Progress is empty.
		if !errors.Is(err, service.ErrNotFound) {
			respondError(w, r, errors.Wrapf(err, "load progress for task %q", t.ID))
			return
		}
	} else {
		prog.Progress = pr
	}

	render.Respond(w, r, prog)
}

func tryReadOffset(s string) (int, error) {
	const (
		latest = "latest"
		tilde  = "~"
	)

	if s == latest {
		return 0, nil
	}
	if strings.HasPrefix(s, tilde) {
		i64, err := strconv.ParseInt(s[1:], 10, 64)
		return int(i64), err
	}
	return -1, nil
}
