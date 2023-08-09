// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type backupHandler struct {
	svc      BackupService
	schedSvc SchedService
}

func newBackupHandler(services Services) *chi.Mux {
	m := chi.NewMux()
	h := backupHandler{
		svc:      services.Backup,
		schedSvc: services.Scheduler,
	}

	rt := m.With(
		h.locationsCtx,
		h.listFilterCtx,
	)
	rt.Get("/", h.list)
	rt.Delete("/", h.deleteSnapshot)
	rt.Get("/files", h.listFiles)
	m.With(h.orphanedLocationsCtx).Delete("/purge", h.purge)

	return m
}

func (h backupHandler) locationsCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			locations backupspec.Locations
			err       error
		)

		// Read locations from the request
		if v := r.FormValue("locations"); v != "" {
			for _, v := range r.Form["locations"] {
				var l backupspec.Location
				if err := l.UnmarshalText([]byte(v)); err != nil {
					respondBadRequest(w, r, err)
					return
				}
				locations = append(locations, l)
			}
		}

		// Fallback read locations from scheduler
		if len(locations) == 0 {
			locations, err = h.extractLocations(r)
			if err != nil {
				respondError(w, r, err)
				return
			}
		}

		// Report error if no locations can be found
		if len(locations) == 0 {
			respondBadRequest(w, r, errors.New("missing locations"))
			return
		}

		ctx := r.Context()
		ctx = context.WithValue(ctx, ctxBackupLocations, locations)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h backupHandler) extractLocations(r *http.Request) ([]backupspec.Location, error) {
	tasks, err := h.schedSvc.ListTasks(r.Context(), mustClusterIDFromCtx(r), scheduler.ListFilter{TaskType: []scheduler.TaskType{scheduler.BackupTask}})
	if err != nil {
		return nil, err
	}
	properties := make([]json.RawMessage, 0, len(tasks))
	for _, t := range tasks {
		if t.Enabled {
			properties = append(properties, t.Properties)
		}
	}
	return h.svc.ExtractLocations(r.Context(), properties), nil
}

func (h backupHandler) orphanedLocationsCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var locations backupspec.Locations

		// Read locations from the request
		if v := r.FormValue("locations"); v != "" {
			for _, v := range r.Form["locations"] {
				var l backupspec.Location
				if err := l.UnmarshalText([]byte(v)); err != nil {
					respondBadRequest(w, r, err)
					return
				}
				locations = append(locations, l)
			}
		}

		// Fallback read locations from scheduler
		if len(locations) == 0 {
			tasksProperties, err := h.getTasksProperties(r.Context(), mustClusterIDFromCtx(r), true, true)
			if err != nil {
				respondError(w, r, err)
				return
			}
			locations = tasksProperties.GetLocations()
		}

		// Report error if no locations can be found
		if len(locations) == 0 {
			respondBadRequest(w, r, errors.New("missing locations"))
			return
		}

		ctx := r.Context()
		ctx = context.WithValue(ctx, ctxBackupLocations, locations)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h backupHandler) getTasksProperties(ctx context.Context, clusterID uuid.UUID, deleted, disabled bool) (backup.TaskPropertiesByUUID, error) {
	filter := scheduler.ListFilter{
		TaskType: []scheduler.TaskType{scheduler.BackupTask},
		Deleted:  deleted,
		Disabled: disabled,
	}
	tasksItems, err := h.schedSvc.ListTasks(ctx, clusterID, filter)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	tasksProperties, err := backup.GetTasksProperties(tasksItems)
	if err != nil {
		return nil, err
	}
	return tasksProperties, nil
}

func (h backupHandler) mustLocationsFromCtx(r *http.Request) []backupspec.Location {
	v, ok := r.Context().Value(ctxBackupLocations).(backupspec.Locations)
	if !ok {
		panic("missing locations in context")
	}
	return v
}

func (h backupHandler) listFilterCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		filter := backup.ListFilter{
			Keyspace:    r.Form["keyspace"],
			SnapshotTag: r.FormValue("snapshot_tag"),
		}

		c := mustClusterFromCtx(r)
		if v := r.FormValue("query_cluster_id"); v != "" {
			if c.ID.String() == v || c.Name == v {
				filter.ClusterID = c.ID
			} else if err := filter.ClusterID.UnmarshalText([]byte(v)); err != nil {
				respondBadRequest(w, r, errors.Wrap(err, "invalid query_cluster_id"))
				return
			}
		}

		if v := r.FormValue("min_date"); v != "" {
			if err := filter.MinDate.UnmarshalText([]byte(v)); err != nil {
				respondBadRequest(w, r, errors.Wrap(err, "invalid min_date"))
				return
			}
		}
		if v := r.FormValue("max_date"); v != "" {
			if err := filter.MaxDate.UnmarshalText([]byte(v)); err != nil {
				respondBadRequest(w, r, errors.Wrap(err, "invalid max_date"))
				return
			}
		}

		ctx := r.Context()
		ctx = context.WithValue(ctx, ctxBackupListFilter, filter)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h backupHandler) mustListFilterFromCtx(r *http.Request) backup.ListFilter {
	v, ok := r.Context().Value(ctxBackupListFilter).(backup.ListFilter)
	if !ok {
		panic("missing filter in context")
	}
	return v
}

func (h backupHandler) list(w http.ResponseWriter, r *http.Request) {
	v, err := h.svc.List(
		r.Context(),
		mustClusterIDFromCtx(r),
		h.mustLocationsFromCtx(r),
		h.mustListFilterFromCtx(r),
	)
	if err != nil {
		respondError(w, r, err)
		return
	}

	render.Respond(w, r, v)
}

func (h backupHandler) listFiles(w http.ResponseWriter, r *http.Request) {
	v, err := h.svc.ListFiles(
		r.Context(),
		mustClusterIDFromCtx(r),
		h.mustLocationsFromCtx(r),
		h.mustListFilterFromCtx(r),
	)
	if err != nil {
		respondError(w, r, err)
		return
	}

	render.Respond(w, r, v)
}

func (h backupHandler) deleteSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshotTags := r.Form["snapshot_tags"]
	if len(snapshotTags) == 0 {
		respondBadRequest(w, r, errors.New("missing snapshot tags"))
		return
	}

	err := h.svc.DeleteSnapshot(
		r.Context(),
		mustClusterIDFromCtx(r),
		h.mustLocationsFromCtx(r),
		snapshotTags,
	)
	if err != nil {
		respondError(w, r, errors.Wrap(err, "delete snapshots"))
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h backupHandler) purge(w http.ResponseWriter, r *http.Request) {
	tasksProp, err := h.getTasksProperties(r.Context(), mustClusterIDFromCtx(r), false, false)
	if err != nil {
		respondError(w, r, err)
		return
	}

	dryRun := false
	if v := r.FormValue("dry_run"); v == "true" {
		dryRun = true
	}

	deleted, warnings, err := h.svc.PurgeBackups(
		r.Context(),
		mustClusterIDFromCtx(r),
		h.mustLocationsFromCtx(r),
		tasksProp.GetRetentionMap(),
		dryRun,
	)
	if err != nil {
		fmt.Println("")
		respondError(w, r, errors.Wrap(err, "manual purge snapshots"))
		return
	}
	render.Respond(w, r, BackupPurgeOut{Deleted: ConvertManifestsToListItems(deleted), Warnings: warnings})
}

// ConvertManifestsToListItems converts Manifests to ListItems.
func ConvertManifestsToListItems(deleted backupspec.Manifests) backup.ListItems {
	out := &backup.ListItems{}
	for _, manifest := range deleted {
		item := out.GetOrAppend(manifest.ClusterID, manifest.TaskID)
		sInfo := item.SnapshotInfo.GetOrAppend(manifest.SnapshotTag)
		sInfo.Nodes++
	}
	return *out
}

// BackupPurgeOut represent response information backup purge.
type BackupPurgeOut struct {
	Deleted  backup.ListItems `json:"deleted"`
	Warnings []string         `json:"warnings"`
}
