// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
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

	m.Group(func(m chi.Router) {
		m.Use(
			h.locationsCtx,
			h.listFilterCtx,
		)
		m.Get("/", h.list)
		m.Delete("/", h.deleteSnapshot)
		m.Get("/files", h.listFiles)
	})
	m.Get("/schema", h.describeSchema)

	return m
}

func (h backupHandler) locationsCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			locations []backupspec.Location
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

func (h backupHandler) mustLocationsFromCtx(r *http.Request) []backupspec.Location {
	v, ok := r.Context().Value(ctxBackupLocations).([]backupspec.Location)
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

func (h backupHandler) describeSchema(w http.ResponseWriter, r *http.Request) {
	cluster := mustClusterFromCtx(r)

	snapshotTag := r.URL.Query().Get("snapshot_tag")
	if snapshotTag == "" {
		respondError(w, r, util.ErrValidate(errors.New("no snapshot_tag specified")))
		return
	}
	if !backupspec.IsSnapshotTag(snapshotTag) {
		respondError(w, r, util.ErrValidate(errors.Errorf("%s is not a snapshot tag", snapshotTag)))
		return
	}

	rawLocation := r.URL.Query().Get("location")
	if rawLocation == "" {
		respondError(w, r, util.ErrValidate(errors.New("no location specified")))
		return
	}
	var location backupspec.Location
	if err := location.UnmarshalText([]byte(rawLocation)); err != nil {
		respondError(w, r, util.ErrValidate(errors.Wrap(err, "parse location")))
		return
	}

	queryClusterID, err := parseOptionalUUID(r.URL.Query().Get("query_cluster_id"))
	if err != nil {
		respondError(w, r, util.ErrValidate(errors.Wrap(err, "parse query_cluster_id")))
		return
	}
	queryTaskID, err := parseOptionalUUID(r.URL.Query().Get("query_task_id"))
	if err != nil {
		respondError(w, r, util.ErrValidate(errors.Wrap(err, "parse query_task_id")))
		return
	}
	filter := backup.DescribeSchemaFilter{
		ClusterID: queryClusterID,
		TaskID:    queryTaskID,
	}

	schema, err := h.svc.GetDescribeSchema(r.Context(), cluster.ID, snapshotTag, location, filter)
	if err != nil {
		respondError(w, r, errors.Wrap(err, "get describe schema"))
		return
	}
	render.Respond(w, r, convertSchema(schema))
}

func parseOptionalUUID(v string) (uuid.UUID, error) {
	if v == "" {
		return uuid.Nil, nil
	}
	return uuid.Parse(v)
}

func convertSchema(schema query.DescribedSchema) models.BackupDescribeSchema {
	convertedRows := slices.Map(schema, func(row query.DescribedSchemaRow) *models.BackupDescribeSchemaItem {
		return &models.BackupDescribeSchemaItem{
			Keyspace: row.Keyspace,
			Type:     row.Type,
			Name:     row.Name,
			CqlStmt:  row.CQLStmt,
		}
	})
	return models.BackupDescribeSchema{
		Schema: convertedRows,
	}
}
