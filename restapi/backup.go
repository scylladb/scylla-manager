// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/service/backup"
)

type backupHandler struct {
	svc BackupService
}

func newBackupHandler(s BackupService) *chi.Mux {
	m := chi.NewMux()
	h := &backupHandler{
		svc: s,
	}

	m.Get("/", h.listBackups)

	return m
}

func (h backupHandler) listBackups(w http.ResponseWriter, r *http.Request) {
	c := mustClusterFromCtx(r)

	var host string
	if v := r.FormValue("host"); v != "" {
		host = v
	} else {
		respondBadRequest(w, r, errors.New("missing host"))
		return
	}

	var locations []backup.Location
	if v := r.FormValue("locations"); v == "" {
		respondBadRequest(w, r, errors.New("missing locations"))
		return
	}
	for _, v := range r.Form["locations"] {
		var l backup.Location
		if err := l.UnmarshalText([]byte(v)); err != nil {
			respondBadRequest(w, r, err)
			return
		}
		locations = append(locations, l)
	}

	var filter backup.ListFilter
	if v := r.FormValue("cluster_id"); v != "" {
		if v == c.Name || v == c.ID.String() {
			filter.ClusterID = c.ID
		} else if err := filter.ClusterID.UnmarshalText([]byte(v)); err != nil {
			respondBadRequest(w, r, errors.Wrap(err, "invalid cluster_id"))
			return
		}
	}
	filter.Keyspace = r.Form["keyspace"]
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

	list, err := h.svc.List(r.Context(), c.ID, host, locations, filter)
	if err != nil {
		respondError(w, r, err)
		return
	}

	render.Respond(w, r, list)
}
