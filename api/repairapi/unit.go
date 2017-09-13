// Copyright (C) 2017 ScyllaDB

package repairapi

import (
	"net/http"
	"net/url"
	"path"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/api/clusterapi"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/uuid"
)

type unitRequest struct {
	*repair.Unit

	ProtectedID        string `json:"id,omitempty"`
	ProtectedClusterID string `json:"cluster_id,omitempty"`
}

// ReqUnitID extracts a unit ID from a request.
func ReqUnitID(req *http.Request) (uuid.UUID, error) {
	var unitID uuid.UUID
	if err := unitID.UnmarshalText([]byte(chi.URLParam(req, "unit_id"))); err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid unit ID")
	}
	return unitID, nil
}

func (h *handler) ListUnits(w http.ResponseWriter, r *http.Request) {
	clusterID, err := clusterapi.ReqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ids, err := h.svc.ListUnitIDs(r.Context(), clusterID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render.Respond(w, r, ids)
}

func (h *handler) CreateUnit(w http.ResponseWriter, r *http.Request) {
	var err error
	clusterID, err := clusterapi.ReqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	u := new(unitRequest)
	if err := render.DecodeJSON(r.Body, u); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if u.ID, err = uuid.NewRandom(); err != nil {
		http.Error(w, "failed to generate a unit ID "+err.Error(), http.StatusInternalServerError)
		return
	}
	u.ClusterID = clusterID

	err = h.svc.PutUnit(r.Context(), u.Unit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	unitURL := r.URL.ResolveReference(&url.URL{Path: path.Join("unit", u.Unit.ID.String())})
	w.Header().Set("Location", unitURL.String())
	w.WriteHeader(http.StatusCreated)
}

func (h *handler) LoadUnit(w http.ResponseWriter, r *http.Request) {
	clusterID, err := clusterapi.ReqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := ReqUnitID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	u, err := h.svc.GetUnit(r.Context(), clusterID, id)
	if err != nil {
		if err == mermaid.ErrNotFound {
			http.Error(w, id.String(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	render.Respond(w, r, u)
}

func (h *handler) UpdateUnit(w http.ResponseWriter, r *http.Request) {
	clusterID, err := clusterapi.ReqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := ReqUnitID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	u := new(unitRequest)
	if err := render.DecodeJSON(r.Body, u); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	u.ID = id
	u.ClusterID = clusterID
	err = h.svc.PutUnit(r.Context(), u.Unit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render.Respond(w, r, u.Unit)
}

func (h *handler) DeleteUnit(w http.ResponseWriter, r *http.Request) {
	clusterID, err := clusterapi.ReqClusterID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := ReqUnitID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.svc.DeleteUnit(r.Context(), clusterID, id)
	if err != nil {
		if err == mermaid.ErrNotFound {
			http.Error(w, id.String(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}
