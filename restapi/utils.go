// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/uuid"
)

// reqClusterID extracts a cluster ID from a request.
func reqClusterID(r *http.Request) (uuid.UUID, error) {
	var clusterID uuid.UUID
	if err := clusterID.UnmarshalText([]byte(chi.URLParam(r, "cluster_id"))); err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid cluster ID")
	}
	return clusterID, nil
}

// reqUnitID extracts a unit ID from a request URL.
func reqUnitID(r *http.Request) (uuid.UUID, error) {
	var unitID uuid.UUID
	if err := unitID.UnmarshalText([]byte(chi.URLParam(r, "unit_id"))); err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid unit ID")
	}
	return unitID, nil
}

// reqUnitIDQuery extracts a unit ID from a request Query arg.
// returned error is a suitable *httpError.
func reqUnitIDQuery(r *http.Request) (uuid.UUID, error) {
	id := r.FormValue("unit_id")
	if id == "" {
		return uuid.Nil, httpErrBadRequest(r, errors.Errorf("missing or empty query arg %q", "unit_id"))
	}
	var unitID uuid.UUID
	if err := unitID.UnmarshalText([]byte(id)); err != nil {
		return uuid.Nil, httpErrBadRequest(r, errors.Errorf("bad %q arg: %s", "unit_id", err.Error()))
	}
	return unitID, nil
}
