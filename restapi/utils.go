// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/uuid"
)

// reqClusterID extracts a cluster ID from a request.
func reqClusterID(req *http.Request) (uuid.UUID, error) {
	var clusterID uuid.UUID
	if err := clusterID.UnmarshalText([]byte(chi.URLParam(req, "cluster_id"))); err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid cluster ID")
	}
	return clusterID, nil
}

// reqUnitID extracts a unit ID from a request.
func reqUnitID(req *http.Request) (uuid.UUID, error) {
	var unitID uuid.UUID
	if err := unitID.UnmarshalText([]byte(chi.URLParam(req, "unit_id"))); err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid unit ID")
	}
	return unitID, nil
}
