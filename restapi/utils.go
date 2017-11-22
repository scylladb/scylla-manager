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
