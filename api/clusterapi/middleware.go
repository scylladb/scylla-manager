// Copyright (C) 2017 ScyllaDB

package clusterapi

import (
	"net/http"

	"github.com/go-chi/chi"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/uuid"
)

// ReqClusterID extracts a cluster ID from a request.
func ReqClusterID(req *http.Request) (uuid.UUID, error) {
	var clusterID uuid.UUID
	if err := clusterID.UnmarshalText([]byte(chi.URLParam(req, "cluster_id"))); err != nil {
		return uuid.Nil, errors.Wrap(err, "invalid cluster ID")
	}
	return clusterID, nil
}
