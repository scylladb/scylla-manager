// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/scylladb/scylla-manager/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// ctxt is a context key type.
type ctxt byte

const (
	ctxClusterID ctxt = iota
	ctxCluster
	ctxTask

	ctxBackupLocations
	ctxBackupListFilter
)

func mustClusterIDFromCtx(r *http.Request) uuid.UUID {
	u, ok := r.Context().Value(ctxClusterID).(uuid.UUID)
	if !ok {
		panic("missing cluster ID in context")
	}
	return u
}

func mustClusterFromCtx(r *http.Request) *cluster.Cluster {
	c, ok := r.Context().Value(ctxCluster).(*cluster.Cluster)
	if !ok {
		panic("missing cluster in context")
	}
	return c
}

func mustTaskFromCtx(r *http.Request) *scheduler.Task {
	u, ok := r.Context().Value(ctxTask).(*scheduler.Task)
	if !ok {
		panic("missing task in context")
	}
	return u
}
