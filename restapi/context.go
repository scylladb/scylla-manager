// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"net/http"

	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/sched"
	"github.com/scylladb/mermaid/uuid"
)

// ctxt is a context key type.
type ctxt byte

const (
	ctxClusterID ctxt = iota
	ctxCluster
	ctxRepairUnit
	ctxTask
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

func mustUnitFromCtx(r *http.Request) *repair.Unit {
	u, ok := r.Context().Value(ctxRepairUnit).(*repair.Unit)
	if !ok {
		panic("missing repair unit in context")
	}
	return u
}

func mustTaskFromCtx(r *http.Request) *sched.Task {
	u, ok := r.Context().Value(ctxTask).(*sched.Task)
	if !ok {
		panic("missing task in context")
	}
	return u
}
