// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"

	"github.com/scylladb/mermaid/uuid"
)

// ctxt is a context key type.
type ctxt byte

const (
	ctxClusterID ctxt = iota
	ctxRepairUnit
)

// clusterIDFromCtx returns the Cluster ID of the (request) context ctx.
func clusterIDFromCtx(ctx context.Context) uuid.UUID {
	u, _ := ctx.Value(ctxClusterID).(uuid.UUID)
	return u
}

// newClusterIDCtx returns a new context.Context that carries clusterID.
func newClusterIDCtx(ctx context.Context, clusterID uuid.UUID) context.Context {
	return context.WithValue(ctx, ctxClusterID, clusterID)
}
