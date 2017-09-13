// Copyright (C) 2017 ScyllaDB

package repairapi

import (
	"context"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/uuid"
)

type handler struct {
	chi.Router

	svc Service
}

//go:generate mockgen -source api.go -destination repairmock/service_mock.go -package repairmock

// Service is the repair service interface required by the repair REST API handlers.
type Service interface {
	GetUnit(ctx context.Context, clusterID, ID uuid.UUID) (*repair.Unit, error)
	PutUnit(ctx context.Context, u *repair.Unit) error
	DeleteUnit(ctx context.Context, clusterID, ID uuid.UUID) error
	ListUnitIDs(ctx context.Context, clusterID uuid.UUID) ([]uuid.UUID, error)
}

// New returns an http.Handler implementing the repair.Unit REST API endpoints.
func New(svc Service) http.Handler {
	h := &handler{Router: chi.NewRouter(), svc: svc}
	h.Get("/units", h.ListUnits)
	h.Post("/units", h.CreateUnit)
	h.Get("/unit/{unit_id}", h.LoadUnit)
	h.Put("/unit/{unit_id}", h.UpdateUnit)
	h.Delete("/unit/{unit_id}", h.DeleteUnit)
	return h
}
