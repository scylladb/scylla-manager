// Copyright (C) 2024 ScyllaDB

package repair

import (
	"context"
	"math"
	"net/netip"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
)

type sizeSetter interface {
	SetSize(size int)
}

type intensityParallelHandler struct {
	taskID           uuid.UUID
	runID            uuid.UUID
	logger           log.Logger
	maxHostIntensity map[netip.Addr]Intensity
	intensity        *atomic.Int64
	maxParallel      int
	parallel         *atomic.Int64
	poolController   sizeSetter
}

const (
	maxIntensity     Intensity = 0
	defaultIntensity Intensity = 1
	defaultParallel            = 0
	chanSize                   = 10000
)

// SetIntensity sets the effective '--intensity' value used by newly created repair jobs.
// It can be used to control currently running repair without stopping it and changing '--intensity' flag.
// The change is ephemeral, so it does not change the value of '--intensity' flag in SM DB, meaning that without
// additional actions, it won't be visible in next task runs.
func (i *intensityParallelHandler) SetIntensity(ctx context.Context, intensity Intensity) {
	i.logger.Info(ctx, "Setting repair intensity", "value", intensity, "previous", i.intensity.Load())
	i.intensity.Store(int64(intensity))
}

// SetParallel works in the same way as SetIntensity, but for '--parallel' value.
func (i *intensityParallelHandler) SetParallel(ctx context.Context, parallel int) {
	i.logger.Info(ctx, "Setting repair parallel", "value", parallel, "previous", i.parallel.Load())
	i.parallel.Store(int64(parallel))
	if parallel == defaultParallel {
		i.poolController.SetSize(i.maxParallel)
	} else {
		i.poolController.SetSize(parallel)
	}
}

// ReplicaSetMaxIntensity returns the max amount of ranges that can be repaired in parallel on given replica set.
// It results in returning min(max_repair_ranges_in_parallel) across nodes from replica set.
func (i *intensityParallelHandler) ReplicaSetMaxIntensity(replicaSet []netip.Addr) Intensity {
	out := NewIntensity(math.MaxInt)
	for _, rep := range replicaSet {
		if ranges := i.maxHostIntensity[rep]; ranges < out {
			out = ranges
		}
	}
	return out
}

// MaxHostIntensity returns max_token_ranges_in_parallel per host.
func (i *intensityParallelHandler) MaxHostIntensity() map[netip.Addr]Intensity {
	return i.maxHostIntensity
}

// Intensity returns stored value for intensity.
func (i *intensityParallelHandler) Intensity() Intensity {
	return NewIntensity(int(i.intensity.Load()))
}

// MaxParallel returns maximal achievable parallelism.
func (i *intensityParallelHandler) MaxParallel() int {
	return i.maxParallel
}

// Parallel returns stored value for parallel.
func (i *intensityParallelHandler) Parallel() int {
	return int(i.parallel.Load())
}
