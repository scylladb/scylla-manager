// Copyright (C) 2024 ScyllaDB

package repair

import (
	"context"
	"math"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
)

type sizeSetter interface {
	SetSize(size int)
}

type intensityHandler struct {
	taskID           uuid.UUID
	runID            uuid.UUID
	logger           log.Logger
	maxHostIntensity map[string]Intensity
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

// SetIntensity sets the value of '--intensity' flag.
func (i *intensityHandler) SetIntensity(ctx context.Context, intensity Intensity) {
	i.logger.Info(ctx, "Setting repair intensity", "value", intensity, "previous", i.intensity.Load())
	i.intensity.Store(int64(intensity))
}

// SetParallel sets the value of '--parallel' flag.
func (i *intensityHandler) SetParallel(ctx context.Context, parallel int) {
	i.logger.Info(ctx, "Setting repair parallel", "value", parallel, "previous", i.parallel.Load())
	i.parallel.Store(int64(parallel))
	if parallel == defaultParallel {
		i.poolController.SetSize(i.maxParallel)
	} else {
		i.poolController.SetSize(parallel)
	}
}

func (i *intensityHandler) ReplicaSetMaxIntensity(replicaSet []string) Intensity {
	out := NewIntensity(math.MaxInt)
	for _, rep := range replicaSet {
		if ranges := i.maxHostIntensity[rep]; ranges < out {
			out = ranges
		}
	}
	return out
}

// MaxHostIntensity returns max_token_ranges_in_parallel per host.
func (i *intensityHandler) MaxHostIntensity() map[string]Intensity {
	return i.maxHostIntensity
}

// Intensity returns stored value for intensity.
func (i *intensityHandler) Intensity() Intensity {
	return NewIntensity(int(i.intensity.Load()))
}

// MaxParallel returns maximal achievable parallelism.
func (i *intensityHandler) MaxParallel() int {
	return i.maxParallel
}

// Parallel returns stored value for parallel.
func (i *intensityHandler) Parallel() int {
	return int(i.parallel.Load())
}
