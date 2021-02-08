// Copyright (C) 2017 ScyllaDB

package bench

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/procfs"
	"github.com/rclone/rclone/fs"
	roperations "github.com/rclone/rclone/fs/operations"
	rsync "github.com/rclone/rclone/fs/sync"
	"github.com/scylladb/scylla-manager/pkg/backup"
	"github.com/scylladb/scylla-manager/pkg/rclone/operations"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"go.uber.org/multierr"
)

// Scenario contains memory stats recorded at the scenario start, end, and
// overall peak for the duration of the scenario.
type Scenario struct {
	Name        string
	StartedAt   time.Time
	CompletedAt time.Time
	StartMemory memoryStats
	EndMemory   memoryStats
	Err         error

	mu         sync.Mutex
	peakMemory memoryStats

	done chan struct{}
}

// StartScenario starts recording stats for a new benchmarking scenario.
func StartScenario(name string) *Scenario {
	ms := readMemoryStats()
	ss := Scenario{
		Name:        name,
		StartedAt:   timeutc.Now(),
		StartMemory: ms,
		done:        make(chan struct{}),
	}
	// Observe peak memory usage.
	go ss.updatePeakMemory()

	return &ss
}

func (ss *Scenario) updatePeakMemory() {
	const peakMemoryCheckInterval = 50 * time.Millisecond
	func() {
		t := time.NewTicker(peakMemoryCheckInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
			case <-ss.done:
				return
			}
			ms := readMemoryStats()
			ss.mu.Lock()
			if ss.peakMemory.HeapInuse < ms.HeapInuse {
				ss.peakMemory.HeapInuse = ms.HeapInuse
			}
			if ss.peakMemory.HeapIdle < ms.HeapIdle {
				ss.peakMemory.HeapIdle = ms.HeapIdle
			}
			if ss.peakMemory.Alloc < ms.Alloc {
				ss.peakMemory.Alloc = ms.Alloc
			}
			if ss.peakMemory.TotalAlloc < ms.TotalAlloc {
				ss.peakMemory.TotalAlloc = ms.TotalAlloc
			}
			if ss.peakMemory.Sys < ms.Sys {
				ss.peakMemory.Sys = ms.Sys
			}
			if ss.peakMemory.ResidentMemory < ms.ResidentMemory {
				ss.peakMemory.ResidentMemory = ms.ResidentMemory
			}
			if ss.peakMemory.VirtualMemory < ms.VirtualMemory {
				ss.peakMemory.VirtualMemory = ms.VirtualMemory
			}
			ss.mu.Unlock()
		}
	}()
}

// EndScenario finishes recording stats for benchmarking scenario.
// Must be called after scenario is done to release resources.
func (ss *Scenario) EndScenario() {
	close(ss.done)
	ss.EndMemory = readMemoryStats()
	ss.CompletedAt = timeutc.Now()
}

func (ss *Scenario) String() string {
	var b strings.Builder

	fmt.Fprintln(&b, "Scenario:	", ss.Name)
	if ss.Err != nil {
		fmt.Fprintln(&b, "Err:	", ss.Err)
	} else {
		fmt.Fprintln(&b, "Duration:	", ss.CompletedAt.Sub(ss.StartedAt))
		fmt.Fprintln(&b, "StartMemory:	", ss.StartMemory)
		fmt.Fprintln(&b, "EndMemory:	", ss.EndMemory)
		ss.mu.Lock()
		if ss.peakMemory.ResidentMemory > 0 {
			fmt.Fprintln(&b, "PeakMemory:	", ss.peakMemory)
		}
		ss.mu.Unlock()
	}

	return b.String()
}

type memoryStats struct {
	HeapInuse      uint64
	HeapIdle       uint64
	Alloc          uint64
	TotalAlloc     uint64
	Sys            uint64
	ResidentMemory uint64
	VirtualMemory  uint64
}

func readMemoryStats() memoryStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ms := memoryStats{
		HeapInuse:  m.HeapInuse,
		HeapIdle:   m.HeapIdle,
		Alloc:      m.Alloc,
		TotalAlloc: m.TotalAlloc,
		Sys:        m.Sys,
	}

	p, err := procfs.Self()
	// Failure to read memory will be manifested as ResidentMemory = 0.
	if err == nil {
		stat, err := p.Stat()
		if err == nil {
			ms.ResidentMemory = uint64(stat.ResidentMemory())
			ms.VirtualMemory = uint64(stat.VirtualMemory())
		}
	}

	return ms
}

func (ms memoryStats) String() string {
	tmp := "HeapInuse:%dM,	" +
		"HeapIdle:%dM,	" +
		"Alloc:%dM,	" +
		"TotalAlloc:%dM,	" +
		"Sys:%dM,	" +
		"Resident:%dM,	" +
		"Virtual:%dM"

	return fmt.Sprintf(tmp,
		bToMb(ms.HeapInuse),
		bToMb(ms.HeapIdle),
		bToMb(ms.Alloc),
		bToMb(ms.TotalAlloc),
		bToMb(ms.Sys),
		bToMb(ms.ResidentMemory),
		bToMb(ms.VirtualMemory),
	)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// Benchmark allows setting up and running rclone copy scenarios against
// cloud storage providers.
type Benchmark struct {
	dst fs.Fs
}

// NewBenchmark setups new benchmark object for the provided location.
func NewBenchmark(ctx context.Context, loc string) (*Benchmark, error) {
	l, err := backup.StripDC(loc)
	if err != nil {
		return nil, errors.Wrapf(err, loc)
	}

	f, err := fs.NewFs(l)
	if err != nil {
		if errors.Is(err, fs.ErrorNotFoundInConfigFile) {
			return nil, backup.ErrInvalid
		}
		return nil, errors.Wrapf(err, loc)
	}

	// Get better debug information if there are permission issues
	if err := operations.CheckPermissions(ctx, f); err != nil {
		return nil, err
	}

	return &Benchmark{
		dst: f,
	}, nil
}

// CopyDir performs directory copy of files from the provided scenario path to
// the benchmarked location.
// It returns Stats collected during execution.
func (b *Benchmark) CopyDir(ctx context.Context, scenarioPath string) (*Scenario, error) {
	stats := StartScenario(scenarioPath)
	cleanup, err := copyDir(ctx, scenarioPath, b.dst)
	if err != nil {
		stats.Err = err
	}
	stats.EndScenario()
	if err := cleanup(); err != nil {
		stats.Err = multierr.Combine(stats.Err, err)
	}

	return stats, nil
}

func copyDir(ctx context.Context, scenarioPath string, dstFs fs.Fs) (cleanup func() error, err error) {
	const benchmarkDir = "benchmark"
	deleteFiles := func() error {
		return roperations.Purge(ctx, dstFs, benchmarkDir)
	}

	srcFs, err := fs.NewFs(scenarioPath)
	if err != nil {
		return deleteFiles, err
	}
	if err := rsync.CopyDir2(ctx, dstFs, benchmarkDir, srcFs, "", false); err != nil {
		return deleteFiles, err
	}

	return deleteFiles, nil
}
