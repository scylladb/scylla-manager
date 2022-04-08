// Copyright (C) 2017 ScyllaDB

package bench

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/procfs"
	"github.com/rclone/rclone/fs"
	roperations "github.com/rclone/rclone/fs/operations"
	rsync "github.com/rclone/rclone/fs/sync"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone/operations"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"go.uber.org/multierr"
)

// Scenario contains memory stats recorded at the scenario start, end, and
// overall peak for the duration of the scenario.
type Scenario struct {
	name        string
	size        uint64
	startedAt   time.Time
	completedAt time.Time
	startMemory memoryStats
	endMemory   memoryStats
	maxMemory   memoryStats
	err         error

	done chan struct{}
}

// StartScenario starts recording stats for a new benchmarking scenario.
func StartScenario(dir string) *Scenario {
	s := &Scenario{
		name: dir,
		done: make(chan struct{}),
	}

	s.size, s.err = dirSize(dir)

	ms := readMemoryStats()
	s.startMemory = ms
	s.maxMemory = ms
	go s.observeMemory()

	s.startedAt = timeutc.Now()

	return s
}

func dirSize(dir string) (uint64, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var sum uint64
	for _, f := range files {
		info, err := f.Info()
		if err != nil {
			return 0, err
		}
		sum += uint64(info.Size())
	}
	return sum, err
}

func (s *Scenario) observeMemory() {
	const interval = 300 * time.Millisecond
	func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
			case <-s.done:
				return
			}
			s.maxMemory.max(readMemoryStats())
		}
	}()
}

// EndScenario finishes recording stats for benchmarking scenario.
// Must be called after scenario is done to release resources.
func (s *Scenario) EndScenario() {
	s.completedAt = timeutc.Now()

	close(s.done)
	ms := readMemoryStats()
	s.endMemory = ms
	s.maxMemory.max(ms)
}

func (s *Scenario) WriteTo(w io.Writer) (n int64, err error) {
	b := &strings.Builder{}
	defer func() {
		if err == nil {
			m, e := w.Write([]byte(b.String()))
			n, err = int64(m), e
		}
	}()

	fmt.Fprintf(b, "Scenario:\t%s\n", path.Base(s.name))
	if s.err != nil {
		fmt.Fprintf(b, "Error:\t%s\n", s.err)
	}
	fmt.Fprintf(b, "Size:\t\t%s\n", fs.SizeSuffix(s.size))
	fmt.Fprintf(b, "Duration:\t%s\n", s.completedAt.Sub(s.startedAt))

	rows := []struct {
		Field string
		Value func(m *memoryStats) fs.SizeSuffix
	}{
		{
			Field: "HeapInuse",
			Value: func(m *memoryStats) fs.SizeSuffix { return fs.SizeSuffix(m.HeapInuse) },
		},
		{
			Field: "Alloc",
			Value: func(m *memoryStats) fs.SizeSuffix { return fs.SizeSuffix(m.Alloc) },
		},
		{
			Field: "TotalAlloc",
			Value: func(m *memoryStats) fs.SizeSuffix { return fs.SizeSuffix(m.TotalAlloc) },
		},
		{
			Field: "Sys",
			Value: func(m *memoryStats) fs.SizeSuffix { return fs.SizeSuffix(m.Sys) },
		},
		{
			Field: "Resident",
			Value: func(m *memoryStats) fs.SizeSuffix { return fs.SizeSuffix(m.Resident) },
		},
		{
			Field: "Virtual",
			Value: func(m *memoryStats) fs.SizeSuffix { return fs.SizeSuffix(m.Virtual) },
		},
	}

	for _, r := range rows {
		if len(r.Field) < 6 {
			fmt.Fprintf(b, "%s:\t\t", r.Field)
		} else {
			fmt.Fprintf(b, "%s:\t", r.Field)
		}
		fmt.Fprintf(b, "%s/%s/%s", r.Value(&s.startMemory), r.Value(&s.endMemory), r.Value(&s.maxMemory))
		fmt.Fprintln(b)
	}

	return
}

type memoryStats struct {
	HeapInuse  uint64
	HeapIdle   uint64
	Alloc      uint64
	TotalAlloc uint64
	Sys        uint64

	Resident uint64
	Virtual  uint64
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

	// Failure to read memory will be manifested as Resident = 0.
	if p, err := procfs.Self(); err == nil {
		stat, err := p.Stat()
		if err == nil {
			ms.Resident = uint64(stat.ResidentMemory())
			ms.Virtual = uint64(stat.VirtualMemory())
		}
	}

	return ms
}

func (ms *memoryStats) max(v memoryStats) {
	if ms.HeapInuse < v.HeapInuse {
		ms.HeapInuse = v.HeapInuse
	}
	if ms.HeapIdle < v.HeapIdle {
		ms.HeapIdle = v.HeapIdle
	}
	if ms.Alloc < v.Alloc {
		ms.Alloc = v.Alloc
	}
	if ms.TotalAlloc < v.TotalAlloc {
		ms.TotalAlloc = v.TotalAlloc
	}
	if ms.Sys < v.Sys {
		ms.Sys = v.Sys
	}
	if ms.Resident < v.Resident {
		ms.Resident = v.Resident
	}
	if ms.Virtual < v.Virtual {
		ms.Virtual = v.Virtual
	}
}

// Benchmark allows setting up and running rclone copy scenarios against
// cloud storage providers.
type Benchmark struct {
	dst fs.Fs
}

// NewBenchmark setups new benchmark object for the provided location.
func NewBenchmark(ctx context.Context, location string) (*Benchmark, error) {
	f, err := fs.NewFs(ctx, location)
	if err != nil {
		return nil, errors.Wrapf(err, "init location")
	}

	// Get better debug information if there are permission issues
	if err := operations.CheckPermissions(ctx, f); err != nil {
		return nil, err
	}

	return &Benchmark{dst: f}, nil
}

// StartScenario copies files from the provided dir to the benchmark location.
// It returns memory stats collected during the execution.
func (b *Benchmark) StartScenario(ctx context.Context, dir string) (*Scenario, error) {
	s := StartScenario(dir)
	cleanup, err := copyDir(ctx, dir, b.dst)
	if err != nil {
		s.err = multierr.Append(s.err, errors.Wrap(err, "copy dir"))
	}
	s.EndScenario()

	if err := cleanup(); err != nil {
		s.err = multierr.Combine(s.err, errors.Wrap(err, "cleanup"))
	}

	return s, nil
}

func copyDir(ctx context.Context, dir string, dstFs fs.Fs) (cleanup func() error, err error) {
	const benchmarkDir = "benchmark"

	cleanup = func() error {
		return roperations.Purge(ctx, dstFs, benchmarkDir)
	}

	srcFs, err := fs.NewFs(ctx, dir)
	if err != nil {
		return cleanup, err
	}
	if err := rsync.CopyDir2(ctx, dstFs, benchmarkDir, srcFs, "", false); err != nil {
		return cleanup, err
	}

	return cleanup, err
}
