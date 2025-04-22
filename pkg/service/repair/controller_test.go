// Copyright (C) 2023 ScyllaDB

package repair

import (
	"context"
	"net/netip"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/util/workerpool"
	"go.uber.org/atomic"
)

func TestRowLevelRepairController_TryBlock(t *testing.T) {
	var (
		node1 = netip.MustParseAddr("192.168.1.1")
		node2 = netip.MustParseAddr("192.168.1.2")
		node3 = netip.MustParseAddr("192.168.1.3")
		node4 = netip.MustParseAddr("192.168.1.4")
		node5 = netip.MustParseAddr("192.168.1.5")
		node6 = netip.MustParseAddr("192.168.1.6")
	)

	maxRangesPerHost := map[netip.Addr]Intensity{
		node1: 20,
		node2: 19,
		node3: 18,
		node4: 17,
		node5: 16,
		node6: 15,
	}
	defaultIntensityHandler := func() *intensityParallelHandler {
		return &intensityParallelHandler{
			logger:           log.Logger{},
			maxHostIntensity: maxRangesPerHost,
			intensity:        atomic.NewInt64(int64(defaultIntensity)),
			maxParallel:      3,
			parallel:         atomic.NewInt64(defaultParallel),
			poolController: workerpool.New[*worker, job, jobResult](context.Background(), func(ctx context.Context, id int) *worker {
				return &worker{}
			}, 1024),
		}
	}

	t.Run("make sure TryBlock() will deny if replicaset is already blocked", func(t *testing.T) {
		replicaSet := []netip.Addr{node1, node2}
		c := newRowLevelRepairController(defaultIntensityHandler())

		if ok, _ := c.TryBlock(replicaSet); !ok {
			t.Fatal("expected to return ranges to repair, but got false")
		}
		if ok, rangesCount := c.TryBlock(replicaSet); ok {
			t.Fatalf("expected to return false, but got {%d}", rangesCount)
		}

		c.Unblock(replicaSet)
		if ok, _ := c.TryBlock(replicaSet); !ok {
			t.Fatal("expected to return ranges to repair, but got false")
		}
	})

	t.Run("make sure TryBlock() returns {intensity} number of ranges when {intensity != 0}", func(t *testing.T) {
		const (
			expectedNrOfRanges = 10
			maxParallel        = 2
		)
		replicaSet := []netip.Addr{node1, node2}
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		ih.SetIntensity(context.Background(), expectedNrOfRanges)
		if ok, rangesCount := c.TryBlock(replicaSet); !ok || rangesCount != expectedNrOfRanges {
			t.Fatalf("expected to return {%d} ranges to repair, but got {%d}", expectedNrOfRanges, rangesCount)
		}
	})

	t.Run("make sure TryBlock() returns {replicaMaxRanges} number of ranges when {intensity = 0}", func(t *testing.T) {
		const (
			maxParallel        = 2
			expectedNrOfRanges = 19
		)
		replicaSet := []netip.Addr{node1, node2}
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		ih.SetIntensity(context.Background(), expectedNrOfRanges)
		if ok, rangesCount := c.TryBlock(replicaSet); !ok || rangesCount != expectedNrOfRanges {
			t.Fatalf("expected to return {%d} ranges to repair, but got {%d}", expectedNrOfRanges, rangesCount)
		}
	})

	t.Run("make sure TryBlock() returns min max_ranges_in_parallel ranges for replica set when it is less than set intensity", func(t *testing.T) {
		const (
			maxParallel         = 2
			intensity           = 20
			minRangesInParallel = 15
		)
		replicaSet := []netip.Addr{node1, node2, node6}
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		ih.SetIntensity(context.Background(), intensity)
		if ok, rangesCount := c.TryBlock(replicaSet); !ok || rangesCount != minRangesInParallel {
			t.Fatalf("expected to return {%d} ranges to repair, but got {%d}", minRangesInParallel, rangesCount)
		}
	})

	t.Run("make sure TryBlock() will deny if there is more jobs than {parallel} already", func(t *testing.T) {
		replicaSet1 := []netip.Addr{node1, node2}
		replicaSet2 := []netip.Addr{node3, node4}
		maxParallel := 10
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		ih.SetParallel(context.Background(), 1)
		if ok, _ := c.TryBlock(replicaSet1); !ok {
			t.Fatal("expected to let in, but was denied")
		}
		if ok, _ := c.TryBlock(replicaSet2); ok {
			t.Fatal("expected to deny, but was let in")
		}
	})

	t.Run("make sure TryBlock() will deny if there is more jobs than maxParallel=2 already", func(t *testing.T) {
		replicaSet1 := []netip.Addr{node1, node2}
		replicaSet2 := []netip.Addr{node3, node4}
		replicaSet3 := []netip.Addr{node3, node4}
		maxParallel := 2
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		if ok, _ := c.TryBlock(replicaSet1); !ok {
			t.Fatal("expected to let in, but was denied")
		}
		if ok, _ := c.TryBlock(replicaSet2); !ok {
			t.Fatal("expected to let in, but was denied")
		}
		if ok, _ := c.TryBlock(replicaSet3); ok {
			t.Fatal("expected to deny, but was let in")
		}
	})
}
