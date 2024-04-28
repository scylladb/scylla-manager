// Copyright (C) 2023 ScyllaDB

package repair

import (
	"context"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/util/workerpool"
	"go.uber.org/atomic"
)

func TestRowLevelRepairController_TryBlock(t *testing.T) {
	const (
		node1 = "192.168.1.1"
		node2 = "192.168.1.2"
		node3 = "192.168.1.3"
		node4 = "192.168.1.4"
		node5 = "192.168.1.5"
		node6 = "192.168.1.6"
	)

	maxRangesPerHost := map[string]int{
		node1: 20,
		node2: 19,
		node3: 18,
		node4: 17,
		node5: 16,
		node6: 15,
	}
	defaultIntensityHandler := func() *intensityHandler {
		return &intensityHandler{
			logger:                    log.Logger{},
			maxRepairRangesInParallel: maxRangesPerHost,
			intensity:                 atomic.NewInt64(int64(defaultIntensity)),
			maxParallel:               3,
			parallel:                  atomic.NewInt64(defaultParallel),
			maxJobsPerHost:            atomic.NewInt64(1),
			poolController: workerpool.New[*worker, job, jobResult](context.Background(), func(ctx context.Context, id int) *worker {
				return &worker{}
			}, 1024),
		}
	}

	t.Run("make sure TryBlock() will deny if replicaset is already blocked", func(t *testing.T) {
		replicaSet := []string{node1, node2}
		c := newRowLevelRepairController(defaultIntensityHandler())

		if rangesCnt, ok := c.TryBlock(replicaSet, 1); !ok || rangesCnt != 1 {
			t.Fatal("expected to return ranges to repair, but got 0")
		}
		if rangesCnt, ok := c.TryBlock(replicaSet, 1); ok || rangesCnt != 0 {
			t.Fatalf("expected to return 0 to repair, but got {%d}", rangesCnt)
		}

		c.Unblock(replicaSet, 1)
		if rangesCnt, ok := c.TryBlock(replicaSet, 1); !ok || rangesCnt != 1 {
			t.Fatal("expected to return ranges to repair, but got 0")
		}
	})

	t.Run("make sure TryBlock() returns {intensity} number of ranges when {intensity != 0}", func(t *testing.T) {
		const (
			expectedNrOfRanges = 10
			maxParallel        = 2
		)
		replicaSet := []string{node1, node2}
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		ih.SetIntensity(context.Background(), expectedNrOfRanges)
		if rangesCnt, ok := c.TryBlock(replicaSet, 10*expectedNrOfRanges); !ok || rangesCnt != expectedNrOfRanges {
			t.Fatalf("expected to return {%d} ranges to repair, but got {%d}", expectedNrOfRanges, rangesCnt)
		}
	})

	t.Run("make sure TryBlock() returns {replicaMaxRanges} number of ranges when {intensity = 0}", func(t *testing.T) {
		const (
			maxParallel        = 2
			expectedNrOfRanges = 19
		)
		replicaSet := []string{node1, node2}
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		ih.SetIntensity(context.Background(), expectedNrOfRanges)
		if rangesCnt, ok := c.TryBlock(replicaSet, 10*expectedNrOfRanges); !ok || rangesCnt != expectedNrOfRanges {
			t.Fatalf("expected to return {%d} ranges to repair, but got {%d}", expectedNrOfRanges, rangesCnt)
		}
	})

	t.Run("make sure TryBlock() returns min max_ranges_in_parallel ranges for replica set when it is less than set intensity", func(t *testing.T) {
		t.Skip("This branch was specifically changed so that SM does not cap intensity to max_ranges_in_parallel")

		const (
			maxParallel         = 2
			intensity           = 20
			minRangesInParallel = 15
		)
		replicaSet := []string{node1, node2, node6}
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		ih.SetIntensity(context.Background(), intensity)
		if rangesCnt, ok := c.TryBlock(replicaSet, 10*minRangesInParallel); !ok || rangesCnt != minRangesInParallel {
			t.Fatalf("expected to return {%d} ranges to repair, but got {%d}", minRangesInParallel, rangesCnt)
		}
	})

	t.Run("make sure TryBlock() will deny if there is more jobs than {parallel} already", func(t *testing.T) {
		replicaSet1 := []string{node1, node2}
		replicaSet2 := []string{node3, node4}
		maxParallel := 10
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		ih.SetParallel(context.Background(), 1)
		if rangesCnt, ok := c.TryBlock(replicaSet1, 1); !ok || rangesCnt == 0 {
			t.Fatal("expected to let in, but was denied")
		}
		if rangesCnt, ok := c.TryBlock(replicaSet2, 1); ok || rangesCnt != 0 {
			t.Fatal("expected to deny, but was let in")
		}
	})

	t.Run("make sure TryBlock() will deny if there is more jobs than maxParallel=2 already", func(t *testing.T) {
		replicaSet1 := []string{node1, node2}
		replicaSet2 := []string{node3, node4}
		replicaSet3 := []string{node3, node4}
		maxParallel := 2
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		if rangesCnt, ok := c.TryBlock(replicaSet1, 1); !ok || rangesCnt == 0 {
			t.Fatal("expected to let in, but was denied")
		}
		if rangesCnt, ok := c.TryBlock(replicaSet2, 1); !ok || rangesCnt == 0 {
			t.Fatal("expected to let in, but was denied")
		}
		if rangesCnt, ok := c.TryBlock(replicaSet3, 1); ok || rangesCnt != 0 {
			t.Fatal("expected to deny, but was let in")
		}
	})

	t.Run("max-jobs-per-host", func(t *testing.T) {
		replicaSet1 := []string{node1, node2}
		replicaSet2 := []string{node1, node3}
		replicaSet3 := []string{node1, node4}
		replicaSet4 := []string{node1, node5}

		replicaSet5 := []string{node2, node3}
		replicaSet6 := []string{node2, node4}

		ih := defaultIntensityHandler()
		ih.intensity.Store(10)
		ih.parallel.Store(0)
		ih.maxParallel = 10
		ih.maxJobsPerHost.Store(3)
		c := newRowLevelRepairController(ih)
		// Initial blocks around node1
		if rangesCnt, ok := c.TryBlock(replicaSet1, 3); !ok || rangesCnt != 3 {
			t.Fatal("expected to let in, but was denied")
		}
		if rangesCnt, ok := c.TryBlock(replicaSet2, 3); !ok || rangesCnt != 3 {
			t.Fatal("expected to let in, but was denied")
		}
		if rangesCnt, ok := c.TryBlock(replicaSet3, 3); !ok || rangesCnt != 3 {
			t.Fatal("expected to let in, but was denied")
		}
		// Hitting limit of max-jobs-per-host
		if _, ok := c.TryBlock(replicaSet4, 1); ok {
			t.Fatal("expected to deny, but was let in")
		}
		// Hitting limit of intensity - returns fewer ranges
		if rangesCnt, ok := c.TryBlock(replicaSet5, 10); !ok || rangesCnt != 7 {
			t.Fatal("expected to let in, but was denied")
		}
		// Hitting limit of intensity - no ranges to return
		if _, ok := c.TryBlock(replicaSet6, 1); ok {
			t.Fatal("expected to deny, but was let in")
		}
		// Unblocking node2 and blocking it again
		c.Unblock(replicaSet1, 3)
		if rangesCnt, ok := c.TryBlock(replicaSet6, 3); !ok || rangesCnt != 3 {
			t.Fatal("expected to deny, but was let in")
		}
	})
}
