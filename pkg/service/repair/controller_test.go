// Copyright (C) 2023 ScyllaDB

package repair

import (
	"context"
	"testing"

	"github.com/scylladb/go-log"
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
			logger:           log.Logger{},
			maxHostIntensity: maxRangesPerHost,
			intensity:        atomic.NewFloat64(defaultIntensity),
			maxParallel:      3,
			parallel:         atomic.NewInt64(defaultParallel),
		}
	}

	t.Run("make sure TryBlock() will deny if replicaset is already blocked", func(t *testing.T) {
		replicaSet := []string{node1, node2}
		c := newRowLevelRepairController(defaultIntensityHandler())

		if rangesCount := c.TryBlock(replicaSet); rangesCount == 0 {
			t.Fatal("expected to return ranges to repair, but got 0")
		}
		if rangesCount := c.TryBlock(replicaSet); rangesCount != 0 {
			t.Fatalf("expected to return 0 to repair, but got {%d}", rangesCount)
		}

		c.Unblock(replicaSet)
		if rangesCount := c.TryBlock(replicaSet); rangesCount == 0 {
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

		if err := ih.SetIntensity(context.Background(), expectedNrOfRanges); err != nil {
			t.Fatalf("unexpected error = {%v}", err)
		}
		if rangesCount := c.TryBlock(replicaSet); rangesCount != expectedNrOfRanges {
			t.Fatalf("expected to return {%d} ranges to repair, but got {%d}", expectedNrOfRanges, rangesCount)
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

		if err := ih.SetIntensity(context.Background(), float64(expectedNrOfRanges)); err != nil {
			t.Fatalf("unexpected error = {%v}", err)
		}
		if rangesCount := c.TryBlock(replicaSet); rangesCount != expectedNrOfRanges {
			t.Fatalf("expected to return {%d} ranges to repair, but got {%d}", expectedNrOfRanges, rangesCount)
		}
	})

	t.Run("make sure TryBlock() returns min max_ranges_in_parallel ranges for replica set when it is less than set intensity", func(t *testing.T) {
		const (
			maxParallel         = 2
			intensity           = 20
			minRangesInParallel = 15
		)
		replicaSet := []string{node1, node2, node6}
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		if err := ih.SetIntensity(context.Background(), intensity); err != nil {
			t.Fatalf("unexpected error = {%v}", err)
		}
		if rangesCount := c.TryBlock(replicaSet); rangesCount != minRangesInParallel {
			t.Fatalf("expected to return {%d} ranges to repair, but got {%d}", minRangesInParallel, rangesCount)
		}
	})

	t.Run("make sure TryBlock() will deny if there is more jobs than {parallel} already", func(t *testing.T) {
		replicaSet1 := []string{node1, node2}
		replicaSet2 := []string{node3, node4}
		maxParallel := 10
		ih := defaultIntensityHandler()
		ih.maxParallel = maxParallel
		c := newRowLevelRepairController(ih)

		if err := ih.SetParallel(context.Background(), 1); err != nil {
			t.Fatalf("unexpected error {%v}", err)
		}
		if rangesCount := c.TryBlock(replicaSet1); rangesCount == 0 {
			t.Fatal("expected to let in, but was denied")
		}
		if rangesCount := c.TryBlock(replicaSet2); rangesCount != 0 {
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

		if rangesCount := c.TryBlock(replicaSet1); rangesCount == 0 {
			t.Fatal("expected to let in, but was denied")
		}
		if rangesCount := c.TryBlock(replicaSet2); rangesCount == 0 {
			t.Fatal("expected to let in, but was denied")
		}
		if rangesCount := c.TryBlock(replicaSet3); rangesCount != 0 {
			t.Fatal("expected to deny, but was let in")
		}
	})
}
