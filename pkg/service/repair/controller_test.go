// Copyright (C) 2017 ScyllaDB

package repair

import (
	"context"
	"testing"

	"github.com/scylladb/go-log"
	"go.uber.org/atomic"
)

type controllerTestSuite struct {
	intensityHandler *intensityHandler
	hostRangesLimit  hostRangesLimit
}

const (
	controllerTestDefaultRangesLimit = 8
	controllerTestMaxRangesLimit     = 3*controllerTestDefaultRangesLimit + 1
	controllerTestRf                 = 3
)

func makeControllerTestSuite() controllerTestSuite {
	return controllerTestSuite{
		intensityHandler: &intensityHandler{
			logger:      log.NewDevelopment(),
			intensity:   atomic.NewInt64(1),
			parallel:    atomic.NewInt64(0),
			maxParallel: 2,
		},
		hostRangesLimit: hostRangesLimit{
			"a": rangesLimit{Default: controllerTestDefaultRangesLimit, Max: controllerTestMaxRangesLimit},
			"b": rangesLimit{Default: controllerTestDefaultRangesLimit, Max: controllerTestMaxRangesLimit},
			"c": rangesLimit{Default: controllerTestDefaultRangesLimit, Max: controllerTestMaxRangesLimit},
			"d": rangesLimit{Default: controllerTestDefaultRangesLimit, Max: controllerTestMaxRangesLimit},
			"e": rangesLimit{Default: controllerTestDefaultRangesLimit, Max: controllerTestMaxRangesLimit},
			"f": rangesLimit{Default: controllerTestDefaultRangesLimit, Max: controllerTestMaxRangesLimit + 1},
		},
	}
}

func (s controllerTestSuite) newRowLevelRepairController() *rowLevelRepairController {
	return newRowLevelRepairController(s.intensityHandler, s.hostRangesLimit, len(s.hostRangesLimit), controllerTestRf)
}

func TestRowLevelRepairController(t *testing.T) {
	ctx := context.Background()

	t.Run("TryBlock with maxIntensity to follow the to ensure number of token ranges per job", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := s.newRowLevelRepairController()

		const intensity = 1000

		s.intensityHandler.SetIntensity(ctx, intensity)

		ok, a := ctl.TryBlock([]string{"a", "b", "c"})
		if !ok {
			t.Fatal("TryBlock() failed to block")
		}
		if a.Ranges != intensity {
			t.Fatal("Picked up more ranges than defined with intensity")
		}

	})

	t.Run("TryBlock to ensure number of token ranges per job", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := s.newRowLevelRepairController()

		const intensity = 1000

		s.intensityHandler.SetIntensity(ctx, intensity)

		ok, a := ctl.TryBlock([]string{"a", "b", "c"})
		if !ok {
			t.Fatal("TryBlock() failed to block")
		}
		if a.Ranges != intensity {
			t.Fatal("Picked up more ranges than defined with intensity")
		}

	})

	t.Run("TryBlock to ensure max number of non-overlapping replica sets in parallel", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := s.newRowLevelRepairController()

		// parallel = 2 => max two non-overlapping replica sets to be repaired at the moment
		s.intensityHandler.SetParallel(ctx, 2)
		ok, a1 := ctl.TryBlock([]string{"a", "b", "c"})
		if !ok {
			t.Fatal("TryBlock() failed to block")
		}
		ok, a2 := ctl.TryBlock([]string{"d", "e", "f"})
		if !ok {
			t.Fatal("TryBlock() failed to block")
		}
		ctl.Unblock(a1)
		ctl.Unblock(a2)

		// parallel = 1 => max one non-overlapping replica sets to be repaired at the moment
		s.intensityHandler.SetParallel(ctx, 1)
		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
		if ok, _ := ctl.TryBlock([]string{"d", "e", "f"}); ok {
			t.Fatal("TryBlock() unexpected success")
		}
	})

	t.Run("TryBlock cross replicas not allowed", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := s.newRowLevelRepairController()

		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
		if ok, _ := ctl.TryBlock([]string{"b", "c", "d"}); ok {
			t.Fatal("TryBlock() unexpected success")
		}
		if ok, _ := ctl.TryBlock([]string{"c", "d", "e"}); ok {
			t.Fatal("TryBlock() unexpected success")
		}
		if ok, _ := ctl.TryBlock([]string{"d", "e", "f"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
	})

	t.Run("Busy", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := s.newRowLevelRepairController()

		ok, a := ctl.TryBlock([]string{"a", "b", "c"})
		if !ok {
			t.Fatal("TryBlock() failed to block")
		}
		if !ctl.Busy() {
			t.Fatalf("Busy() = %v", ctl.Busy())
		}
		ctl.Unblock(a)
		if ctl.Busy() {
			t.Fatalf("Busy() = %v", ctl.Busy())
		}
	})

	t.Run("MaxWorkerCount", func(t *testing.T) {
		s := makeControllerTestSuite()

		ctl := s.newRowLevelRepairController()
		golden := s.intensityHandler.MaxParallel() * controllerTestDefaultRangesLimit
		if ctl.MaxWorkerCount() != golden {
			t.Fatalf("MaxWorkerCount() = %d, expected %d", ctl.MaxWorkerCount(), golden)
		}
	})
}
