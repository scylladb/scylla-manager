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
)

func makeControllerTestSuite() controllerTestSuite {
	return controllerTestSuite{
		intensityHandler: &intensityHandler{
			logger:      log.NewDevelopment(),
			intensity:   atomic.NewFloat64(1),
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

func TestDefaultController(t *testing.T) {
	ctx := context.Background()

	t.Run("TryBlock", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := newDefaultController(s.intensityHandler, s.hostRangesLimit)

		ok, a := ctl.TryBlock([]string{"a", "b", "c"})
		if !ok {
			t.Fatal("TryBlock() failed to block")
		}
		for _, v := range []string{"a", "b", "c"} {
			if ok, _ := ctl.TryBlock([]string{v, "d", "e"}); ok {
				t.Fatal("TryBlock() unexpected success")
			}
		}
		if ok, _ := ctl.TryBlock([]string{"d", "e", "f"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
		ctl.Unblock(a)
		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
	})

	t.Run("TryBlock with parallel", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := newDefaultController(s.intensityHandler, s.hostRangesLimit)

		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
		s.intensityHandler.SetParallel(ctx, 1)
		if ok, _ := ctl.TryBlock([]string{"d", "e", "f"}); ok {
			t.Fatal("TryBlock() unexpected success")
		}
		s.intensityHandler.SetParallel(ctx, 0)
		if ok, _ := ctl.TryBlock([]string{"d", "e", "f"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
	})

	t.Run("TryBlock allowance", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := newDefaultController(s.intensityHandler, s.hostRangesLimit)

		{
			s.intensityHandler.SetIntensity(ctx, 1)
			ok, a := ctl.TryBlock([]string{"a", "b", "c"})
			if !ok {
				t.Fatal("TryBlock() failed to block")
			}
			if a.Ranges != controllerTestDefaultRangesLimit {
				t.Fatalf("TryBlock() = %v, expected %d ranges", a, controllerTestDefaultRangesLimit)
			}
		}

		{
			s.intensityHandler.SetIntensity(ctx, 0)
			ok, a := ctl.TryBlock([]string{"d", "e", "f"})
			if !ok {
				t.Fatal("TryBlock() failed to block")
			}
			if a.Ranges != controllerTestMaxRangesLimit {
				t.Fatalf("TryBlock() = %v, expected %d ranges", a, controllerTestMaxRangesLimit)
			}
		}
	})

	t.Run("Busy", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := newDefaultController(s.intensityHandler, s.hostRangesLimit)

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

		ctl := newDefaultController(s.intensityHandler, s.hostRangesLimit)
		if ctl.MaxWorkerCount() != s.intensityHandler.MaxParallel() {
			t.Fatalf("MaxWorkerCount() = %d, expected %d", ctl.MaxWorkerCount(), s.intensityHandler.MaxParallel())
		}
	})
}

func TestRowLevelRepairController(t *testing.T) {
	ctx := context.Background()

	t.Run("TryBlock", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := newRowLevelRepairController(s.intensityHandler, s.hostRangesLimit)

		for i := 0; i < controllerTestDefaultRangesLimit; i++ {
			if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
				t.Fatal("TryBlock() failed to block")
			}
		}
		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); ok {
			t.Fatal("TryBlock() unexpected success")
		}
		ctl.Unblock(allowance{
			Replicas: []string{"a", "b", "c"},
			Ranges:   1,
		})
		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
	})

	t.Run("TryBlock with intensity", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := newRowLevelRepairController(s.intensityHandler, s.hostRangesLimit)

		var queue []allowance
		for i := 0; i < controllerTestDefaultRangesLimit; i++ {
			if ok, a := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
				t.Fatal("TryBlock() failed to block")
			} else {
				queue = append(queue, a)
			}
		}
		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); ok {
			t.Fatal("TryBlock() unexpected success")
		}

		s.intensityHandler.SetIntensity(ctx, 2)
		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); ok {
			t.Fatal("TryBlock() unexpected success - all workers busy")
		}
		for _, a := range queue {
			ctl.Unblock(a)
		}

		for i := 0; i < controllerTestDefaultRangesLimit/2-1; i++ {
			if ok, a := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
				t.Fatal("TryBlock() failed to block")
			} else {
				queue = append(queue, a)
			}
		}

		s.intensityHandler.SetIntensity(ctx, 0)
		ok, a := ctl.TryBlock([]string{"a", "b", "c"})
		if !ok {
			t.Fatal("TryBlock() failed to block")
		}
		if a.Ranges <= 2 {
			t.Fatalf("TryBlock() = %v, expected full intensity", a)
		}

		s.intensityHandler.SetIntensity(ctx, 1)
		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); ok {
			t.Fatal("TryBlock() unexpected success - too many ranges")
		}
		ctl.Unblock(a)

		for i := 0; i < 2; i++ {
			if ok, _ = ctl.TryBlock([]string{"a", "b", "c"}); !ok {
				t.Fatal("TryBlock() failed to block")
			}
		}
	})

	t.Run("TryBlock with parallel", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := newRowLevelRepairController(s.intensityHandler, s.hostRangesLimit)

		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
		s.intensityHandler.SetParallel(ctx, 1)
		if ok, _ := ctl.TryBlock([]string{"d", "e", "f"}); ok {
			t.Fatal("TryBlock() failed to block")
		}
		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
			t.Fatal("TryBlock() unexpected success")
		}
		s.intensityHandler.SetParallel(ctx, 2)
		if ok, _ := ctl.TryBlock([]string{"d", "e", "f"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
	})

	t.Run("TryBlock cross replicas", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := newRowLevelRepairController(s.intensityHandler, s.hostRangesLimit)

		if ok, _ := ctl.TryBlock([]string{"a", "b", "c"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
		if ok, _ := ctl.TryBlock([]string{"b", "c", "d"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
		if ok, _ := ctl.TryBlock([]string{"c", "d", "e"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
		if ok, _ := ctl.TryBlock([]string{"d", "e", "f"}); !ok {
			t.Fatal("TryBlock() failed to block")
		}
	})

	t.Run("Busy", func(t *testing.T) {
		s := makeControllerTestSuite()
		ctl := newDefaultController(s.intensityHandler, s.hostRangesLimit)

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

		ctl := newRowLevelRepairController(s.intensityHandler, s.hostRangesLimit)
		golden := s.intensityHandler.MaxParallel() * controllerTestDefaultRangesLimit
		if ctl.MaxWorkerCount() != golden {
			t.Fatalf("MaxWorkerCount() = %d, expected %d", ctl.MaxWorkerCount(), golden)
		}
	})
}
