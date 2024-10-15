// Copyright (C) 2024 ScyllaDB

package rcserver

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/rc"
	"go.uber.org/atomic"
)

// globalConfigGuard is global configGuard that should be used
// by rc calls when performing global config changes.
var globalConfigGuard = &configGuard{}

// configGuard is a tool for performing global config changes.
// It supports setting transfers and bandwidth limit.
// It does not re-set config values if they are already
// set to the desired value.
type configGuard struct {
	mu          sync.Mutex
	initialized atomic.Bool

	defaultTransfers int
	transfers        int
	bandwidthLimit   string
}

func (cg *configGuard) init() {
	if cg.initialized.CompareAndSwap(false, true) {
		defaultTransfers := fs.GetConfig(context.Background()).Transfers
		cg.defaultTransfers = defaultTransfers
		cg.transfers = defaultTransfers
		cg.bandwidthLimit = ""
	}
}

// SetTransfers sets global transfers value in rclone config.
// It also clears fs.Fs cache, so that they can be re-initialized
// with the new transfers value.
func SetTransfers(transfers int) error {
	globalConfigGuard.mu.Lock()
	defer globalConfigGuard.mu.Unlock()
	globalConfigGuard.init()

	if transfers == -1 {
		transfers = globalConfigGuard.defaultTransfers
	}
	if transfers <= 0 {
		return errors.Errorf("transfers count must be greater than 0, got %d", transfers)
	}
	// Returns global config
	ci := fs.GetConfig(context.Background())
	if transfers == globalConfigGuard.transfers {
		// Safety check in case configguard is not in sync with global config
		if transfers == ci.Transfers {
			// Transfers are already set to specified value
			return nil
		}
	}

	globalConfigGuard.transfers = transfers
	ci.Transfers = transfers
	// The amount of transfers impacts fs.Fs initialization (e.g. pool.Pool and fs.Pacer),
	// so fs.Fs cache should be cleared on transfers count change.
	cache.Clear()
	return nil
}

// SetBandwidthLimit sets global bandwidth limit in token bucket.
func SetBandwidthLimit(limit string) error {
	globalConfigGuard.mu.Lock()
	defer globalConfigGuard.mu.Unlock()
	globalConfigGuard.init()

	if limit == globalConfigGuard.bandwidthLimit {
		return nil
	}

	in := rc.Params{
		"rate": limit,
	}
	_, err := rcCalls.Get("core/bwlimit").Fn(context.Background(), in)
	if err != nil {
		return errors.Wrapf(err, "set bandwidth to %s", limit)
	}
	return nil
}
