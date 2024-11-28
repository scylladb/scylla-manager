// Copyright (C) 2024 ScyllaDB

package rcserver

import (
	"context"
	"slices"
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
}

func (cg *configGuard) init() {
	if cg.initialized.CompareAndSwap(false, true) {
		defaultTransfers := fs.GetConfig(context.Background()).Transfers
		cg.defaultTransfers = defaultTransfers
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
	if transfers == ci.Transfers {
		// Transfers are already set to specified value
		return nil
	}

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

	currLimit, err := getBandwidthLimit()
	if err != nil {
		return err
	}
	eq, err := equalBandwidths(limit, currLimit)
	if err != nil {
		return err
	}
	if eq {
		// Bandwidth limit is already set to specified value
		return nil
	}

	in := rc.Params{
		"rate": limit,
	}
	// Uses *tokenBucket.rcBwlimit method
	_, err = rcCalls.Get("core/bwlimit").Fn(context.Background(), in)
	if err != nil {
		return errors.Wrapf(err, "set bandwidth to %s", limit)
	}
	return nil
}

func getBandwidthLimit() (string, error) {
	// Uses *tokenBucket.rcBwlimit method
	out, err := rcCalls.Get("core/bwlimit").Fn(context.Background(), make(rc.Params))
	if err != nil {
		return "", errors.Wrap(err, "get bandwidth")
	}
	limit, err := out.GetString("rate")
	if err != nil {
		return "", errors.Wrap(err, "parse current bandwidth")
	}
	return limit, err
}

func equalBandwidths(limit1, limit2 string) (bool, error) {
	bws1, err := parseBandwidth(limit1)
	if err != nil {
		return false, err
	}
	bws2, err := parseBandwidth(limit2)
	if err != nil {
		return false, err
	}
	return slices.EqualFunc(bws1, bws2, func(s1 fs.BwTimeSlot, s2 fs.BwTimeSlot) bool {
		if s1.HHMM != s2.HHMM || s1.DayOfTheWeek != s2.DayOfTheWeek {
			return false
		}
		// Unlimited bandwidth can be described by any number <= 0
		if s1.Bandwidth.Tx > 0 || s2.Bandwidth.Tx > 0 {
			if s1.Bandwidth.Tx != s2.Bandwidth.Tx {
				return false
			}
		}
		if s1.Bandwidth.Rx > 0 || s2.Bandwidth.Rx > 0 {
			if s1.Bandwidth.Rx != s2.Bandwidth.Rx {
				return false
			}
		}
		return true
	}), nil
}

func parseBandwidth(limit string) (fs.BwTimetable, error) {
	var bws fs.BwTimetable
	err := bws.Set(limit)
	if err != nil {
		return nil, errors.Wrapf(err, "parse limit: %s", limit)
	}
	return bws, nil
}
