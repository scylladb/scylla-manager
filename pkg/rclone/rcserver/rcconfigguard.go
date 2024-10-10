// Copyright (C) 2024 ScyllaDB

package rcserver

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
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

func (cg *configGuard) SetTransfers(transfers int) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.init()

	if transfers == -1 {
		transfers = cg.defaultTransfers
	}
	if transfers <= 0 {
		return errors.Errorf("transfers count must be greater than 0, got %d", transfers)
	}
	if transfers == cg.transfers {
		return nil
	}

	in := rc.Params{
		"transfers": transfers,
	}
	_, err := rcCalls.Get("core/transfers").Fn(context.Background(), in)
	if err != nil {
		return errors.Wrapf(err, "set transfers to %d", transfers)
	}
	return nil
}

func (cg *configGuard) SetBandwidthLimit(limit string) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.init()

	if limit == cg.bandwidthLimit {
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
