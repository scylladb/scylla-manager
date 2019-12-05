// Copyright (C) 2017 ScyllaDB

package tickrun

import "time"

func OverrideTickerChan(c <-chan time.Time) {
	overrideTickerChanTestHook = func() <-chan time.Time { return c }
}
