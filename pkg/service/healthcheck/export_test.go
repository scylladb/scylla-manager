// Copyright (C) 2017 ScyllaDB

package healthcheck

import "time"

func (dt *dynamicTimeout) calculateTimeout() time.Duration {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	return dt.calculateTimeoutLocked()
}
