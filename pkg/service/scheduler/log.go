// Copyright (C) 2017 ScyllaDB

package scheduler

import "context"

func (s *Service) logInfoOrDebug(t TaskType) func(ctx context.Context, msg string, keyvals ...interface{}) {
	switch t {
	case HealthCheckTask, HealthCheckRESTTask:
		return s.logger.Debug
	default:
		return s.logger.Info
	}
}
