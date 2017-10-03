// Copyright (C) 2017 ScyllaDB

package gocqllog

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/scylladb/mermaid/log"
)

// New creates a gocql.StdLogger that writes logs to the passed logger on debug
// level.
func New(ctx context.Context, logger log.Logger) gocql.StdLogger {
	return &stdLogger{
		ctx:    ctx,
		logger: logger,
	}
}

type stdLogger struct {
	ctx    context.Context
	logger log.Logger
}

func (l stdLogger) Print(v ...interface{}) {
	l.logger.Debug(l.ctx, fmt.Sprint(v...))
}

func (l stdLogger) Printf(format string, v ...interface{}) {
	l.logger.Debug(l.ctx, fmt.Sprintf(format, v...))
}

func (l stdLogger) Println(v ...interface{}) {
	l.logger.Debug(l.ctx, fmt.Sprint(v...))
}
