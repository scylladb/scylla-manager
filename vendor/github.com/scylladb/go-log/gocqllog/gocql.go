// Copyright (C) 2017 ScyllaDB

package gocqllog

import (
	"context"
	"fmt"

	"github.com/scylladb/go-log"
)

// StdLogger implements github.com/gocql/gocql.StdLogger and writes logs to the
// passed Logger with debug level.
type StdLogger struct {
	BaseCtx context.Context
	Logger  log.Logger
}

func (l StdLogger) Print(v ...interface{}) {
	l.Logger.Debug(l.BaseCtx, fmt.Sprint(v...))
}

func (l StdLogger) Printf(format string, v ...interface{}) {
	l.Logger.Debug(l.BaseCtx, fmt.Sprintf(format, v...))
}

func (l StdLogger) Println(v ...interface{}) {
	l.Logger.Debug(l.BaseCtx, fmt.Sprint(v...))
}
