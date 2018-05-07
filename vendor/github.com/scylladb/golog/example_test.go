package golog_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/scylladb/golog"
	"go.uber.org/zap/zapcore"
)

func TestExample(t *testing.T) {
	ctx := golog.WithTraceID(context.Background())

	logger, err := golog.NewProduction(golog.Config{
		Mode:  golog.SyslogMode,
		Level: zapcore.InfoLevel,
	})
	if err != nil {
		t.Fatal(err)
	}
	logger.Info(ctx, "Could not connect to database",
		"sleep", 5*time.Second,
		"error", errors.New("I/O error"),
	)

	logger.Named("sub").Error(ctx, "Unexpected error", "error", errors.New("unexpected"))
}
