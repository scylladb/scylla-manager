// Copyright (C) 2017 ScyllaDB

package main

import (
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/rclone"
	"go.uber.org/zap"
)

func setupCommand(configFiles []string, debug bool) (log.Logger, error) {
	c, err := config.ParseAgentConfigFiles(configFiles)
	if err != nil {
		return log.NopLogger, err
	}

	l := zap.ErrorLevel
	if debug {
		l = zap.DebugLevel
	}
	logger, err := log.NewProduction(log.Config{
		Mode:  log.StderrMode,
		Level: zap.NewAtomicLevelAt(l),
	})
	if err != nil {
		return logger, err
	}

	// Redirect standard logger to the logger
	zap.RedirectStdLog(log.BaseOf(logger))

	// Redirect rclone logger to the logger
	rclone.RedirectLogPrint(logger.Named("rclone"))
	// Init rclone config options
	rclone.InitFsConfigWithOptions(c.Rclone)
	// Register rclone providers
	if err := rclone.RegisterS3Provider(c.S3); err != nil {
		return logger, err
	}
	if err := rclone.RegisterGCSProvider(c.GCS); err != nil {
		return logger, err
	}
	if err := rclone.RegisterAzureProvider(c.Azure); err != nil {
		return logger, err
	}

	return logger, nil
}
