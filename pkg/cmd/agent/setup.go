// Copyright (C) 2017 ScyllaDB

package main

import (
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/rclone"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func setupCommand(configFiles []string, level zapcore.Level) (config.AgentConfig, log.Logger, error) {
	c, err := config.ParseAgentConfigFiles(configFiles)
	if err != nil {
		return c, log.NopLogger, err
	}

	logger, err := log.NewProduction(log.Config{
		Mode:  log.StderrMode,
		Level: zap.NewAtomicLevelAt(level),
	})
	if err != nil {
		return c, logger, err
	}

	// Redirect standard logger to the logger
	zap.RedirectStdLog(log.BaseOf(logger))

	// Redirect rclone logger to the logger
	rclone.RedirectLogPrint(logger.Named("rclone"))
	// Init rclone config options
	rclone.InitFsConfigWithOptions(c.Rclone)
	// Register rclone providers
	if err := rclone.RegisterS3Provider(c.S3); err != nil {
		return c, logger, err
	}
	if err := rclone.RegisterGCSProvider(c.GCS); err != nil {
		return c, logger, err
	}
	if err := rclone.RegisterAzureProvider(c.Azure); err != nil {
		return c, logger, err
	}

	return c, logger, nil
}
