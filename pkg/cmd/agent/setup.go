// Copyright (C) 2017 ScyllaDB

package main

import (
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/rclone"
	"go.uber.org/zap"
)

func setupCommand(configFile []string, debug bool) (config, log.Logger, error) {
	c, err := parseConfigFile(configFile)
	if err != nil {
		return c, log.Logger{}, err
	}

	l := zap.FatalLevel
	if debug {
		l = zap.DebugLevel
	}
	logger, err := log.NewProduction(log.Config{
		Mode:  log.StderrMode,
		Level: l,
	})
	if err != nil {
		return c, logger, err
	}

	// Redirect standard logger to the logger
	zap.RedirectStdLog(log.BaseOf(logger))

	// Redirect rclone logger to the logger
	rclone.RedirectLogPrint(logger.Named("rclone"))
	// Init rclone config options
	rclone.InitFsConfig()
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
