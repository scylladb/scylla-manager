// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/backup"
	"github.com/scylladb/scylla-manager/pkg/rclone"
	"github.com/scylladb/scylla-manager/pkg/rclone/operations"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var checkLocationArgs = struct {
	configFile []string
	location   string
	debug      bool
}{}

var checkLocationCmd = &cobra.Command{
	Use:   "check-location",
	Short: "Checks if backup location is accessible",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		defer func() {
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "FAILED: %v\n", err)
				os.Exit(1)
			}
		}()

		c, _, err := setupAgentCommand(checkLocationArgs.configFile, checkLocationArgs.debug)
		if err != nil {
			return err
		}

		if err := registerConfiguredProviders(c); err != nil {
			return errors.Wrap(err, "register providers")
		}

		location, err := backup.StripDC(checkLocationArgs.location)
		if err != nil {
			return err
		}

		f, err := fs.NewFs(context.Background(), location)
		if err != nil {
			if errors.Is(err, fs.ErrorNotFoundInConfigFile) {
				return fmt.Errorf("unknown provider %s", location)
			}
			return errors.Wrap(err, "init fs")
		}

		return operations.CheckPermissions(context.Background(), f)
	},
}

func registerConfiguredProviders(c config) error {
	if err := rclone.RegisterS3Provider(c.S3); err != nil {
		return err
	}
	if err := rclone.RegisterGCSProvider(c.GCS); err != nil {
		return err
	}
	if err := rclone.RegisterAzureProvider(c.Azure); err != nil {
		return err
	}

	return nil
}

func setupAgentCommand(configFile []string, debug bool) (config, log.Logger, error) {
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

	if err := registerConfiguredProviders(c); err != nil {
		return c, logger, errors.Wrap(err, "register providers")
	}

	return c, logger, nil
}

func init() {
	cmd := checkLocationCmd

	f := cmd.Flags()
	f.StringSliceVarP(&checkLocationArgs.configFile, "config-file", "c", []string{"/etc/scylla-manager-agent/scylla-manager-agent.yaml"}, "configuration file `path`")
	f.StringVarP(&checkLocationArgs.location, "location", "L", "",
		"backup location in the format [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. The supported providers are: "+strings.Join(backup.Providers(), ", ")) // nolint: lll
	f.BoolVar(&checkLocationArgs.debug, "debug", false, "enable debug logs")

	if err := cmd.MarkFlagRequired("location"); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(cmd)
}
