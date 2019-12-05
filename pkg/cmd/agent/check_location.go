// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/rclone"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var checkLocationArgs = struct {
	configFile string
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

		c, err := parseConfigFile(rootArgs.configFile)
		if err != nil {
			return err
		}

		l := zap.ErrorLevel
		if checkLocationArgs.debug {
			l = zap.DebugLevel
		}
		logger, err := log.NewProduction(log.Config{
			Mode:  log.StderrMode,
			Level: l,
		})
		if err != nil {
			return err
		}

		// Redirect standard logger to the logger
		zap.RedirectStdLog(log.BaseOf(logger))

		// Redirect rclone logger to the logger
		rclone.RedirectLogPrint(logger.Named("rclone"))
		// Init rclone config options
		rclone.InitFsConfig()
		// Register rclone providers
		if err := rclone.RegisterS3Provider(c.S3); err != nil {
			return err
		}

		f, err := fs.NewFs(checkLocationArgs.location)
		if err != nil {
			return errors.Wrap(err, "initialize location")
		}
		if err := operations.List(context.Background(), f, ioutil.Discard); err != nil {
			return errors.Wrap(err, "access location")
		}

		return nil
	},
}

func init() {
	cmd := checkLocationCmd

	f := cmd.Flags()
	f.StringVarP(&checkLocationArgs.configFile, "config-file", "c", "/etc/scylla-manager-agent/scylla-manager-agent.yaml", "configuration file `path`")
	f.StringVarP(&checkLocationArgs.location, "location", "L", "", "path to the backup location in provider format e.g. s3:backup-bucket")
	f.BoolVar(&checkLocationArgs.debug, "debug", false, "enable debug logs")

	if err := cmd.MarkFlagRequired("location"); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(cmd)
}
