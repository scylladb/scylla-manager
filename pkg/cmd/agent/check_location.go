// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"os"
	"regexp"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/scylladb/go-log"
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

var errInvalidLocation = errors.Errorf("invalid location, the format is [dc:]<provider>:<path> ex. s3:my-bucket, the path must be DNS compliant")

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

		c, err := parseConfigFile(checkLocationArgs.configFile)
		if err != nil {
			return err
		}

		l := zap.FatalLevel
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
		if err := rclone.RegisterGCSProvider(c.GCS); err != nil {
			return err
		}

		// Providers require that resource names are DNS compliant.
		// The following is a super simplified DNS (plus provider prefix)
		// matching regexp.
		pattern := regexp.MustCompile(`^(([a-zA-Z0-9\-\_\.]+):)?([a-z0-9]+):([a-z0-9\-\.]+)$`)

		m := pattern.FindStringSubmatch(checkLocationArgs.location)
		if m == nil {
			return errInvalidLocation
		}

		f, err := fs.NewFs(m[3] + ":" + m[4])
		if err != nil {
			if errors.Cause(err) == fs.ErrorNotFoundInConfigFile {
				return errInvalidLocation
			}
			return errors.Wrap(err, "init fs")
		}

		return operations.CheckPermissions(context.Background(), f)
	},
}

func init() {
	cmd := checkLocationCmd

	f := cmd.Flags()
	f.StringSliceVarP(&checkLocationArgs.configFile, "config-file", "c", []string{"/etc/scylla-manager-agent/scylla-manager-agent.yaml"}, "configuration file `path`")
	f.StringVarP(&checkLocationArgs.location, "location", "L", "",
		"backup location in the format [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. The supported providers are: s3, gcs") //nolint: lll
	f.BoolVar(&checkLocationArgs.debug, "debug", false, "enable debug logs")

	if err := cmd.MarkFlagRequired("location"); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(cmd)
}
