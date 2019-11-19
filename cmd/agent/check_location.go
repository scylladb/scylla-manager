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
	"github.com/scylladb/mermaid/rclone"
	"github.com/scylladb/mermaid/rclone/rcserver"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var checkLocationCmd = &cobra.Command{
	Use:   "check-location",
	Short: "Checks if backup location is accessible",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		defer func() {
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "%v\n", err)
				os.Exit(1)
			}
		}()

		debug, err := cmd.Flags().GetBool("debug")
		if err != nil {
			return err
		}
		l := zap.ErrorLevel
		if debug {
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
		rclone.SetDefaultConfig()
		// Register rclone providers
		if err := rcserver.RegisterInMemoryConf(); err != nil {
			return errors.Wrap(err, "configure agent")
		}

		location, err := cmd.Flags().GetString("location")
		if err != nil {
			return err
		}

		f, err := fs.NewFs(location)
		if err != nil {
			return errors.Wrap(err, "initialize location")
		}
		if err := operations.List(context.Background(), f, ioutil.Discard); err != nil {
			return errors.Wrap(err, "access location")
		}

		fmt.Fprintln(cmd.OutOrStdout(), "OK")
		return nil
	},
}

func init() {
	cmd := checkLocationCmd

	flags := cmd.Flags()
	flags.StringP("location", "L", "", "path to the backup location in provider format e.g. s3:backup-bucket")
	flags.Bool("debug", false, "path to the backup location in provider format e.g. s3:backup-bucket")

	if err := cmd.MarkFlagRequired("location"); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(cmd)
}
