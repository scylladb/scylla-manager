// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/scylladb/scylla-manager/pkg/rclone/operations"
	"github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

var checkLocationArgs = struct {
	configFiles []string
	location    backupspec.LocationValue
	debug       bool
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

		level := zapcore.ErrorLevel
		if checkLocationArgs.debug {
			level = zapcore.DebugLevel
		}
		_, err = setupCommand(checkLocationArgs.configFiles, level)
		if err != nil {
			return err
		}

		f, err := fs.NewFs(context.Background(), checkLocationArgs.location.Value().RemotePath(""))
		if err != nil {
			return errors.Wrap(err, "init location")
		}

		return operations.CheckPermissions(context.Background(), f)
	},
}

func init() {
	cmd := checkLocationCmd

	f := cmd.Flags()
	f.StringSliceVarP(&checkLocationArgs.configFiles, "config-file", "c", []string{"/etc/scylla-manager-agent/scylla-manager-agent.yaml"}, "configuration file `path`")
	f.VarP(&checkLocationArgs.location, "location", "L", "backup location in the format [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. The supported providers are: "+strings.Join(backupspec.Providers(), ", ")) // nolint: lll
	f.BoolVar(&checkLocationArgs.debug, "debug", false, "enable debug logs")

	requireFlags(cmd, "location")
	rootCmd.AddCommand(cmd)
}
